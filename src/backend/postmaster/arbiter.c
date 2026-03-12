/*
 * arbiter.c
 * 		cluster monitor and new leader selector based on shared storage.
 * 
 * IDENTIFICATION
 * 		src/backend/postmaster/arbiter.c
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/walreceiver.h"
#include "storage/arbiter.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#define LEADER_LEASE_TIMEOUT 10

static SSLMSharedState *SSLMShmem = NULL;

static void ProcessArbiterInterrupts();
static void ProcessClusterStateCheck();
static bool TryPromoteSlave(bool wait, int wait_seconds);

extern bool Atomic_CAS_Disk(int fd, uint64 old_gen, SharedStorageControl *new_ctrl);

/*
 * Amount of shared memory required for this module.
 */
Size
SSLMShmemSize(void)
{
	return sizeof(SSLMSharedState);
}

/*
 * Create or attach to shared memory segment for this module.
 */
void
SSLMShmemInit(void)
{
	bool found;

	SSLMShmem = (SSLMSharedState *)
		ShmemInitStruct("SSLM State", SSLMShmemSize(), &found);

	if (!found)
	{
		memset(SSLMShmem, 0, sizeof(SSLMSharedState));
		SpinLockInit(&SSLMShmem->mutex);
		InitSharedLatch(&SSLMShmem->proc_latch);
	}
}

/* Interrupt handler for main loop of arbiter process */
static void
ProcessArbiterInterrupts()
{
	if (ProcSignalBarrierPending)
		ProcessProcSignalBarrier();

	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}
	if (ShutdownRequestPending)
	{
		elog(LOG, "arbiter process shutting down");
		proc_exit(0);
	}
}

/*
 * Check cluster's state and select a new leader if needed.
 */
static void
ProcessClusterStateCheck()
{
	SharedStorageControl local_ctrl;
	time_t now;
	bool am_i_primary;

	now = time(NULL);
	am_i_primary = !RecoveryInProgress();

	if (am_i_primary)
	{
		/* leader: update lease */
		SpinLockAcquire(&SSLMShmem->mutex);
		SSLMShmem->is_leader = true;
		local_ctrl.heartbeat_ts = now;
		local_ctrl.generation++;
		/* update generation */
		/*
		if (pwrite(lock_fd, &local_ctrl, 512, 0) != 512)
		{
			SpinLockRelease(&SSLMShmem->mutex);
			ereport(PANIC, (errmsg("lost access to shared storage! Fencing myself.")));
		}
		*/

		SSLMShmem->last_disk_copy = local_ctrl;
		SpinLockRelease(&SSLMShmem->mutex);
	}
	else
	{
		/* slave: check lease */
		bool need_reconnect = false;

		SpinLockAcquire(&SSLMShmem->mutex);
		SSLMShmem->is_leader = false;

		/* check if leader has changed */
		if (strcmp(SSLMShmem->last_disk_copy.leader_conn, local_ctrl.leader_conn) != 0)
			need_reconnect = true;

		SSLMShmem->last_disk_copy = local_ctrl;
		SpinLockRelease(&SSLMShmem->mutex);

		/* new leader has selected, we need to connect to the new leader */
		if (need_reconnect)
		{
			ereport(LOG, (errmsg("detected new primary IP: %s, signaling walreceiver", local_ctrl.leader_conn)));
			/*
			 * TODO: send a signal to walreceiver to restart
			 */
		}

		/* check if the current lease is valid */
		if (now - local_ctrl.heartbeat_ts > LEADER_LEASE_TIMEOUT)
		{
			/* uint64 old_gen; */
			ereport(LOG, (errmsg("primary heartbeat timeout, attempting CAS election...")));

			local_ctrl.generation++;
			local_ctrl.leader_node_id = 1;
			local_ctrl.heartbeat_ts = now;
			snprintf(local_ctrl.leader_conn, 256, "host=192.168.1.100 port=5432");

			/*
			if (Atomic_CAS_Disk(lock_fd, old_gen, &local_ctrl))
			{
				ereport(LOG, (errmsg("CAS Success! Promoting this instance.")));
				DirectlyTriggerPromotion();
			}
			*/
		}
	}
}

/*
 * Do something like pg_promote()
 */
static bool
TryPromoteSlave(bool wait, int wait_seconds)
{
	FILE	   *promote_file;
	int			i;

	if (!RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is not in progress"),
				 errhint("Recovery control functions can only be executed during recovery.")));

	if (wait_seconds <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("\"wait_seconds\" must not be negative or zero")));

	/* create the promote signal file */
	promote_file = AllocateFile(PROMOTE_SIGNAL_FILE, "w");
	if (!promote_file)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						PROMOTE_SIGNAL_FILE)));

	if (FreeFile(promote_file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						PROMOTE_SIGNAL_FILE)));

	/* signal the postmaster */
	if (kill(PostmasterPid, SIGUSR1) != 0)
	{
		(void) unlink(PROMOTE_SIGNAL_FILE);
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("failed to send signal to postmaster: %m")));
	}

	/* return immediately if waiting was not requested */
	if (!wait)
		return true;

	/* wait for the amount of time wanted until promotion */
#define WAITS_PER_SECOND 10
	for (i = 0; i < WAITS_PER_SECOND * wait_seconds; i++)
	{
		int			rc;

		ResetLatch(MyLatch);

		if (!RecoveryInProgress())
			true;

		CHECK_FOR_INTERRUPTS();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000L / WAITS_PER_SECOND,
					   WAIT_EVENT_PROMOTE);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating connection due to unexpected postmaster exit"),
					 errcontext("while waiting on promotion")));
	}

	ereport(WARNING,
			(errmsg_plural("server did not promote within %d second",
						   "server did not promote within %d seconds",
						   wait_seconds,
						   wait_seconds)));
	return false;
}

/*
 * Entry point for arbiter process.
 */
void
ArbiterMain(const void *startup_data, size_t startup_data_len)
{
	bool continue_sleep;
	bool do_promote = false;
	Assert(startup_data_len == 0);

	continue_sleep = false;
	do {
		sleep(1);
	} while (continue_sleep);

	AuxiliaryProcessMainCommon();

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/* Reset some signals that are accepted by postmaster but not here */
	pqsignal(SIGCHLD, SIG_DFL);

	ereport(LOG, (errmsg("Arbiter process started %d", MyProcPid)));
	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);
	OwnLatch(&SSLMShmem->proc_latch);
	/*
	 * Loop forever
	 */
	for (;;)
	{
		int rc;

		/* Process any signals received recently */
		ProcessArbiterInterrupts();

		/* check if walreceiver process is live */
		if (!WalRcvRunning() && RecoveryInProgress())
		{
			/*
			 * Now we simply assume the primary has gone away.
			 * Then we promote the slave to be a new primary.
			 * XXX  in shared storage, we should check the new
			 * leader is whether ourself or not.
			 */
			
			if (!do_promote)
			{
				elog(LOG, "Try to promote a slave");
				do_promote = TryPromoteSlave(false, 1);
			}
			else
				elog(LOG, "after promoting, still in recovery");
		}

		/* sleep... */
		rc = WaitLatch(&SSLMShmem->proc_latch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000L, PG_WAIT_EXTENSION);
		ResetLatch(&SSLMShmem->proc_latch);
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}