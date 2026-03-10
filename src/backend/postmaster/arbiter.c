/*
 * arbiter.c
 * 		Shared storage lock manager
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
#include "storage/arbiter.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

#define LEADER_LEASE_TIMEOUT 10

static SSLMSharedState *SSLMShmem = NULL;

static void ProcessArbiterInterrupts();

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
	if (ShutdownRequestPending)
	{
		elog(LOG, "arbiter process shutting down");
		proc_exit(0);
	}
}

/*
 * Entry point for arbiter process.
 */
void
ArbiterMain(const void *startup_data, size_t startup_data_len)
{
	SharedStorageControl local_ctrl;
	bool continue_sleep;
	Assert(startup_data_len == 0);

	continue_sleep = false;
	do {
		sleep(1);
	} while (continue_sleep);

	AuxiliaryProcessMainCommon();

	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);

	/*
	lock_fd = open("/dev/mapper/shared_disk_vdev", O_RDWR | O_DIRECT);
	if (lock_fd < 0)
		ereport(ERROR, (errmsg("could not open shared storage lock device")));
	*/

	ereport(LOG, (errmsg("SSLM process started %d", MyProcPid)));
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
		time_t now;
		bool am_i_primary;

		/* Process any signals received recently */
		ProcessSSLMInterrupts();

		/* read cluster control file from HSM */
		/*
		if (pread(lock_fd, &local_ctrl, sizeof(SharedStorageControl), 0) != sizeof(SharedStorageControl))
		{
			ereport(WARNING, (errmsg("could not read shared storage control block")));
			goto wait_step;
		}
		*/
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

		/* sleep... */
		rc = WaitLatch(&SSLMShmem->proc_latch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000L, PG_WAIT_EXTENSION);
		ResetLatch(&SSLMShmem->proc_latch);
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}