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
#include <sys/types.h>
#include <sys/stat.h>


#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
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
#include "utils/guc_hooks.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#define LEADER_LEASE_TIMEOUT 10
#define CLUSTER_CONTROL_FILE "/tmp/tac_cluster"
/* Magic for on disk files. */
#define ARBITER_STATE_MAGIC ((uint32) 0x1257DADE)

/* GUC parameters */
int node_id = 1;
int cluster_role = unknown;


/* Options for cluster_role. */
const struct config_enum_entry cluster_role_options[] = {
	{"unknown", unknown, false},
	{"primary", primary, false},
	{"slave", slave, false},
	{NULL, 0, false}
};

static ArbiterShmemState *ArbiterState = NULL;

static void ProcessArbiterInterrupts();
static bool TryPromoteSlave(bool wait, int wait_seconds);
static AlterSystemStmt* CreateDummyASCommand(uint64 gen, int leader_id);

extern bool Atomic_CAS_Disk(int fd, uint64 old_gen, ArbiterControlData *new_ctrl);

/* GUC check_hook for node_id */
bool
check_node_id(int *newval, void **extra, GucSource source)
{
	if (*newval >= 1 && *newval <= 32)
		return true;
	
	return false;
}

/*
 * Amount of shared memory required for this module.
 */
Size
ArbiterShmemSize(void)
{
	return sizeof(ArbiterShmemState);
}

/*
 * Create or attach to shared memory segment for this module.
 */
void
ArbiterShmemInit(void)
{
	bool found;

	ArbiterState = (ArbiterShmemState *)
		ShmemInitStruct("Arbiter State", ArbiterShmemSize(), &found);

	if (!found)
	{
		memset(ArbiterState, 0, sizeof(ArbiterShmemState));
		SpinLockInit(&ArbiterState->mutext);
		ArbiterState->ctl_data.magic = ARBITER_STATE_MAGIC;
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

static AlterSystemStmt*
CreateDummyASCommand(uint64 gen, int leader_id)
{
	char conninfo[256];
	int tmp_generate;
	int tmp_leader_id;
	AlterSystemStmt *ass;
	VariableSetStmt *vss;

	SpinLockAcquire(&ArbiterState->mutext);
	tmp_generate = ArbiterState->ctl_data.generation;
	tmp_leader_id = ArbiterState->ctl_data.leader_node_id;
	memcpy(conninfo, ArbiterState->ctl_data.leader_conn, 256);
	SpinLockRelease(&ArbiterState->mutext);

	/* double check leader doesn't change */
	if (tmp_generate != gen || tmp_leader_id != leader_id)
		return NULL;

	ass = makeNode(AlterSystemStmt);
	vss = makeNode(VariableSetStmt);
	vss->kind = VAR_SET_VALUE;
	vss->location = -1;
	vss->name = "primary_conninfo";
	vss->args = list_make1(makeStringConst(conninfo, -1));
	vss->is_local = false;
	ass->setstmt = vss;

	return ass;
}

/*
 * Do something like pg_promote()
 */
static bool
TryPromoteSlave(bool wait, int wait_seconds)
{
	FILE		*promote_file;
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
	int fd;
	int r;
	bool do_promote = false;
	struct stat stat_buf;
	int local_primary_id = 0;
	pg_time_t now;
	uint64 local_generation = 0;
	cluster_type local_cluster_role = cluster_role;
	bool need_reconnect = false;
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

	if (local_cluster_role == primary)
	{
		now = (pg_time_t) time(NULL);
		if (stat(CLUSTER_CONTROL_FILE, &stat_buf) == 0)
		{
			fd = BasicOpenFile(CLUSTER_CONTROL_FILE,
					   O_RDWR | PG_BINARY);
			if (fd < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
				 		 errmsg("could not open file \"%s\": %m",
							CLUSTER_CONTROL_FILE)));
			r = read(fd, &ArbiterState->ctl_data, sizeof(ArbiterControlData));
			if (r != sizeof(ArbiterControlData))
				ereport(PANIC,
						(errcode_for_file_access(),
					 	 errmsg("could not read file \"%s\": %m",
							CLUSTER_CONTROL_FILE)));
			/* check the data in the cluster control file */
			if (node_id != ArbiterState->ctl_data.leader_node_id)
				elog(PANIC, "node_id should equal with value in the cluster control file");
			SpinLockAcquire(&ArbiterState->mutext);
			ArbiterState->ctl_data.heartbeat_ts = now;
			SpinLockRelease(&ArbiterState->mutext);
			if (write(fd, &ArbiterState->ctl_data, sizeof(ArbiterControlData)) !=
					  sizeof(ArbiterControlData))
			{
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not write to file \"%s\": %m",
							CLUSTER_CONTROL_FILE)));
			}
			if (close(fd) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not close file \"%s\": %m",
								CLUSTER_CONTROL_FILE)));
		}
		else
		{
			/* no found cluster control file */
			if (errno != ENOENT)
				ereport(PANIC,
						(errcode_for_file_access(),
					 	 errmsg("could not stat file \"%s\": %m",
								CLUSTER_CONTROL_FILE)));
			/* Cluster is first startup, we should create the control file */
			fd = BasicOpenFile(CLUSTER_CONTROL_FILE,
					   O_RDWR | PG_BINARY);
			if (fd < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
				 		 errmsg("could not open file \"%s\": %m",
							CLUSTER_CONTROL_FILE)));
			SpinLockAcquire(&ArbiterState->mutext);
			ArbiterState->ctl_data.heartbeat_ts = now;
			ArbiterState->ctl_data.generation++;
			ArbiterState->ctl_data.leader_node_id = node_id;
			snprintf(ArbiterState->ctl_data.leader_conn, 256,"user=%s host=%s port=%d",
					 "ubuntu", "localhost", PostPortNumber);
			SpinLockRelease(&ArbiterState->mutext);
			if (write(fd, &ArbiterState->ctl_data, sizeof(ArbiterControlData)) !=
					  sizeof(ArbiterControlData))
			{
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not write to file \"%s\": %m",
							CLUSTER_CONTROL_FILE)));
			}
			if (close(fd) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not close file \"%s\": %m",
								CLUSTER_CONTROL_FILE)));
		}
	}

	/*
	 * Loop forever
	 */
	for (;;)
	{
		int rc;

		/* Process any signals received recently */
		ProcessArbiterInterrupts();
		
		if (local_cluster_role == primary)
		{
			/* update hearbeat */
		}
		else
		{
			/* read cluster control file */
			fd = BasicOpenFile(CLUSTER_CONTROL_FILE,
					   O_RDWR | PG_BINARY);
			if (fd < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
				 		 errmsg("could not open file \"%s\": %m",
							CLUSTER_CONTROL_FILE)));
			r = read(fd, &ArbiterState->ctl_data, sizeof(ArbiterControlData));
			if (r != sizeof(ArbiterControlData))
				ereport(PANIC,
						(errcode_for_file_access(),
					 	 errmsg("could not read file \"%s\": %m",
							CLUSTER_CONTROL_FILE)));
			/* check the data in the cluster control file */
			if (node_id == ArbiterState->ctl_data.leader_node_id)
				elog(PANIC, "node_id should not equal with value in the cluster control file");
			SpinLockAcquire(&ArbiterState->mutext);
			local_primary_id = ArbiterState->ctl_data.leader_node_id;
			local_generation = ArbiterState->ctl_data.generation;
			SpinLockRelease(&ArbiterState->mutext);
			if (close(fd) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not close file \"%s\": %m",
								CLUSTER_CONTROL_FILE)));
		}

		/* check if walreceiver process is live */
		if (!(local_cluster_role == primary) &&
			!WalRcvRunning() &&
			RecoveryInProgress())
		{
			/* Primary has gone away, try to be the new primary */
			struct flock fl;
			fd = BasicOpenFile(CLUSTER_CONTROL_FILE,
					   O_RDWR | PG_BINARY);
			if (fd < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
				 		 errmsg("could not open file \"%s\": %m",
							CLUSTER_CONTROL_FILE)));

			fl.l_type = F_WRLCK;
			fl.l_whence = SEEK_SET;
			fl.l_start = 0;
			fl.l_len = sizeof(ArbiterControlData);
			if (fcntl(fd, F_SETLK, &fl) == -1)
			{
				close(fd);
				goto WAIT_MOMENT;
			}
			r = read(fd, &ArbiterState->ctl_data, sizeof(ArbiterControlData));
			if (r != sizeof(ArbiterControlData))
				ereport(PANIC,
						(errcode_for_file_access(),
					 	 errmsg("could not read file \"%s\": %m",
							CLUSTER_CONTROL_FILE)));
			SpinLockAcquire(&ArbiterState->mutext);
			if (local_generation == ArbiterState->ctl_data.generation &&
				local_primary_id == ArbiterState->ctl_data.leader_node_id)
			{
				ArbiterState->ctl_data.generation++;
				ArbiterState->ctl_data.leader_node_id = node_id;
				ArbiterState->ctl_data.heartbeat_ts = (pg_time_t) time(NULL);
				snprintf(ArbiterState->ctl_data.leader_conn, 256,"user=%s host=%s port=%d",
					 "ubuntu", "localhost", PostPortNumber);
				local_cluster_role = primary;
			}
			else
			{
				/* leader has changed */
				/* walreceiver needs to connect the new leader */
				need_reconnect = true;
			}
			local_generation = ArbiterState->ctl_data.generation;
			local_primary_id = ArbiterState->ctl_data.leader_node_id;
			SpinLockRelease(&ArbiterState->mutext);
			if (local_cluster_role == primary)
			{
				/* We are the new leader */
				if (write(fd, &ArbiterState->ctl_data, sizeof(ArbiterControlData)) !=
					  sizeof(ArbiterControlData))
				{
					ereport(PANIC,
							(errcode_for_file_access(),
							 errmsg("could not write to file \"%s\": %m",
								CLUSTER_CONTROL_FILE)));
				}
			}
			fl.l_type = F_UNLCK;
			fcntl(fd, F_SETLK, &fl);
			if (close(fd) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not close file \"%s\": %m",
								CLUSTER_CONTROL_FILE)));
			/*
			 * Now we simply assume the primary has gone away.
			 * Then we promote the slave to be a new primary.
			 * XXX  in shared storage, we should check the new
			 * leader is whether ourself or not.
			 */
			if (!do_promote && (local_cluster_role == primary))
			{
				/*We are selected to the new leader */
				/* We shoulde promote ourself */
				elog(LOG, "Try to promote a slave");
				do_promote = TryPromoteSlave(false, 1);
			}
			else if (need_reconnect)
			{
				/* We are not selected to be the leader
				 * We must reconnect the new leader
				 */
				AlterSystemStmt *ass = CreateDummyASCommand(local_generation, local_primary_id);
				AlterSystemSetConfigFile(ass);
				kill(PostmasterPid, SIGHUP);
			}
		}

WAIT_MOMENT:
		/* sleep... */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}