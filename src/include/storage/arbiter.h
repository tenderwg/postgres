/*
 * arbiter.h
 *	  Cluster monitor and new leader selector
 *
 * src/include/storage/arbiter.h
 */
#ifndef ARBITER_H
#define ARBITER_H

#include "storage/latch.h"
#include "storage/shmem.h"
#include "storage/spin.h"

extern PGDLLIMPORT int node_id;

typedef enum cluster_type
{
	unknown,
	primary,
	slave,
} cluster_type;

extern PGDLLIMPORT int cluster_role;

/* Shared-memory state for arbiter */
typedef struct ArbiterControlData
{
	uint64		magic;
	uint64		generation;
	uint32		leader_node_id;
	pg_time_t	heartbeat_ts;
	char		leader_conn[256]; 
} ArbiterControlData;

typedef struct ArbiterShmemState
{
	ArbiterControlData  ctl_data;
	slock_t		mutext;
} ArbiterShmemState;

extern void ArbiterMain(const void *startup_data, size_t startup_data_len);
extern size_t ArbiterShmemSize(void);
extern void ArbiterShmemInit(void);

#endif