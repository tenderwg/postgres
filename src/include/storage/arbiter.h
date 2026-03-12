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

/* Shared-memory state for arbiter */
typedef struct ArbiterControlData {
	uint64      magic;
	uint64      generation;
	uint32      leader_node_id;
	uint64      heartbeat_ts;
	char        leader_conn[256]; 
	uint8       padding[232];
} ArbiterControlData;

typedef struct ArbiterSharedState {
	slock_t     mutex;
	ArbiterControlData last_disk_copy;
	bool        is_leader;
	Latch       proc_latch;
} ArbiterSharedState;

extern void ArbiterMain(const void *startup_data, size_t startup_data_len);
extern size_t SSLMShmemSize(void);
extern void SSLMShmemInit(void);

#endif