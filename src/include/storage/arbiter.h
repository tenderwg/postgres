/*
 * arbiter.h
 *	  Cluster monitor and leader selector
 *
 * src/include/storage/arbiter.h
 */
#ifndef ARBITER_H
#define ARBITER_H

#include "storage/latch.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/* Shared-memory state for arbiter */
typedef struct SharedStorageControl {
	uint64      magic;
	uint64      generation;
	uint32      leader_node_id;
	uint64      heartbeat_ts;
	char        leader_conn[256]; 
	uint8       padding[232];
} SharedStorageControl;


typedef struct SSLMSharedState {
	slock_t     mutex;
	SharedStorageControl last_disk_copy;
	bool        is_leader;
	Latch       proc_latch;
} SSLMSharedState;

extern void ArbiterMain(const void *startup_data, size_t startup_data_len);
extern size_t SSLMShmemSize(void);
extern void SSLMShmemInit(void);

#endif