/*-------------------------------------------------------------------------
 *
 * memtable.c
 *	  in-memtable table access method
 *
 * Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 *	  contrib/memtable/memtable.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/skey.h"
#include "access/tableam.h"
#include "fmgr.h"

PG_MODULE_MAGIC;



static const TupleTableSlotOps *
memtable_slot_callbacks(Relation relation)
{
	return NULL;
}

static TableScanDesc
memtable_beginscan(Relation relation, Snapshot snapshot,
				   int nkeys, ScanKey key,
				   ParallelTableScanDesc parallel_scan,
				   uint32 flags)
{
	return NULL;
}

static void
memtable_endscan(TableScanDesc sscan)
{
	return;
}

static void
memtable_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
			bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	return;
}

static bool
memtable_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	return false;
}

static void
memtable_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
				  ItemPointer maxtid)
{
	return;
}

static bool
memtable_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
						  TupleTableSlot *slot)
{
	return false;
}

static Size
memtable_parallelscan_estimate(Relation rel)
{
	return 0;
}

static Size
memtable_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	return 0;
}

static void
memtable_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	return;
}

static IndexFetchTableData *
memtable_index_fetch_begin(Relation rel, uint32 flags)
{
	return NULL;
}

static void
memtable_index_fetch_reset(IndexFetchTableData *scan)
{
	/*
	 * Resets are a no-op.
	 *
	 * Deliberately avoid dropping pins now held in xs_cbuf and xs_vmbuffer.
	 * This saves cycles during certain tight nested loop joins (it can avoid
	 * repeated pinning and unpinning of the same buffer across rescans).
	 */
}

static void
memtable_index_fetch_end(IndexFetchTableData *scan)
{

}

static bool
memtable_index_fetch_tuple(struct IndexFetchTableData *scan,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 bool *heap_continue, bool *all_dead)
{
	return false;
}

static void
memtable_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					uint32 options, BulkInsertState bistate)
{

}

static void
memtable_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								CommandId cid, uint32 options,
								BulkInsertState bistate, uint32 specToken)
{

}

static void
memtable_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
								  uint32 specToken, bool succeeded)
{

}

static void
memtable_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
				  CommandId cid, uint32 options, BulkInsertState bistate)
{

}

static TM_Result
memtable_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					uint32 options, Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd)
{
	return TM_Ok;
}

static TM_Result
memtable_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					CommandId cid, uint32 options,
					Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd,
					LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	return TM_Ok;
}

static TM_Result
memtable_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
				  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				  LockWaitPolicy wait_policy, uint8 flags,
				  TM_FailureData *tmfd)
{
	return TM_Ok;
}

static bool
memtable_fetch_row_version(Relation relation,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot)
{
	return false;
}

static void
memtable_get_latest_tid(TableScanDesc sscan,
					ItemPointer tid)
{

}

static bool
memtable_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return false;
}

static bool
memtable_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								Snapshot snapshot)
{
	return false;
}

static TransactionId
memtable_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	return InvalidTransactionId;
}

static void
memtable_relation_set_new_filelocator(Relation rel,
									const RelFileLocator *newrlocator,
									char persistence,
									TransactionId *freezeXid,
									MultiXactId *minmulti)
{

}

static void
memtable_relation_nontransactional_truncate(Relation rel)
{

}

static void
memtable_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{

}

static void
memtable_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								 Relation OldIndex, bool use_sort,
								 TransactionId OldestXmin,
								 Snapshot snapshot,
								 TransactionId *xid_cutoff,
								 MultiXactId *multi_cutoff,
								 double *num_tuples,
								 double *tups_vacuumed,
								 double *tups_recently_dead)
{

}

static void
memtable_vacuum_rel(Relation rel, const VacuumParams *params,
				BufferAccessStrategy bstrategy)
{

}

static bool
memtable_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	return false;
}

static bool
memtable_scan_analyze_next_tuple(TableScanDesc scan,
							   double *liverows, double *deadrows,
							   TupleTableSlot *slot)
{
	return false;
}

static double
memtable_index_build_range_scan(Relation heapRelation,
							  Relation indexRelation,
							  IndexInfo *indexInfo,
							  bool allow_sync,
							  bool anyvisible,
							  bool progress,
							  BlockNumber start_blockno,
							  BlockNumber numblocks,
							  IndexBuildCallback callback,
							  void *callback_state,
							  TableScanDesc scan)
{
	return 0.0;
}

static void
memtable_index_validate_scan(Relation heapRelation,
						   Relation indexRelation,
						   IndexInfo *indexInfo,
						   Snapshot snapshot,
						   ValidateIndexState *state)
{

}

static uint64
memtable_relation_size(Relation rel, ForkNumber forkNumber)
{
	return 0;
}

static bool
memtable_relation_needs_toast_table(Relation rel)
{
	return false;
}

static Oid
memtable_relation_toast_am(Relation rel)
{
	return InvalidOid;
}

static void
memtable_fetch_toast_slice(Relation toastrel, Oid8 valueid, int32 attrsize,
					   int32 sliceoffset, int32 slicelength,
					   varlena *result)
{

}

static void
memtable_estimate_rel_size(Relation rel, int32 *attr_widths,
						 BlockNumber *pages, double *tuples,
						 double *allvisfrac)
{

}

static bool
memtable_scan_bitmap_next_tuple(TableScanDesc scan,
							  TupleTableSlot *slot,
							  bool *recheck,
							  uint64 *lossy_pages,
							  uint64 *exact_pages)
{
	return false;
}

static bool
memtable_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	return false;
}

static bool
memtable_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
							  TupleTableSlot *slot)
{
	return false;
}

/* ------------------------------------------------------------------------
 * Definition of the in-memory table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine memtable_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = memtable_slot_callbacks,

	.scan_begin = memtable_beginscan,
	.scan_end = memtable_endscan,
	.scan_rescan = memtable_rescan,
	.scan_getnextslot = memtable_getnextslot,

	.scan_set_tidrange = memtable_set_tidrange,
	.scan_getnextslot_tidrange = memtable_getnextslot_tidrange,

	.parallelscan_estimate = memtable_parallelscan_estimate,
	.parallelscan_initialize = memtable_parallelscan_initialize,
	.parallelscan_reinitialize = memtable_parallelscan_reinitialize,

	.index_fetch_begin = memtable_index_fetch_begin,
	.index_fetch_reset = memtable_index_fetch_reset,
	.index_fetch_end = memtable_index_fetch_end,
	.index_fetch_tuple = memtable_index_fetch_tuple,

	.tuple_insert = memtable_tuple_insert,
	.tuple_insert_speculative = memtable_tuple_insert_speculative,
	.tuple_complete_speculative = memtable_tuple_complete_speculative,
	.multi_insert = memtable_multi_insert,
	.tuple_delete = memtable_tuple_delete,
	.tuple_update = memtable_tuple_update,
	.tuple_lock = memtable_tuple_lock,

	.tuple_fetch_row_version = memtable_fetch_row_version,
	.tuple_get_latest_tid = memtable_get_latest_tid,
	.tuple_tid_valid = memtable_tuple_tid_valid,
	.tuple_satisfies_snapshot = memtable_tuple_satisfies_snapshot,
	.index_delete_tuples = memtable_index_delete_tuples,

	.relation_set_new_filelocator = memtable_relation_set_new_filelocator,
	.relation_nontransactional_truncate = memtable_relation_nontransactional_truncate,
	.relation_copy_data = memtable_relation_copy_data,
	.relation_copy_for_cluster = memtable_relation_copy_for_cluster,
	.relation_vacuum = memtable_vacuum_rel,
	.scan_analyze_next_block = memtable_scan_analyze_next_block,
	.scan_analyze_next_tuple = memtable_scan_analyze_next_tuple,
	.index_build_range_scan = memtable_index_build_range_scan,
	.index_validate_scan = memtable_index_validate_scan,

	.relation_size = memtable_relation_size,
	.relation_needs_toast_table = memtable_relation_needs_toast_table,
	.relation_toast_am = memtable_relation_toast_am,
	.relation_fetch_toast_slice = memtable_fetch_toast_slice,

	.relation_estimate_size = memtable_estimate_rel_size,

	.scan_bitmap_next_tuple = memtable_scan_bitmap_next_tuple,
	.scan_sample_next_block = memtable_scan_sample_next_block,
	.scan_sample_next_tuple = memtable_scan_sample_next_tuple
};

PG_FUNCTION_INFO_V1(memtable_handler);

Datum
memtable_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&memtable_methods);
}