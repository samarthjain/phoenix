package com.salesforce.hbase.index.builder.covered;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Codec for creating index updates from the current state of a table
 */
public interface IndexCodec {

  // JY: used for both batch case and covering delete case
  /**
   * Get the index cleanup entries. Currently, this must return just single row deletes (where just
   * the row-key is specified and no columns are returned) mapped to the table name. For instance,
   * to you have an index 'myIndex' with row :
   * 
   * <pre>
   * v1,v2,v3 | CF:CQ0  | rowkey
   *          | CF:CQ1  | rowkey
   * </pre>
   * 
   * To then cleanup this entry, you would just return 'v1,v2,v3', 'myIndex'.
   * @param state the current state of the table that needs to be cleaned up. Generally, you only
   *          care about the latest column values, for each column you are indexing for each index
   *          table.
   * @return the pairs of (deletes, index table name) that should be applied.
   */
  public Iterable<Pair<Delete, byte[]>> getIndexDeletes(TableState state);

  // table state has the pending update already applied, before calling
  // get the new index entries
  /**
   * Get the index updates for the primary table state, for each index table. The returned
   * {@link Put}s need to be fully specified (including timestamp) to minimize passes over the same
   * key-values multiple times. Generally, you should specify the same timestamps on the Put as
   * {@link TableState#getCurrentTimestamp()} so the index entries match the primary table row.
   * @param state the current state of the table that needs to an index update Generally, you only
   *          care about the latest column values, for each column you are indexing for each index
   *          table.
   * @return the pairs of (updates,index table name) that should be applied.
   */
  public Iterable<Pair<Put, byte[]>> getIndexUpserts(TableState state);
  // JY: after this, internally checks the current TS vs the put TS and push a delete if we need to
  // (see coveredColumnCodec)
}