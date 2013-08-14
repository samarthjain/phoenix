package com.salesforce.hbase.index.builder.covered;

import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

/**
 * Codec for creating index updates from the current state of a table
 */
public interface IndexCodec {

  // check current state, create delete(s) for the current row
  // JY: used for both batch case and covering delete case
  public Iterable<Delete> cleanupOldValues(Mutation update, TableState state);

  // table state has the pending update already applied, before calling
  // get the new index entries
  public Iterable<Put> getIndexUpserts(Map<String, byte[]> updateAttributes, TableState state);
  // JY: after this, internally checks the current TS vs the put TS and push a delete if we need to
  // (see coveredColumnCodec)
}