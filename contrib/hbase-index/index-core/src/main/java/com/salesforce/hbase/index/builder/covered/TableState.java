package com.salesforce.hbase.index.builder.covered;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.ColumnTracker;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Interface for the current state of the table. This is generally going to be as of a timestamp - a view on the current state of the HBase table - so you don't have to worry about exposing too much information.
 */
public interface TableState {

  // use this to get batch ids/ptable stuff
  /**
   * WARNING: messing with this can affect the indexing plumbing. Use with caution :)
   * @return get the current environment in which this table lives.
   */
  public RegionCoprocessorEnvironment getEnvironment();

  /**
   * @return the current timestamp up-to-which we are releasing table state.
   */
  public long getCurrentTimestamp();

  /**
   * Set the current timestamp up to which the table should allow access to the underlying table.
   * This overrides the timestamp view provided by the indexer - use with care!
   * @param timestamp timestamp up to which the table should allow access.
   */
  public void setCurrentTimestamp(long timestamp);

  /**
   * @return the attributes attached to the current update (e.g. {@link Mutation}).
   */
  public Map<String, byte[]> getUpdateAttributes();

  // use this to get the cf:cq as of the current timestamp
  public Iterator<KeyValue> getNonIndexedColumnsTableState(List<ColumnReference> columns)
      throws IOException;

  /**
   * Get an iterator on the columns that will be indexed. This is similar to
   * {@link #getNonIndexedColumnsTableState(List)}, but should only be called for columns that you
   * need to index to ensure we can properly cleanup the index in the case of out of order updates.
   * As a side-effect, we update a timestamp for the next-most-recent timestamp for the columns you
   * request - you will never see a column with the timestamp we are tracking, but the next oldest
   * timestamp for that column.
   * <p>
   * If you are always guaranteed to get the most recent update on the index columns for the primary
   * table row (e.g. the client is not setting custom timestamps, but instead relying on the server
   * to set them), then you don't need to use this method and can instead just use
   * {@link #getNonIndexedColumnsTableState(List)}.
   * @param indexedColumns the columns to that will be indexed
   * @return an iterator over the columns and a general tracker for the {@link ColumnReference}s.
   *         The reference should be passed back up as part of the {@link IndexUpdate}.
   * @throws IOException
   */
  Pair<Iterator<KeyValue>, com.salesforce.hbase.index.builder.covered.ColumnTracker>
      getIndexedColumnsTableState(List<ColumnReference> indexedColumns) throws IOException;
}
