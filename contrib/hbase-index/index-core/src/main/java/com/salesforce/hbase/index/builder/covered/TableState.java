package com.salesforce.hbase.index.builder.covered;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

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

  // use this to get the cf:cq as of the current timestamp
  public Iterator<KeyValue> getTableState(List<ColumnReference> columns) throws IOException;

  /**
   * @return the attributes attached to the current update (e.g. {@link Mutation}).
   */
  public Map<String, byte[]> getUpdateAttributes();
}
