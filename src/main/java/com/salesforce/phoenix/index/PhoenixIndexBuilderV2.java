/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.index;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.salesforce.hbase.index.builder.BaseIndexBuilder;
import com.salesforce.hbase.index.builder.covered.CoveredColumnIndexer;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTableImpl;
import com.salesforce.phoenix.util.IndexUtil;

/**
 * Build covered indexes for phoenix updates.
 * <p>
 * Before any call to prePut/preDelete, the row has already been locked. This ensures that we don't
 * need to do any extra synchronization in the IndexBuilder.
 * <p>
 * This is a very simple mechanism that doesn't do covered indexes (as in
 * {@link CoveredColumnIndexer}), but just serves as a starting point for implementing comprehensive
 * indexing in phoenix.
 * <p>
 * NOTE: This implementation doesn't cleanup the index when we remove a key-value on compaction or
 * flush, leading to a bloated index that needs to be cleaned up by a background process.
 */
public class PhoenixIndexBuilderV2 extends BaseIndexBuilder {

  private static final Log LOG = LogFactory.getLog(PhoenixIndexBuilderV2.class);
  private static final String PTABLE_ATTRIBUTE_KEY = "phoenix.ptable.primary";
  private static final String INDEX_PTABLE_ATTRIBUTE_KEY = "phoenix.ptable.index";
  private RegionCoprocessorEnvironment env;
  private volatile HTableInterface localTable;

  @Override
  public void setup(RegionCoprocessorEnvironment env) throws IOException {
    this.env = env;
  }

  /**
   * Ensure we have a connection to the local table. We need to do this after
   * {@link #setup(RegionCoprocessorEnvironment)} because we are created on region startup and the
   * table isn't actually accessible until later.
   * @throws IOException if we can't reach the table
   */
  private void ensureLocalTable() throws IOException {
    if (this.localTable == null) {
      synchronized (this) {
        if (this.localTable == null) {
          localTable = env.getTable(env.getRegion().getTableDesc().getName());
        }
      }
    }
  }

  private Map<byte[], Result> currentRowCache = new TreeMap<byte[], Result>(Bytes.BYTES_COMPARATOR);

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Put p) throws IOException {
    //check to see if this put is the first in a batch
    if (matchesKnownBatch(p)) {
      byte[] sourceRow = p.getRow();
      Result currentRow = currentRowCache.get(sourceRow);
      // we haven't seen this row before, so look it up
      if (currentRow == null) {
        currentRow = getCurrentRow(sourceRow);
        // stick it back in the cache
        this.currentRowCache.put(sourceRow, currentRow);
      }

      return getIndexUpdateForMutationWithCurrentRow(p, currentRow);
    }


    //
    return getIndexUpdatesForMutation(p);
  }


  /**
   * Get the index updates that need to be made for the current row
   * @param p
   * @param currentRow
   * @return
   */
  private Collection<Pair<Mutation, String>> getIndexUpdateForMutationWithCurrentRow(Put p,
      Result currentRow) {
    // TODO Implement PhoenixIndexBuilderV2.getIndexUpdateForMutationWithCurrentRow
    return null;
  }

  /**
   * Check to see if the update is a batch-based {@link Mutation}, in which case we want to use a
   * row cache to avoid N look-ups to the row.
   * @param m mutation to check for a batch-id
   * @return <tt>true</tt> if the mutation is a batch mutation we know about
   */
  private boolean matchesKnownBatch(Mutation m) {
    // TODO Implement PhoenixIndexBuilderV2.matchesKnownBatch
    return false;
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Delete d) throws IOException {
    return getIndexUpdatesForMutation(d);
  }
  
  private Collection<Pair<Mutation, String>> getIndexUpdatesForMutation(Mutation m)
      throws IOException {
    // deserialize the ptable for the put
    byte[] serializedPtable = m.getAttribute(PTABLE_ATTRIBUTE_KEY);
    PTable table = new PTableImpl();
    table.readFields(new DataInputStream(new ByteArrayInputStream(serializedPtable)));
    // read in the index table
    serializedPtable = m.getAttribute(INDEX_PTABLE_ATTRIBUTE_KEY);
    PTable indexTable = new PTableImpl();
    indexTable.readFields(new DataInputStream(new ByteArrayInputStream(serializedPtable)));

    //generate the index updates 
    try {
      List<Mutation> mutations =
          IndexUtil.generateIndexData(table, indexTable, Lists.<Mutation> newArrayList(m));
      List<Pair<Mutation, String>> updates =
          new ArrayList<Pair<Mutation, String>>(mutations.size());
      // XXX is this right? Seems a bit convoluted for the real table name...
      String indexTableName = Bytes.toString(indexTable.getName().getBytes());
      for (Mutation update : mutations) {
        updates.add(new Pair<Mutation, String>(update, indexTableName));
      }
      return updates;

    } catch (SQLException e) {
      throw new IOException("Failed to build index updates from update!", e);
    }
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered) throws IOException {
    // TODO Implement IndexBuilder.getIndexUpdateForFilteredRows
    return null;
  }

  /**
   * @param sourceRow row key to extract
   * @return the full state of the given row. Includes all current versions (even if they are not
   *         usually visible to the client (unless they are also doing a raw scan)).
   */
  private Result getCurrentRow(byte[] sourceRow) throws IOException {
    Scan s = new Scan(sourceRow, sourceRow);
    s.setRaw(true);
    s.setMaxVersions();
    ResultScanner results = localTable.getScanner(s);
    Result r = results.next();
    assert results.next() == null : "Got more than one result when scanning"
        + " a single row in the primary table!";
    results.close();
    return r;
  }
}