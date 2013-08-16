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

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * Lazy wrapper around the local HTable and a per-rowkey cache. Uses the row cache to minimize
 * lookups for a given row over the course of a given batch and lazy instantiation of the connection
 * the local HTable (for cases that don't even even need to access local table state).
 */
public class LocalTable {
  private static final Log LOG = LogFactory.getLog(LocalTable.class);

  private volatile HTableInterface localTable;
  private RegionCoprocessorEnvironment env;
  private Map<byte[], Result> rowCache;
  private BatchCache batchCache;

  public LocalTable(RegionCoprocessorEnvironment env, Map<byte[], Result> rowCache,
      BatchCache batchCache) {
    this.env = env;
    this.rowCache = rowCache;
    this.batchCache = batchCache;
  }

  /**
   * @param m mutation for which we should get the current table state
   * @return the full state of the given row. Includes all current versions (even if they are not
   *         usually visible to the client (unless they are also doing a raw scan)).
   * @throws IOException if there is an issue reading the row
   */
  public Result getCurrentRowState(Mutation m) throws IOException {
    // check to see if this put is the first in a batch
    boolean matchesBatch = this.batchCache.matchesKnownBatch(m);
    Result currentRow = null;
    byte[] sourceRow = m.getRow();
    if (matchesBatch) {
      currentRow = rowCache.get(sourceRow);
    }

    // we haven't seen this row before, so look it up
    if (currentRow == null) {
      currentRow = getCurrentRow(sourceRow);
      if (matchesBatch) {
        // stick it back in the cache
        this.rowCache.put(sourceRow, currentRow);
      }
    }

    return currentRow;
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
    ResultScanner results = getLocalTable().getScanner(s);
    Result r = results.next();
    assert results.next() == null : "Got more than one result when scanning"
        + " a single row in the primary table!";
    results.close();
    return r;
  }

  /**
   * Ensure we have a connection to the local table. We need to do this after
   * {@link #setup(RegionCoprocessorEnvironment)} because we are created on region startup and the
   * table isn't actually accessible until later.
   * @throws IOException if we can't reach the table
   */
  private HTableInterface getLocalTable() throws IOException {
    if (this.localTable == null) {
      synchronized (this) {
        if (this.localTable == null) {
          localTable = env.getTable(env.getRegion().getTableDesc().getName());
        }
      }
    }
    return this.localTable;
  }
}