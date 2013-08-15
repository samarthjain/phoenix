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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.salesforce.hbase.index.builder.covered.ColumnReference;
import com.salesforce.hbase.index.builder.covered.TableState;

/**
 * Manage the state of the HRegion's view of the table, for the single row.
 */
public class LocalTableState implements TableState {

  private long ts;
  private RegionCoprocessorEnvironment env;
  private Map<String, byte[]> attributes;

  public LocalTableState(RegionCoprocessorEnvironment environment, Result currentState,
      Map<String, byte[]> updateAttributes) {
    this.env = environment;
    this.attributes = updateAttributes;
  }

  public void addUpdate(Collection<KeyValue> kvs) {
    // TODO implement LocalTableState #addUpdate
  }

  @Override
  public RegionCoprocessorEnvironment getEnvironment() {
    return this.env;
  }

  @Override
  public long getCurrentTimestamp() {
    return this.ts;
  }

  @Override
  public void setCurrentTimestamp(long timestamp) {
    this.ts = timestamp;
  }

  @Override
  public Iterator<KeyValue> getTableState(List<ColumnReference> columns) {
    ensureLocalStateInitialized();
    // TODO Implement TableState.getTableState
    return null;
  }
  
  /**
   * Initialize the managed local state. Generally, this will only be called by
   * {@link #getTableState(List)}, which is unlikely to be called concurrently from the outside.
   * Even then, there is still fairly low contention as each new Put/Delete will have its own table
   * state.
   */
  private synchronized void ensureLocalStateInitialized(){
    // TODO implement LocalTableState#ensureLocalStateInitialized
    //check the local store - is it initialized?
      // not initialized, but maybe the current row is in the row cache?
        // not in the row cache, so get it from the local table
  }

  @Override
  public Map<String, byte[]> getUpdateAttributes() {
    return this.attributes;
  }
}