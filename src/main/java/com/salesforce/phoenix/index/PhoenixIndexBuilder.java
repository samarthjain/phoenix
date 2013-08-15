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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.TreeMultimap;
import com.salesforce.hbase.index.builder.covered.CoveredColumnIndexer;
import com.salesforce.hbase.index.builder.covered.IndexCodec;

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
public class PhoenixIndexBuilder extends BaseCoveredColumnIndexer {

  private static final Log LOG = LogFactory.getLog(PhoenixIndexBuilder.class);
  private static final String CODEC_INSTANCE_KEY = "com.salesforce.hbase.index.codec.class";

  private Map<byte[], Result> currentRowCache = new TreeMap<byte[], Result>(Bytes.BYTES_COMPARATOR);
  private IndexCodec codec;

  @Override
  public void setup(RegionCoprocessorEnvironment env) throws IOException {
    super.setup(env);
    // setup the phoenix codec. Generally, this will just be in standard one, but abstracting here
    // so we can use it later when generalizing covered indexes
    Configuration conf = env.getConfiguration();
    Class<? extends IndexCodec> codecClass =
        conf.getClass(CODEC_INSTANCE_KEY, null, IndexCodec.class);
    try {
      Constructor<? extends IndexCodec> meth = codecClass.getDeclaredConstructor(new Class[0]);
      meth.setAccessible(true);
      this.codec = meth.newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Put p) throws IOException {
    Result currentRow = getCurrentRowState(p);
    return getIndexUpdateForMutationWithCurrentRow(p, currentRow);
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
    // build the index updates for each group
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();

    // create a state manager, so we can manage each batch
    LocalTableState tableState = new LocalTableState(env, currentRow, p.getAttributesMap());

    batchMutationAndAddUpdates(updateMap, tableState, p);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Found index updates for Put: " + updateMap);
    }
    return updateMap;
  }

  /**
   * Split the mutation into batches based on the timestamps of each keyvalue. We need to check each
   * key-value in the update to see if it matches the others. Generally, this will be the case, but
   * you can add kvs to a mutation that don't all have the timestamp, so we need to manage
   * everything in batches based on timestamp.
   * @param updateMap index updates into which to add new updates. Modified as a side-effect.
   * @param state current state of the row for the mutation.
   * @param m mutation to batch
   */
  private void batchMutationAndAddUpdates(List<Pair<Mutation, String>> updateMap,
      LocalTableState state, Mutation m) {
    // split the mutation into timestamp-based batches
    TreeMultimap<Long, KeyValue> batches = createTimestampBatchesFromFamilyMap(m);
    // go through each batch of keyvalues and build separate index entries for each
    for (Entry<Long, Collection<KeyValue>> batch : batches.asMap().entrySet()) {
      // update the table state to expose up to the batch's newest timestamp
      state.setCurrentTimestamp(batch.getKey());
      /*
       * We have to split the work between the cleanup and the update for each group because when we
       * update the current state of the row for the current batch (appending the mutations for the
       * current batch) the next group will see that as the current state, which will can cause the
       * a delete and a put to be created for the next group.
       */
      addMutationsForBatch(updateMap, batch, state);
    }
  }

  /**
   * @param updateMap
   * @param batch
   */
  private void addMutationsForBatch(Collection<Pair<Mutation, String>> updateMap,
      Entry<Long, Collection<KeyValue>> batch, LocalTableState state) {
    /*
     * Generally, the current update will be the most recent thing to be added. In that case, all we
     * need to is issue a delete for the previous index row (the state of the row, without the
     * update applied) at the current timestamp. This gets rid of anything currently in the index
     * for the current state of the row (at the timestamp).
     *
     * If things arrive out of order (we are using custom timestamps) we should still see the index
     * in the correct order (assuming we scan after the out-of-order update in finished). Therefore,
     * we when we aren't the most recent update to the index, we need to delete the state at the
     * current timestamp (similar to above), but also issue a delete for the added for at the next
     * newest timestamp of any of the columns in the update; we need to cleanup the insert so it
     * looks like it was also deleted at that newer timestamp. see the most recent update in the
     * index, even if we are making a put back in time (out of order).
     */

    // start by getting the cleanup for the current state of the
    long ts = batch.getKey();
    addDeleteUpdatesToMap(updateMap, state, ts);

    // add the current batch to the map
    state.addUpdate(batch.getValue());

    // get the updates to the current index
    Iterable<Pair<Put, byte[]>> upserts = codec.getIndexUpserts(state);
    if (upserts != null) {
      for (Pair<Put, byte[]> p : upserts) {
        // TODO replace this as just storing a byte[], to avoid all the String <-> byte[] swapping
        // HBase does
        String table = Bytes.toString(p.getSecond());
        updateMap.add(new Pair<Mutation, String>(p.getFirst(), table));
      }
    }
  }

  /**
   * Get the index deletes from the codec (IndexCodec{@link #getIndexDeletes(TableState)} and then add them to the update map.
   */
  private void addDeleteUpdatesToMap(Collection<Pair<Mutation, String>> updateMap,
      LocalTableState state, long ts) {
    Iterable<Pair<Delete, byte[]>> cleanup = codec.getIndexDeletes(state);
    if (cleanup != null) {
      for (Pair<Delete, byte[]> d : cleanup) {
        // override the timestamps in the delete to match the current batch.
        Delete remove = d.getFirst();
        remove.setTimestamp(ts);
        // TODO replace this as just storing a byte[], to avoid all the String <-> byte[] swapping
        // HBase does
        String table = Bytes.toString(d.getSecond());
        updateMap.add(new Pair<Mutation, String>(remove, table));
      }
    }
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Delete d) throws IOException {
    // this should probably be managed by the TableState. Easier to leave here for now. -jyates
    Result currenRow = getCurrentRowState(d);

    // stores all the return values
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();

    // We have to figure out which kind of delete it is, since we need to do different things if its
    // a general (row) delete, versus a delete of just a single column or family
    Map<byte[], List<KeyValue>> families = d.getFamilyMap();
    LocalTableState state = new LocalTableState(env, currenRow, d.getAttributesMap());

    // Option 1: its a row delete marker, so we just need to delete the most recent state for each
    // group, as of the specified timestamp in the delete
    if (families.size() == 0) {
      // get a consistent view of name
      long now = d.getTimeStamp();
      if (now == HConstants.LATEST_TIMESTAMP) {
        now = EnvironmentEdgeManager.currentTimeMillis();
        // update the delete's idea of 'now' to be consistent with the index
        d.setTimestamp(now);
      }
      // get deletes from the codec
      addDeleteUpdatesToMap(updateMap, state, now);
      return updateMap;
    }

    // Option 2: Its actually a bunch single updates, which can have different timestamps.
    // Therefore, we need to do something similar to the put case and batch by timestamp
    batchMutationAndAddUpdates(updateMap, state, d);
    return updateMap;
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered) throws IOException {
    // TODO Implement IndexBuilder.getIndexUpdateForFilteredRows
    return null;
  }

  @Override
  public void batchCompleted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) {
    // cleanup the row cache for each mutation
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Pair<Mutation, Integer> op = miniBatchOp.getOperation(i);
      Mutation m = op.getFirst();
      this.currentRowCache.remove(m.getRow());
    }
  }

  private Result getCurrentRowState(Mutation m) throws IOException {
    // check to see if this put is the first in a batch
    boolean matchesBatch = matchesKnownBatch(m);
    Result currentRow = null;
    byte[] sourceRow = m.getRow();
    if (matchesBatch) {
      currentRow = currentRowCache.get(sourceRow);
    }

    // we haven't seen this row before, so look it up
    if (currentRow == null) {
      currentRow = getCurrentRow(sourceRow);
      if (matchesBatch) {
        // stick it back in the cache
        this.currentRowCache.put(sourceRow, currentRow);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating index for row: " + Bytes.toString(sourceRow));
    }
    return currentRow;
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

}