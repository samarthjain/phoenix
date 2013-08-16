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

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.salesforce.hbase.index.builder.BaseIndexBuilder;
import com.salesforce.hbase.index.builder.covered.CoveredColumnIndexCodec;
import com.salesforce.hbase.index.builder.covered.CoveredColumnIndexer;
import com.salesforce.hbase.index.builder.covered.IndexCodec;
import com.salesforce.hbase.index.builder.covered.TableState;

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
public class PhoenixIndexBuilder extends BaseIndexBuilder {

  private static final Log LOG = LogFactory.getLog(PhoenixIndexBuilder.class);
  private static final String CODEC_INSTANCE_KEY = "com.salesforce.hbase.index.codec.class";

  private IndexCodec codec;
  private Map<byte[], Result> rowCache = new TreeMap<byte[], Result>(Bytes.BYTES_COMPARATOR);
  private RegionCoprocessorEnvironment env;

  // TODO actually get this from the CP endpoint
  private final BatchCache batchCache = new BatchCache();
  private LocalTable localTable;

  @Override
  public void setup(RegionCoprocessorEnvironment env) throws IOException {
    this.env = env;
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
    
    this.localTable = new LocalTable(env, rowCache, batchCache);
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdate(Put p) throws IOException {
    // build the index updates for each group
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();

    // create a state manager, so we can manage each batch
    LocalTableState state = new LocalTableState(env, localTable, p);

    batchMutationAndAddUpdates(updateMap, state, p);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Found index updates for Put: " + updateMap);
    }

    // we have all the updates for this row, so we just need to update the row cache for this row
    updateRowCache(p, state);
    return updateMap;
  }

  /**
   * Split the mutation into batches based on the timestamps of each keyvalue. We need to check each
   * key-value in the update to see if it matches the others. Generally, this will be the case, but
   * you can add kvs to a mutation that don't all have the timestamp, so we need to manage
   * everything in batches based on timestamp.
   * <p>
   * Adds all the updates in the {@link Mutation} to the state, as a side-effect.
   * @param updateMap index updates into which to add new updates. Modified as a side-effect.
   * @param state current state of the row for the mutation.
   * @param m mutation to batch
   */
  private void batchMutationAndAddUpdates(List<Pair<Mutation, String>> updateMap,
      LocalTableState state, Mutation m) {
    // split the mutation into timestamp-based batches
    TreeMultimap<Long, KeyValue> batches = createTimestampBatchesFromFamilyMap(m);
    // we can need more deletes if a batch is 'back in time' from the current state of the row. The
    // deletes would then cover the current state with the next state for the row
    boolean needMoreDeletes = false;
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
      needMoreDeletes = addMutationsForBatch(updateMap, batch, state);
    }

    if (needMoreDeletes) {
      // TODO delete management
    }
  }

  /**
   * Batch all the {@link KeyValue}s in a {@link Mutation} by timestamp. Updates any
   * {@link KeyValue} with a timestamp == {@link HConstants#LATEST_TIMESTAMP} to a single value
   * obtained when the method is called.
   * @param m {@link Mutation} from which to extract the {@link KeyValue}s
   * @return map of timestamp to all the keyvalues with the same timestamp. the implict tree sorting
   *         in the returned ensures that batches (when iterating through the keys) will iterate the
   *         kvs in timestamp order
   */
  protected TreeMultimap<Long, KeyValue> createTimestampBatchesFromFamilyMap(Mutation m) {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    byte[] nowBytes = Bytes.toBytes(now);
    TreeMultimap<Long, KeyValue> batches =
        TreeMultimap.create(Ordering.natural(), KeyValue.COMPARATOR);
    for (List<KeyValue> kvs : m.getFamilyMap().values()) {
      for (KeyValue kv : kvs) {
        long ts = kv.getTimestamp();
        // override the timestamp to the current time, so the index and primary tables match
        // all the keys with LATEST_TIMESTAMP will then be put into the same batch
        if (ts == HConstants.LATEST_TIMESTAMP) {
          kv.updateLatestStamp(nowBytes);
        }
        batches.put(kv.getTimestamp(), kv);
      }
    }
    return batches;
  }

  /**
   * For a single batch, get all the index updates and add them to the updateMap
   * <p>
   * Adds all the updates in the {@link Mutation} to the state, as a side-effect.
   * @param updateMap map to update with new index elements
   * @param batch timestamp-based batch of edits
   * @param state local state to update and pass to the codec
   */
  private boolean addMutationsForBatch(Collection<Pair<Mutation, String>> updateMap,
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
    long maxTs = 0;
    if (upserts != null) {
      for (Pair<Put, byte[]> p : upserts) {
        // TODO replace this as just storing a byte[], to avoid all the String <-> byte[] swapping
        // HBase does
        String table = Bytes.toString(p.getSecond());
        Put put = p.getFirst();

        // find the latest timestamp in this put
        for (List<KeyValue> kvs : put.getFamilyMap().values()) {
          for (KeyValue kv : kvs) {
            maxTs = maxTs < kv.getTimestamp() ? kv.getTimestamp() : maxTs;
          }
        }
        updateMap.add(new Pair<Mutation, String>(p.getFirst(), table));
        
        // TODO - fix this bit. See CoveredColumnIndexer#getIndexRow, #getNextEntries
        // if the next newest timestamp is newer than our timestamp there are entries in the table
        // for the index rows that affect this index entry, so we need to delete the Put, but at the
        // newer timestamp
        Delete d = null;
        if (indexRow.nextNewestTs > timestamp
            && indexRow.nextNewestTs != CoveredColumnIndexCodec.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP) {
          d = new Delete(rowKey);
          d.setTimestamp(indexRow.nextNewestTs);
        }
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
    // stores all the return values
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>();

    // We have to figure out which kind of delete it is, since we need to do different things if its
    // a general (row) delete, versus a delete of just a single column or family
    Map<byte[], List<KeyValue>> families = d.getFamilyMap();
    LocalTableState state = new LocalTableState(env, localTable, d);

    /*
     * Option 1: its a row delete marker, so we just need to delete the most recent state for each
     * group, as of the specified timestamp in the delete. This can happen if we have a single row
     * update and it is part of a batch mutation (prepare doesn't happen until later... maybe a
     * bug?). In a single delete, this delete gets all the column families appended, so the family
     * map won't be empty by the time it gets here.
     */
    if (families.size() == 0) {
      // get a consistent view of name
      long now = d.getTimeStamp();
      if (now == HConstants.LATEST_TIMESTAMP) {
        now = EnvironmentEdgeManager.currentTimeMillis();
        // update the delete's idea of 'now' to be consistent with the index
        d.setTimestamp(now);
      }
      // get deletes from the codec
      // we only need to get deletes and not add puts because this delete covers all columns
      addDeleteUpdatesToMap(updateMap, state, now);

      /*
       * Update the current state for all the kvs in the delete. Generally, we would just iterate
       * the family map, but since we go here, the family map is empty! Therefore, we need to fake a
       * bunch of family deletes (just like hos HRegion#prepareDelete works). This is just needed
       * for current version of HBase that has an issue where the batch update doesn't update the
       * deletes before calling the hook.
       */
      byte[] deleteRow = d.getRow();
      for (byte[] family : this.env.getRegion().getTableDesc().getFamiliesKeys()) {
        state.addUpdate(new KeyValue(deleteRow, family, null, now, KeyValue.Type.DeleteFamily));
      }
    } else {
    // Option 2: Its actually a bunch single updates, which can have different timestamps.
    // Therefore, we need to do something similar to the put case and batch by timestamp
    batchMutationAndAddUpdates(updateMap, state, d);
    }

    // we have all the updates for this row, so we just need to update the row cache for this row
    updateRowCache(d, state);

    return updateMap;
  }

  private void updateRowCache(Mutation m, LocalTableState state) {
    Result r = state.getCurrentRowState();
    this.rowCache.put(m.getRow(), r);
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
      this.rowCache.remove(m.getRow());
    }
  }
}