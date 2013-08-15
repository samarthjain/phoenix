package com.salesforce.phoenix.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.salesforce.hbase.index.builder.BaseIndexBuilder;

public abstract class BaseCoveredColumnIndexer extends BaseIndexBuilder {

  private static final Log LOG = LogFactory.getLog(BaseCoveredColumnIndexer.class);


  private volatile HTableInterface localTable;
  protected RegionCoprocessorEnvironment env;

  @Override
  public void setup(RegionCoprocessorEnvironment env) throws IOException {
    this.env = env;
  }

  // TODO we loop through all the keyvalues for the row a few times - we should be able to do better

  /**
   * Ensure we have a connection to the local table. We need to do this after
   * {@link #setup(RegionCoprocessorEnvironment)} because we are created on region startup and the
   * table isn't actually accessible until later.
   * @throws IOException if we can't reach the table
   */
  protected HTableInterface getLocalTable() throws IOException {
    if (this.localTable == null) {
      synchronized (this) {
        if (this.localTable == null) {
          localTable = env.getTable(env.getRegion().getTableDesc().getName());
        }
      }
    }
    return this.localTable;
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
   * Make a delete for the state of the current row, at the given timestamp.
   * @param codec to form the row key
   * @param timestamp
   * @return the delete to apply or <tt>null</tt>, if no {@link Delete} is necessary
   */
  private Delete getIndexCleanupForCurrentRow(CoveredColumnIndexCodec codec, long timestamp) {
    byte[] currentRowkey = codec.getIndexRowKey(timestamp).rowKey;
    // no previous state for the current group, so don't create a delete
    if (CoveredColumnIndexCodec.checkRowKeyForAllNulls(currentRowkey)) {
      return null;
    }

    Delete cleanup = new Delete(currentRowkey);
    cleanup.setTimestamp(timestamp);

    return cleanup;
  }

  /**
   * Exposed for testing! Set the local table that should be used to lookup the state of the current
   * row.
   * @param table
   */
  public void setTableForTesting(HTableInterface table) {
    this.localTable = table;
  }

  @Override
  public Collection<Pair<Mutation, String>> getIndexUpdateForFilteredRows(Collection<KeyValue> filtered)
      throws IOException {
    ensureLocalTable();

    // stores all the return values
    List<Pair<Mutation, String>> updateMap = new ArrayList<Pair<Mutation, String>>(filtered.size());
    // batch the updates by row to make life easier and ordered
    TreeMultimap<byte[], KeyValue> batches = batchByRow(filtered);

    for (Entry<byte[], Collection<KeyValue>> batch : batches.asMap().entrySet()) {
      // get the current state of the row in our table
      final byte[] sourceRow = batch.getKey();
      // have to do a raw scan so we see everything, which includes things that haven't been
      // filtered out yet, but aren't generally client visible. We don't need to do this in the
      // other update cases because they are only concerned wi
      Result r = getCurrentRow(sourceRow);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating index for row: " + Bytes.toString(sourceRow));
      }

      // build up the index entries for each kv
      CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r);
      // find all the matching groups that we need to update
      Set<ColumnGroup> matches = new HashSet<ColumnGroup>();
      for (KeyValue kv : batch.getValue()) {
        findMatchingGroups(matches, kv);
        // didn't find a match, so go to the next kv
        if (matches.size() == 0) {
          continue;
        }

        // for each matching group, we need to get the delete that index entry
        for (ColumnGroup group : matches) {
          // the kv here will definitely have a valid timestamp, since it came from the memstore, so
          // we don't need to do any of the timestamp adjustment we do above.
          codec.setGroup(group);
          Delete cleanup = getIndexCleanupForCurrentRow(codec, kv.getTimestamp());
          if (cleanup != null) {
            updateMap.add(new Pair<Mutation, String>(cleanup, group.getTable()));
          }
        }

        matches.clear();
      }
    }
    return updateMap;
  }

  /**
   * @param sourceRow row key to extract
   * @return the full state of the given row. Includes all current versions (even if they are not
   *         usually visible to the client (unless they are also doing a raw scan)).
   */
  protected Result getCurrentRow(byte[] sourceRow) throws IOException {
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

  /**
   * @param filtered
   * @return
   */
  private TreeMultimap<byte[], KeyValue> batchByRow(Collection<KeyValue> filtered) {
    TreeMultimap<byte[], KeyValue> batches =
        TreeMultimap.create(Bytes.BYTES_COMPARATOR, KeyValue.COMPARATOR);

    for (KeyValue kv : filtered) {
      batches.put(kv.getRow(), kv);
    }

    return batches;
  }
}