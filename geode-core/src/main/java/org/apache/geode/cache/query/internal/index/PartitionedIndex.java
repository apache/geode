/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query.internal.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.execute.BucketMovedException;

/**
 * This class implements a Partitioned index over a group of partitioned region buckets.
 *
 * @since GemFire 5.1
 */

public class PartitionedIndex extends AbstractIndex {

  /**
   * Contains the reference for all the local indexed buckets.
   */
  private final Map<Region, List<Index>> bucketIndexes =
      Collections.synchronizedMap(new HashMap<>());

  // An arbitrary bucket index from this PartiionedIndex that is used as a representative
  // index for the entire PartitionIndex. Usually used for scoring/sizing of an index when
  // selecting which index to use
  private volatile Index arbitraryBucketIndex;
  /**
   * Type on index represented by this partitioned index.
   *
   * @see IndexType#FUNCTIONAL
   * @see IndexType#PRIMARY_KEY
   * @see IndexType#HASH
   */
  private final IndexType type;

  /**
   * Number of remote buckets indexed when creating an index on the partitioned region instance.
   */
  private int numRemoteBucektsIndexed;

  /**
   * String for imports if needed for index creations
   */
  private final String imports;

  protected Set mapIndexKeys = Collections.newSetFromMap(new ConcurrentHashMap());

  // Flag indicating that the populationg of this index is in progress
  private volatile boolean populateInProgress;

  /**
   * Constructor for partitioned indexed. Creates the partitioned index on given a partitioned
   * region. An index can be created programmatically or through cache.xml during initialization.
   */
  public PartitionedIndex(InternalCache cache, IndexType iType, String indexName, Region r,
      String indexedExpression, String fromClause, String imports) {
    super(cache, indexName, r, fromClause, indexedExpression, null, fromClause, indexedExpression,
        null, null);
    type = iType;
    this.imports = imports;

    if (iType == IndexType.HASH) {
      if (!getRegion().getAttributes().getIndexMaintenanceSynchronous()) {
        throw new UnsupportedOperationException(
            "Hash index is currently not supported for regions with Asynchronous index maintenance.");
      }
    }
  }

  /**
   * Adds an index on a bucket to the list of already indexed buckets in the partitioned region.
   *
   * @param index bucket index to be added to the list.
   */
  public void addToBucketIndexes(Region r, Index index) {
    synchronized (bucketIndexes) {
      setArbitraryBucketIndex(index);
      List<Index> indexes = bucketIndexes.get(r);
      if (indexes == null) {
        indexes = new ArrayList<>();
      }
      indexes.add(index);
      bucketIndexes.put(r, indexes);
    }
  }

  public void removeFromBucketIndexes(Region r, Index index) {
    synchronized (bucketIndexes) {
      List<Index> indexes = bucketIndexes.get(r);
      if (indexes != null) {
        indexes.remove(index);
        if (indexes.isEmpty()) {
          bucketIndexes.remove(r);
        }
      }
      if (index == arbitraryBucketIndex) {
        resetArbitraryBucketIndex(retrieveArbitraryBucketIndex());
      }
    }
  }

  /**
   * Returns the number of locally indexed buckets.
   *
   * @return int number of buckets.
   */
  public int getNumberOfIndexedBuckets() {
    synchronized (bucketIndexes) {
      int size = 0;
      for (List<Index> indexList : bucketIndexes.values()) {
        size += indexList.size();
      }
      return size;
    }
  }

  /**
   * Gets a collection of all the bucket indexes created so far.
   *
   * @return bucketIndexes collection of all the bucket indexes.
   */
  public List getBucketIndexes() {
    synchronized (bucketIndexes) {
      List<Index> indexes = new ArrayList<>();
      for (List<Index> indexList : bucketIndexes.values()) {
        indexes.addAll(indexList);
      }
      return indexes;
    }
  }

  public List<Index> getBucketIndexes(Region r) {
    synchronized (bucketIndexes) {
      List<Index> indexes = new ArrayList<>();
      List<Index> indexList = bucketIndexes.get(r);
      if (indexList != null) {
        indexes.addAll(indexList);
      }
      return indexes;
    }
  }

  public void setArbitraryBucketIndex(Index index) {
    if (arbitraryBucketIndex == null) {
      arbitraryBucketIndex = index;
    }
  }

  private void resetArbitraryBucketIndex(Index index) {
    arbitraryBucketIndex = index;
  }

  public Index retrieveArbitraryBucketIndex() {
    Index index = null;
    synchronized (bucketIndexes) {
      if (bucketIndexes.size() > 0) {
        List<Index> indexList = bucketIndexes.values().iterator().next();
        if (indexList != null && indexList.size() > 0) {
          index = indexList.get(0);
        }
      }
    }
    return index;
  }

  public Index getBucketIndex() {
    return arbitraryBucketIndex;
  }

  protected Map.Entry<Region, List<Index>> getFirstBucketIndex() {
    Map.Entry<Region, List<Index>> firstIndexEntry = null;
    synchronized (bucketIndexes) {
      if (bucketIndexes.size() > 0) {
        firstIndexEntry = bucketIndexes.entrySet().iterator().next();
      }
    }
    return firstIndexEntry;
  }

  /**
   * Returns the type of index this partitioned index represents.
   *
   * @return indexType type of partitioned index.
   */
  @Override
  public IndexType getType() {
    return type;
  }

  /**
   * Returns the index for the bucket.
   */
  public static AbstractIndex getBucketIndex(PartitionedRegion pr, String indexName, Integer bId)
      throws QueryInvocationTargetException {
    try {
      pr.checkReadiness();
    } catch (Exception ex) {
      throw new QueryInvocationTargetException(ex.getMessage());
    }
    PartitionedRegionDataStore prds = pr.getDataStore();
    BucketRegion bukRegion;
    bukRegion = prds.getLocalBucketById(bId);
    if (bukRegion == null) {
      throw new BucketMovedException("Bucket not found for the id :" + bId);
    }
    AbstractIndex index = null;
    if (bukRegion.getIndexManager() != null) {
      index = (AbstractIndex) (bukRegion.getIndexManager().getIndex(indexName));
    } else {
      if (pr.getCache().getLogger().fineEnabled()) {
        pr.getCache().getLogger().fine("Index Manager not found for the bucket region "
            + bukRegion.getFullPath() + " unable to fetch the index " + indexName);
      }
      throw new QueryInvocationTargetException(
          "Index Manager not found, " + " unable to fetch the index " + indexName);
    }

    return index;
  }

  /**
   * Verify if the index is available of the buckets. If not create index on the bucket.
   */
  public void verifyAndCreateMissingIndex(List buckets) throws QueryInvocationTargetException {
    PartitionedRegion pr = (PartitionedRegion) getRegion();
    PartitionedRegionDataStore prds = pr.getDataStore();

    for (Object bId : buckets) {
      // create index
      BucketRegion bukRegion = prds.getLocalBucketById((Integer) bId);
      if (bukRegion == null) {
        throw new QueryInvocationTargetException("Bucket not found for the id :" + bId);
      }
      IndexManager im = IndexUtils.getIndexManager(cache, bukRegion, true);
      if (im != null && im.getIndex(indexName) == null) {
        try {
          if (pr.getCache().getLogger().fineEnabled()) {
            pr.getCache().getLogger()
                .fine("Verifying index presence on bucket region. " + " Found index "
                    + indexName + " not present on the bucket region "
                    + bukRegion.getFullPath() + ", index will be created on this region.");
          }

          ExecutionContext externalContext = new ExecutionContext(null, bukRegion.getCache());
          externalContext.setBucketRegion(pr, bukRegion);

          im.createIndex(indexName, type, originalIndexedExpression, fromClause,
              imports, externalContext, this, true);
        } catch (IndexExistsException iee) {
          // Index exists.
        } catch (IndexNameConflictException ince) {
          // ignore.
        }
      }
    }
  }


  @Override
  protected boolean isCompactRangeIndex() {
    return false;
  }

  /**
   * Set the number of remotely indexed buckets when this partitioned index was created.
   *
   * @param remoteBucketsIndexed int representing number of remote buckets.
   */
  public void setRemoteBucketesIndexed(int remoteBucketsIndexed) {
    numRemoteBucektsIndexed = remoteBucketsIndexed;
  }

  /**
   * Returns the number of remotely indexed buckets by this partitioned index.
   *
   * @return int number of remote indexed buckets.
   */
  public int getNumRemoteBucketsIndexed() {
    return numRemoteBucektsIndexed;
  }

  /**
   * The Region this index is on.
   *
   * @return the Region for this index
   */
  @Override
  public Region getRegion() {
    return super.getRegion();
  }

  /**
   * Not supported on partitioned index.
   */
  @Override
  void addMapping(RegionEntry entry) throws IMQException {
    throw new RuntimeException(
        "Not supported on partitioned index");
  }

  /**
   * Not supported on partitioned index.
   */

  @Override
  public void initializeIndex(boolean loadEntries) throws IMQException {
    throw new RuntimeException(
        "Not supported on partitioned index");
  }

  /**
   * Not supported on partitioned index.
   */
  @Override
  void lockedQuery(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator indpndntItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection) {
    throw new RuntimeException(
        "Not supported on partitioned index");

  }

  /**
   * Not supported on partitioned index.
   */
  @Override
  void recreateIndexData() throws IMQException {
    throw new RuntimeException(
        "Not supported on partitioned index");

  }

  /**
   * Not supported on partitioned index.
   */
  @Override
  void removeMapping(RegionEntry entry, int opCode) {
    throw new RuntimeException(
        "Not supported on partitioned index");

  }

  /**
   * Returns false, clear is not supported on partitioned index.
   */

  @Override
  public boolean clear() throws QueryException {
    return false;
  }

  /*
   * Not supported on partitioned index.
   */
  /*
   * public void destroy() { throw new
   * RuntimeException("Not supported on partitioned index".
   * toLocalizedString()); }
   */

  /**
   * Not supported on partitioned index.
   */
  @Override
  public IndexStatistics getStatistics() {
    return internalIndexStats;
  }

  /**
   * Returns string representing imports.
   */
  public String getImports() {
    return imports;
  }

  /**
   * String representing the state.
   *
   * @return string representing all the relevant information.
   */
  public String toString() {
    StringBuffer st = new StringBuffer();
    st.append(super.toString()).append("imports : ").append(imports);
    return st.toString();
  }

  @Override
  protected InternalIndexStatistics createStats(String indexName) {
    if (internalIndexStats == null) {
      internalIndexStats = new PartitionedIndexStatistics(this.indexName);
    }
    return internalIndexStats;
  }

  /**
   * This will create extra {@link IndexStatistics} statistics for MapType PartitionedIndex.
   *
   * @return new PartitionedIndexStatistics
   */
  protected InternalIndexStatistics createExplicitStats(String indexName) {
    return new PartitionedIndexStatistics(indexName);
  }

  /**
   * Internal class for partitioned index statistics. Statistics are not supported right now.
   */
  class PartitionedIndexStatistics extends InternalIndexStatistics {
    private final IndexStats vsdStats;

    public PartitionedIndexStatistics(String indexName) {
      vsdStats = new IndexStats(getRegion().getCache().getDistributedSystem(), indexName);
    }

    /**
     * Return the total number of times this index has been updated
     */
    @Override
    public long getNumUpdates() {
      return vsdStats.getNumUpdates();
    }

    @Override
    public void incNumValues(int delta) {
      vsdStats.incNumValues(delta);
    }

    @Override
    public void incNumUpdates() {
      vsdStats.incNumUpdates();
    }

    @Override
    public void incNumUpdates(int delta) {
      vsdStats.incNumUpdates(delta);
    }

    @Override
    public void updateNumKeys(long numKeys) {
      vsdStats.updateNumKeys(numKeys);
    }

    @Override
    public void incNumKeys(long numKeys) {
      vsdStats.incNumKeys(numKeys);
    }

    @Override
    public void incNumMapIndexKeys(long numKeys) {
      vsdStats.incNumMapIndexKeys(numKeys);
    }

    @Override
    public void incUpdateTime(long delta) {
      vsdStats.incUpdateTime(delta);
    }

    @Override
    public void incUpdatesInProgress(int delta) {
      vsdStats.incUpdatesInProgress(delta);
    }

    @Override
    public void incNumUses() {
      vsdStats.incNumUses();
    }

    @Override
    public void incUseTime(long delta) {
      vsdStats.incUseTime(delta);
    }

    @Override
    public void incUsesInProgress(int delta) {
      vsdStats.incUsesInProgress(delta);
    }

    @Override
    public void incReadLockCount(int delta) {
      vsdStats.incReadLockCount(delta);
    }

    @Override
    public void incNumBucketIndexes(int delta) {
      vsdStats.incNumBucketIndexes(delta);
    }

    /**
     * Returns the number of keys in this index at the highest level
     */
    @Override
    public long getNumberOfMapIndexKeys() {
      return vsdStats.getNumberOfMapIndexKeys();
    }

    /**
     * Returns the total amount of time (in nanoseconds) spent updating this index.
     */
    @Override
    public long getTotalUpdateTime() {
      return vsdStats.getTotalUpdateTime();
    }

    /**
     * Returns the total number of times this index has been accessed by a query.
     */
    @Override
    public long getTotalUses() {
      return vsdStats.getTotalUses();
    }

    /**
     * Returns the number of keys in this index.
     */
    @Override
    public long getNumberOfKeys() {
      return vsdStats.getNumberOfKeys();
    }

    /**
     * Returns the number of values in this index.
     */
    @Override
    public long getNumberOfValues() {
      return vsdStats.getNumberOfValues();
    }

    /**
     * Return the number of read locks taken on this index
     */
    @Override
    public int getReadLockCount() {
      return vsdStats.getReadLockCount();
    }

    @Override
    public int getNumberOfBucketIndexes() {
      return vsdStats.getNumberOfBucketIndexes();
    }

    @Override
    public void close() {
      vsdStats.close();
    }

    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("No Keys = ").append(getNumberOfKeys()).append("\n");
      sb.append("No Map Index Keys = ").append(getNumberOfMapIndexKeys()).append("\n");
      sb.append("No Values = ").append(getNumberOfValues()).append("\n");
      sb.append("No Uses = ").append(getTotalUses()).append("\n");
      sb.append("No Updates = ").append(getNumUpdates()).append("\n");
      sb.append("Total Update time = ").append(getTotalUpdateTime()).append("\n");
      return sb.toString();
    }
  }

  @Override
  void instantiateEvaluator(IndexCreationHelper indexCreationHelper) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ObjectType getResultSetType() {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported on partitioned index.
   */
  @Override
  void lockedQuery(Object lowerBoundKey, int lowerBoundOperator, Object upperBoundKey,
      int upperBoundOperator, Collection results, Set keysToRemove,
      ExecutionContext context)
      throws TypeMismatchException {
    throw new RuntimeException(
        "Not supported on partitioned index");

  }

  @Override
  public int getSizeEstimate(Object key, int op, int matchLevel) {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }


  @Override
  void lockedQuery(Object key, int operator, Collection results, Set keysToRemove,
      ExecutionContext context) throws TypeMismatchException {
    throw new RuntimeException("Not supported on partitioned index");

  }

  @Override
  void addMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    throw new RuntimeException(
        "Not supported on partitioned index");

  }

  @Override
  void saveMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    throw new RuntimeException(
        "Not supported on partitioned index");

  }

  public void incNumMapKeysStats(Object mapKey) {
    if (internalIndexStats != null) {
      if (!mapIndexKeys.contains(mapKey)) {
        mapIndexKeys.add(mapKey);
        internalIndexStats.incNumMapIndexKeys(1);
      }
    }
  }

  public void incNumBucketIndexes() {
    if (internalIndexStats != null) {
      internalIndexStats.incNumBucketIndexes(1);
    }
  }

  @Override
  public boolean isEmpty() {
    boolean empty = true;
    for (Object index : getBucketIndexes()) {
      empty = ((AbstractIndex) index).isEmpty();
      if (!empty) {
        return false;
      }
    }
    return empty;
  }

  public boolean isPopulateInProgress() {
    return populateInProgress;
  }

  public void setPopulateInProgress(boolean populateInProgress) {
    this.populateInProgress = populateInProgress;
  }

}
