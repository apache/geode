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
package org.apache.geode.cache.partition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.FixedPartitionAttributesImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegion.RecoveryLock;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.control.RebalanceResultsImpl;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOp;
import org.apache.geode.internal.cache.partitioned.rebalance.ExplicitMoveDirector;
import org.apache.geode.internal.cache.partitioned.rebalance.PercentageMoveDirector;

/**
 * Utility methods for handling partitioned Regions, for example during execution of {@link Function
 * Functions} on a Partitioned Region.
 * <p>
 * Example of a Function using utility methods:
 *
 * <pre>
 *  public Serializable execute(FunctionContext context) {
 *     if (context instanceof RegionFunctionContext) {
 *         RegionFunctionContext rc = (RegionFunctionContext) context;
 *         if (PartitionRegionHelper.isPartitionedRegion(rc.getDataSet())) {
 *             Region efficientReader =
 *              PartitionRegionHelper.getLocalDataForContext(rc);
 *             efficientReader.get("someKey");
 *             // ...
 *         }
 *      }
 *  // ...
 * </pre>
 *
 * @since GemFire 6.0
 * @see FunctionService#onRegion(Region)
 */
public final class PartitionRegionHelper {

  private PartitionRegionHelper() {
    // do nothing
  }

  /**
   * Given a partitioned Region, return a map of
   * {@linkplain PartitionAttributesFactory#setColocatedWith(String) colocated Regions}. Given a
   * local data reference to a partitioned region, return a map of local
   * {@linkplain PartitionAttributesFactory#setColocatedWith(String) colocated Regions}. If there
   * are no colocated regions, return an empty map.
   *
   * @param r a partitioned Region
   * @throws IllegalStateException if the Region is not a {@linkplain DataPolicy#PARTITION
   *         partitioned Region}
   * @return an unmodifiable map of {@linkplain Region#getFullPath() region name} to {@link Region}
   * @since GemFire 6.0
   */
  public static Map<String, Region<?, ?>> getColocatedRegions(final Region<?, ?> r) {
    Map ret;
    if (isPartitionedRegion(r)) {
      final PartitionedRegion pr = (PartitionedRegion) r;
      ret = ColocationHelper.getAllColocationRegions(pr);
      if (ret.isEmpty()) {
        ret = Collections.emptyMap();
      }
    } else if (r instanceof LocalDataSet) {
      LocalDataSet lds = (LocalDataSet) r;
      InternalRegionFunctionContext fc = lds.getFunctionContext();
      if (fc != null) {
        ret = ColocationHelper.getAllColocatedLocalDataSets(lds.getProxy(), fc);
        if (ret.isEmpty()) {
          ret = Collections.emptyMap();
        }
      } else {
        ret = ColocationHelper.getColocatedLocalDataSetsForBuckets(lds.getProxy(),
            lds.getBucketSet());
      }
    } else {
      throw new IllegalArgumentException(
          String.format("Region %s is not a Partitioned Region",
              r.getFullPath()));
    }
    return Collections.unmodifiableMap(ret);
  }

  /**
   * Test a Region to see if it is a partitioned Region
   *
   * @return true if it is a partitioned Region
   * @since GemFire 6.0
   */
  public static boolean isPartitionedRegion(final Region<?, ?> r) {
    if (r == null) {
      throw new IllegalArgumentException(
          "Argument 'Region' is null".toString());
    }
    return r instanceof PartitionedRegion ? true : false;
  }

  /**
   * Test a Region to see if it is a partitioned Region
   *
   * @return PartitionedRegion if it is a partitioned Region
   * @since GemFire 6.0
   */
  private static PartitionedRegion isPartitionedCheck(final Region<?, ?> r) {
    if (!isPartitionedRegion(r)) {
      throw new IllegalArgumentException(
          String.format("Region %s is not a Partitioned Region",
              r.getFullPath()));
    }
    return (PartitionedRegion) r;
  }

  /**
   * Gathers a set of details about all partitioned regions in the local Cache. If there are no
   * partitioned regions then an empty set will be returned.
   *
   * @param cache the cache which has the regions
   * @return set of details about all locally defined partitioned regions
   * @since GemFire 6.0
   */
  public static Set<PartitionRegionInfo> getPartitionRegionInfo(final Cache cache) {
    Set<PartitionRegionInfo> prDetailsSet = new TreeSet<>();
    fillInPartitionedRegionInfo((InternalCache) cache, prDetailsSet, false);
    return prDetailsSet;
  }

  /**
   * Gathers details about the specified partitioned region. Returns null if the partitioned region
   * is not locally defined.
   *
   * @param region the region to get info about
   * @return details about the specified partitioned region
   * @since GemFire 6.0
   */
  public static PartitionRegionInfo getPartitionRegionInfo(final Region<?, ?> region) {
    try {
      PartitionedRegion partitionedRegion = isPartitionedCheck(region);
      InternalCache cache = (InternalCache) region.getCache();
      return partitionedRegion.getRedundancyProvider().buildPartitionedRegionInfo(false,
          cache.getInternalResourceManager().getLoadProbe());
    } catch (ClassCastException ignore) {
      // not a PR so return null
    }
    return null;
  }

  private static void fillInPartitionedRegionInfo(final InternalCache cache, final Set prDetailsSet,
      final boolean internal) {
    // TODO: optimize by fetching all PR details from each member at once
    Set<PartitionedRegion> partitionedRegions = cache.getPartitionedRegions();
    if (partitionedRegions.isEmpty()) {
      return;
    }
    for (PartitionedRegion partitionedRegion : partitionedRegions) {
      PartitionRegionInfo prDetails = partitionedRegion.getRedundancyProvider()
          .buildPartitionedRegionInfo(internal, cache.getInternalResourceManager().getLoadProbe());
      if (prDetails != null) {
        prDetailsSet.add(prDetails);
      }
    }
  }

  /**
   * Decide which partitions will host which buckets. Gemfire normally assigns buckets to partitions
   * as needed when data is added to a partitioned region. This method provides way to assign all of
   * the buckets without putting any data in partition region. This method should not be called
   * until all of the partitions are running because it will divide the buckets between the running
   * partitions. If the buckets are already assigned this method will have no effect.
   *
   * This method will block until all buckets are assigned.
   *
   * @param region The region which should have it's buckets assigned.
   * @throws IllegalStateException if the provided region is something other than a
   *         {@linkplain DataPolicy#PARTITION partitioned Region}
   * @since GemFire 6.0
   */
  public static void assignBucketsToPartitions(Region<?, ?> region) {
    PartitionedRegion pr = isPartitionedCheck(region);
    RecoveryLock lock = null;
    try {
      lock = pr.getRecoveryLock();
      lock.lock();
      for (int i = 0; i < getNumberOfBuckets(pr); i++) {
        // This method will return quickly if the bucket already exists
        pr.createBucket(i, 0, null);
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  private static int getNumberOfBuckets(PartitionedRegion pr) {
    if (pr.isFixedPartitionedRegion()) {
      int numBuckets = 0;
      Set<FixedPartitionAttributesImpl> fpaSet = new HashSet<FixedPartitionAttributesImpl>(
          pr.getRegionAdvisor().adviseAllFixedPartitionAttributes());
      if (pr.getFixedPartitionAttributesImpl() != null) {
        fpaSet.addAll(pr.getFixedPartitionAttributesImpl());
      }
      for (FixedPartitionAttributesImpl fpa : fpaSet) {
        numBuckets = numBuckets + fpa.getNumBuckets();
      }
      return numBuckets;
    }
    return pr.getTotalNumberOfBuckets();
  }

  /**
   * Get the current primary owner for a key. Upon return there is no guarantee that primary owner
   * remains the primary owner, or that the member is still alive.
   * <p>
   * This method is not a substitute for {@link Region#containsKey(Object)}.
   * </p>
   *
   * @param r a PartitionedRegion
   * @param key the key to evaluate
   * @throws IllegalStateException if the provided region is something other than a
   *         {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return the primary member for the key, possibly null if a primary is not yet determined
   * @since GemFire 6.0
   */
  public static <K, V> DistributedMember getPrimaryMemberForKey(final Region<K, V> r, final K key) {
    PartitionedRegion pr = isPartitionedCheck(r);
    int bucketId = PartitionedRegionHelper.getHashKey(pr, null, key, null, null);
    return pr.getBucketPrimary(bucketId);
  }

  /**
   * Get all potential redundant owners for a key. If the key exists in the Region, upon return
   * there is no guarantee that key has not been moved or that the members are still alive.
   *
   * <p>
   * This method is not a substitute for {@link Region#containsKey(Object)}.
   * </p>
   * <p>
   * This method is equivalent to: <code>
   *  DistributedMember primary = getPrimaryMemberForKey(r, key);
   *  Set<? extends DistributedMember> allMembers = getAllMembersForKey(r, key);
   *  allMembers.remove(primary);
   * </code>
   * </p>
   *
   * @param r a PartitionedRegion
   * @param key the key to evaluate
   * @throws IllegalStateException if the provided region is something other than a
   *         {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return an unmodifiable set of members minus the primary
   * @since GemFire 6.0
   */
  public static <K, V> Set<DistributedMember> getRedundantMembersForKey(final Region<K, V> r,
      final K key) {
    DistributedMember primary = getPrimaryMemberForKey(r, key);
    Set<? extends DistributedMember> owners = getAllForKey(r, key);
    if (primary != null) {
      owners.remove(primary);
    }
    return Collections.unmodifiableSet(owners);
  }

  /**
   * Get all potential owners for a key. If the key exists in the Region, upon return there is no
   * guarantee that it has not moved nor does it guarantee all members are still alive.
   * <p>
   * This method is not a substitute for {@link Region#containsKey(Object)}.
   *
   * @param r PartitionedRegion
   * @param key the key to evaluate
   * @throws IllegalStateException if the provided region is something other than a
   *         {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return an unmodifiable set of all members
   * @since GemFire 6.0
   */
  public static <K, V> Set<DistributedMember> getAllMembersForKey(final Region<K, V> r,
      final K key) {
    return Collections.unmodifiableSet(getAllForKey(r, key));
  }

  private static <K, V> Set<? extends DistributedMember> getAllForKey(final Region<K, V> r,
      final K key) {
    PartitionedRegion pr = isPartitionedCheck(r);
    int bucketId = PartitionedRegionHelper.getHashKey(pr, null, key, null, null);
    return pr.getRegionAdvisor().getBucketOwners(bucketId);
  }

  /**
   * Given a RegionFunctionContext {@linkplain RegionFunctionContext#getDataSet() for a partitioned
   * Region}, return a map of {@linkplain PartitionAttributesFactory#setColocatedWith(String)
   * colocated Regions} with read access limited to the context of the function.
   * <p>
   * Writes using these Region have no constraints and behave the same as a partitioned Region.
   * <p>
   * If there are no colocated regions, return an empty map.
   *
   * @param c the region function context
   * @throws IllegalStateException if the Region is not a {@linkplain DataPolicy#PARTITION
   *         partitioned Region}
   * @return an unmodifiable map of {@linkplain Region#getFullPath() region name} to {@link Region}
   * @since GemFire 6.0
   */
  public static Map<String, Region<?, ?>> getLocalColocatedRegions(final RegionFunctionContext c) {
    final Region r = c.getDataSet();
    isPartitionedCheck(r);
    final InternalRegionFunctionContext rfci = (InternalRegionFunctionContext) c;
    Map ret = rfci.getColocatedLocalDataSets();
    return ret;
  }

  /**
   * Given a RegionFunctionContext {@linkplain RegionFunctionContext#getDataSet() for a partitioned
   * Region}, return a Region providing read access limited to the function context.<br>
   * Returned Region provides only one copy of the data although
   * {@link PartitionAttributes#getRedundantCopies() redundantCopies} configured is more than 0. If
   * the invoking Function is configured to have {@link Function#optimizeForWrite()
   * optimizeForWrite} as true,the returned Region will only contain primary copy of the data.
   * <p>
   * Writes using this Region have no constraints and behave the same as a partitioned Region.
   *
   * @param c a functions context
   * @throws IllegalStateException if {@link RegionFunctionContext#getDataSet()} returns something
   *         other than a {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return a Region for efficient reads
   * @since GemFire 6.0
   */
  public static <K, V> Region<K, V> getLocalDataForContext(final RegionFunctionContext c) {
    final Region r = c.getDataSet();
    isPartitionedCheck(r);
    InternalRegionFunctionContext rfci = (InternalRegionFunctionContext) c;
    return rfci.getLocalDataSet(r);
  }

  /**
   * Given a partitioned Region return a Region providing read access limited to the local heap,
   * writes using this Region have no constraints and behave the same as a partitioned Region.<br>
   *
   * @param r a partitioned region
   * @throws IllegalStateException if the provided region is something other than a
   *         {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return a Region for efficient reads
   * @since GemFire 6.0
   */
  public static <K, V> Region<K, V> getLocalData(final Region<K, V> r) {
    if (isPartitionedRegion(r)) {
      PartitionedRegion pr = (PartitionedRegion) r;
      final Set<Integer> buckets;
      if (pr.getDataStore() != null) {
        buckets = pr.getDataStore().getAllLocalBucketIds();
      } else {
        buckets = Collections.emptySet();
      }
      return new LocalDataSet(pr, buckets);
    } else if (r instanceof LocalDataSet) {
      return r;
    } else {
      throw new IllegalArgumentException(
          String.format("Region %s is not a Partitioned Region",
              r.getFullPath()));
    }
  }

  /**
   * Given a partitioned Region return a Region providing read access to primary copy of the data
   * which is limited to the local heap, writes using this Region have no constraints and behave the
   * same as a partitioned Region.<br>
   *
   * @param r a partitioned region
   * @throws IllegalStateException if the provided region is something other than a
   *         {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return a Region for efficient reads
   * @since GemFire 6.5
   */
  public static <K, V> Region<K, V> getLocalPrimaryData(final Region<K, V> r) {
    if (isPartitionedRegion(r)) {
      PartitionedRegion pr = (PartitionedRegion) r;
      final Set<Integer> buckets;
      if (pr.getDataStore() != null) {
        buckets = pr.getDataStore().getAllLocalPrimaryBucketIds();
      } else {
        buckets = Collections.emptySet();
      }
      return new LocalDataSet(pr, buckets);
    } else if (r instanceof LocalDataSet) {
      return r;
    } else {
      throw new IllegalArgumentException(
          String.format("Region %s is not a Partitioned Region",
              r.getFullPath()));
    }
  }

  /**
   * Moves the bucket which contains the given key from the source member to the destination member.
   * The bucket will be fully transferred once this method is complete, if the method does not throw
   * an exception.
   * <p>
   * All keys which exist in the same bucket will also be moved to the new node.
   * <p>
   * Any data in colocated regions that are colocated with this key will also be moved.
   * <p>
   * This method allows direct control of what data to move. To automatically balance buckets, see
   * {@link ResourceManager#createRebalanceFactory()}
   *
   * @param region The region in which to move the bucket. Data in regions colocated with this
   *        region will also be moved.
   * @param source A member that is currently hosting this bucket. The bucket is moved off of this
   *        member.
   * @param destination A member that is not currently hosting this bucket, but has the partitioned
   *        region defined. The bucket is moved to this member.
   * @param key A key which maps to the bucket to move. This key does not actually need to exist in
   *        the region, but if using a {@link PartitionResolver} the resolver should be able to get
   *        the routing object from this key to determine the bucket to move.
   *
   * @throws IllegalStateException if the bucket is not present on the source, if the source or
   *         destination are not valid members of the system, if the destination already hosts a
   *         copy of the bucket, or if the bucket does not exist.
   *
   * @since GemFire 7.1
   */
  public static <K> void moveBucketByKey(Region<K, ?> region, DistributedMember source,
      DistributedMember destination, K key) {
    PartitionedRegion pr = isPartitionedCheck(region);
    if (pr.isFixedPartitionedRegion()) {
      throw new IllegalStateException("Cannot move data in a fixed partitioned region");
    }
    int bucketId = pr.getKeyInfo(key).getBucketId();
    ExplicitMoveDirector director = new ExplicitMoveDirector(key, bucketId, source, destination,
        region.getCache().getDistributedSystem());
    PartitionedRegionRebalanceOp rebalance =
        new PartitionedRegionRebalanceOp(pr, false, director, true, true);
    rebalance.execute();
  }

  /**
   * Moves data from the source member to the destination member, up to the given percentage of data
   * (measured in bytes). The data will be fully transferred once this method is complete, if the
   * method does not throw an exception. The percentage is a percentage of the amount of data in
   * bytes on the source member for this region.
   * <p>
   *
   * If this region has colocated regions, the colocated data will also be moved. The total amount
   * of data in all colocated regions will be taken into consideration when determining what
   * percentage of data will be moved.
   * <p>
   * It may not be possible to move data to the destination member, if the destination member has no
   * available space, no bucket smaller than the given percentage exists, or if moving data would
   * violate redundancy constraints. If data cannot be moved, this method will return a
   * RebalanceResult object with 0 total bucket transfers.
   * <p>
   * This method allows direct control of what data to move. To automatically balance buckets, see
   * {@link ResourceManager#createRebalanceFactory()}
   *
   * @param region The region in which to move data. Data in regions colocated with this region will
   *        also be moved.
   * @param source A member that is currently hosting data. The bucket is moved off of this member.
   * @param destination A member that that has the partitioned region defined. Data is moved to this
   *        member.
   * @param percentage the maximum amount of data to move, as a percentage from 0 to 100.
   *
   * @throws IllegalStateException if the source or destination are not valid members of the system.
   * @throws IllegalArgumentException if the percentage is not between 0 to 100.
   *
   * @return A RebalanceResult object that contains information about what what data was actually
   *         moved.
   *
   * @since GemFire 7.1
   */
  public static RebalanceResults moveData(Region<?, ?> region, DistributedMember source,
      DistributedMember destination, float percentage) {
    PartitionedRegion pr = isPartitionedCheck(region);
    if (pr.isFixedPartitionedRegion()) {
      throw new IllegalStateException("Cannot move data in a fixed partitioned region");
    }
    if (percentage <= 0 || percentage > 100.0) {
      throw new IllegalArgumentException("Percentage must be between 0 and 100");
    }

    PercentageMoveDirector director = new PercentageMoveDirector(source, destination, percentage);
    PartitionedRegionRebalanceOp rebalance =
        new PartitionedRegionRebalanceOp(pr, false, director, true, true);
    Set<PartitionRebalanceInfo> results = rebalance.execute();

    RebalanceResultsImpl rebalanceResults = new RebalanceResultsImpl();
    for (PartitionRebalanceInfo details : results) {
      rebalanceResults.addDetails(details);
    }

    return rebalanceResults;
  }

}
