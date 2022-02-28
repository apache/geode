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

package org.apache.geode.internal.cache;

import static java.lang.String.format;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.internal.cache.partitioned.BucketId;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.persistence.PRPersistentConfig;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * A utility class to retrieve colocated regions in a colocation hierarchy in various scenarios
 *
 * @since GemFire 6.0
 */
public class ColocationHelper {
  private static final Logger logger = LogService.getLogger();

  /**
   * Whether to ignore missing parallel queues on restart if they are not attached to the region.
   * See bug 50120. Mutable for tests.
   */
  @MutableForTesting
  public static boolean IGNORE_UNRECOVERED_QUEUE =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "IGNORE_UNRECOVERED_QUEUE");

  /**
   * An utility method to retrieve colocated region of a given partitioned region
   *
   * @return colocated PartitionedRegion
   * @throws IllegalStateException for missing colocated region
   * @since GemFire 5.8Beta
   */
  public static PartitionedRegion getColocatedRegion(final PartitionedRegion partitionedRegion) {
    Assert.assertTrue(partitionedRegion != null); // precondition1
    String colocatedWith = partitionedRegion.getPartitionAttributes().getColocatedWith();
    if (colocatedWith == null) {
      // the region is not colocated with any region
      return null;
    }
    Region<?, ?> prRoot = PartitionedRegionHelper.getPRRoot(partitionedRegion.getCache());
    PartitionRegionConfig prConf =
        (PartitionRegionConfig) prRoot.get(getRegionIdentifier(colocatedWith));
    if (prConf == null) {
      partitionedRegion.getCache().getCancelCriterion().checkCancelInProgress(null);
      throw new IllegalStateException(
          format(
              "Region specified in 'colocated-with' (%s) for region %s does not exist. It should be created before setting 'colocated-with' attribute for this region.",
              colocatedWith, partitionedRegion.getFullPath()));
    }
    int prID = prConf.getPRId();
    PartitionedRegion colocatedPR = null;
    try {
      colocatedPR = PartitionedRegion.getPRFromId(prID);
      if (colocatedPR != null) {
        colocatedPR.waitOnBucketMetadataInitialization();
      } else {
        partitionedRegion.getCache().getCancelCriterion().checkCancelInProgress(null);
        throw new IllegalStateException(
            format(
                "Region specified in 'colocated-with' (%s) for region %s does not exist. It should be created before setting 'colocated-with' attribute for this region.",
                colocatedWith, partitionedRegion.getFullPath()));
      }
    } catch (PRLocallyDestroyedException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "PRLocallyDestroyedException : Region with prId={} is locally destroyed on this node",
            prID, e);
      }
    }
    return colocatedPR;
  }

  /**
   * An utility to make sure that a member contains all of the partitioned regions that are
   * colocated with a given region on other members. TODO rebalance - this is rather inefficient,
   * and probably all this junk should be in the advisor.
   */
  public static boolean checkMembersColocation(PartitionedRegion partitionedRegion,
      InternalDistributedMember member) {
    List<PartitionRegionConfig> tempcolocatedRegions = new ArrayList<>();
    Region<?, ?> prRoot = PartitionedRegionHelper.getPRRoot(partitionedRegion.getCache());
    PartitionRegionConfig regionConfig =
        (PartitionRegionConfig) prRoot.get(partitionedRegion.getRegionIdentifier());
    // The region was probably concurrently destroyed
    if (regionConfig == null) {
      return false;
    }
    tempcolocatedRegions.add(regionConfig);
    List<PartitionRegionConfig> colocatedRegions = new ArrayList<>(tempcolocatedRegions);
    PartitionRegionConfig prConf;
    do {
      PartitionRegionConfig tempToBeColocatedWith = tempcolocatedRegions.remove(0);
      for (final Object o : prRoot.keySet()) {
        String prName = (String) o;
        try {
          prConf = (PartitionRegionConfig) prRoot.get(prName);
        } catch (EntryDestroyedException ignore) {
          continue;
        }
        if (prConf == null) {
          continue;
        }
        if (prConf.getColocatedWith() != null) {
          if (prConf.getColocatedWith().equals(tempToBeColocatedWith.getFullPath())
              || (SEPARATOR + prConf.getColocatedWith())
                  .equals(tempToBeColocatedWith.getFullPath())) {
            colocatedRegions.add(prConf);
            tempcolocatedRegions.add(prConf);
          }
        }
      }
    } while (!tempcolocatedRegions.isEmpty());

    PartitionRegionConfig tempColocatedWith = regionConfig;
    while (true) {
      String colocatedWithRegionName = tempColocatedWith.getColocatedWith();
      if (colocatedWithRegionName == null) {
        break;
      } else {
        prConf = (PartitionRegionConfig) prRoot.get(getRegionIdentifier(colocatedWithRegionName));
        if (prConf == null) {
          break;
        }
        colocatedRegions.add(tempColocatedWith);
        tempColocatedWith = prConf;
      }
    }

    // Now check to make sure that all the colocated regions
    // Have this member.
    // We don't need a hostname because the equals method doesn't check it.
    for (PartitionRegionConfig config : colocatedRegions) {
      if (config.isColocationComplete() && !config.containsMember(member)) {
        return false;
      }
    }

    // Check to make sure all the persisted regions that are colocated
    // with this region have been created.
    return !hasOfflineColocatedChildRegions(partitionedRegion);
  }

  /**
   * Returns true if there are regions that are persisted on this member and were previously
   * colocated with the given region, but have not yet been created.
   *
   * @param region The parent region
   * @return true if there are any child regions that are persisted on this member, but have not yet
   *         been created.
   */
  private static boolean hasOfflineColocatedChildRegions(PartitionedRegion region) {
    boolean hasOfflineChildren = false;
    final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);
    try {
      InternalCache cache = region.getCache();
      Collection<DiskStore> stores = cache.listDiskStores();
      // Look through all the disk stores for offline colocated child regions
      for (DiskStore diskStore : stores) {
        // Look at all the partitioned regions.

        for (Map.Entry<String, PRPersistentConfig> entry : ((DiskStoreImpl) diskStore).getAllPRs()
            .entrySet()) {

          PRPersistentConfig config = entry.getValue();
          String childName = entry.getKey();

          // Check to see if they're colocated with this region.
          if (region.getFullPath().equals(config.getColocatedWith())) {
            PartitionedRegion childRegion = (PartitionedRegion) cache.getRegion(childName);

            if (childRegion == null) {
              // If the child region is offline, return true
              // unless it is a parallel queue that the user has removed.
              if (!ignoreUnrecoveredQueue(region, childName)) {
                region.addMissingColocatedRegionLogger(childName);
                hasOfflineChildren = true;
              }
            } else {
              // Otherwise, look for offline children of that region.
              if (hasOfflineColocatedChildRegions(childRegion)) {
                hasOfflineChildren = true;
                // Add the offline children of this child to the region's missingChildren list
                region.addMissingColocatedRegionLogger(childRegion);
              }
            }
          }
        }
      }
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
    return hasOfflineChildren;
  }

  private static boolean ignoreUnrecoveredQueue(PartitionedRegion region, String childName) {
    // Hack for #50120 if the childRegion is an async queue, but we
    // no longer define the async queue, ignore it.
    if (!ParallelGatewaySenderQueue.isParallelQueue(childName)) {
      return false;
    }

    String senderId = ParallelGatewaySenderQueue.getSenderId(childName);

    return !region.getAsyncEventQueueIds().contains(senderId)
        && !region.getParallelGatewaySenderIds().contains(senderId) && IGNORE_UNRECOVERED_QUEUE;

    // TODO Auto-generated method stub
  }

  /**
   * A utility to check to see if a region has been created on
   * all the VMs that host the regions
   * this region is colocated with.
   */
  public static boolean isColocationComplete(PartitionedRegion region) {
    Region<?, ?> prRoot = PartitionedRegionHelper.getPRRoot(region.getCache());
    PartitionRegionConfig config = (PartitionRegionConfig) prRoot.get(region.getRegionIdentifier());
    // Fix for bug 40075. There is race between this call and the region being concurrently
    // destroyed.
    if (config == null) {
      Assert.assertTrue(region.isDestroyed() || region.isClosed,
          "Region is not destroyed, but there is no entry in the prRoot for region " + region);
      return false;
    }
    return config.isColocationComplete();
  }

  /**
   * A utility method to retrieve all partitioned regions(excluding self) in a colocation chain<br>
   * <p>
   * For example, shipmentPR is colocated with orderPR and orderPR is colocated with customerPR <br>
   * <br>
   * getAllColocationRegions(customerPR) --> List{orderPR, shipmentPR}<br>
   * getAllColocationRegions(orderPR) --> List{customerPR, shipmentPR}<br>
   * getAllColocationRegions(shipmentPR) --> List{customerPR, orderPR}<br>
   *
   * @return List of all partitioned regions (excluding self) in a colocated chain
   * @since GemFire 5.8Beta
   */
  public static Map<String, PartitionedRegion> getAllColocationRegions(
      PartitionedRegion partitionedRegion) {
    Map<String, PartitionedRegion> colocatedRegions = new HashMap<>();
    List<PartitionedRegion> colocatedByRegion = partitionedRegion.getColocatedByList();
    if (colocatedByRegion.size() != 0) {
      List<PartitionedRegion> tempColocatedRegions = new ArrayList<>(colocatedByRegion);
      do {
        PartitionedRegion pRegion = tempColocatedRegions.remove(0);
        pRegion.waitOnBucketMetadataInitialization();
        colocatedRegions.put(pRegion.getFullPath(), pRegion);
        tempColocatedRegions.addAll(pRegion.getColocatedByList());
      } while (!tempColocatedRegions.isEmpty());
    }
    PartitionedRegion tempColocatedWith = partitionedRegion;
    while (true) {
      PartitionedRegion colocatedWithRegion = tempColocatedWith.getColocatedWithRegion();
      if (colocatedWithRegion == null) {
        break;
      } else {
        colocatedRegions.put(colocatedWithRegion.getFullPath(), colocatedWithRegion);
        tempColocatedWith = colocatedWithRegion;
      }
    }
    return colocatedRegions;
  }

  /**
   * gets local data of colocated regions on a particular data store
   *
   * @return map of region name to local colocated regions
   * @since GemFire 5.8Beta
   */
  public static Map<String, Region<?, ?>> getAllColocatedLocalDataSets(
      PartitionedRegion partitionedRegion, InternalRegionFunctionContext<?> context) {
    Map<String, PartitionedRegion> colocatedRegions = getAllColocationRegions(partitionedRegion);
    Map<String, Region<?, ?>> colocatedLocalRegions = new HashMap<>();
    for (final Entry<String, PartitionedRegion> entry : colocatedRegions.entrySet()) {
      final Region<?, ?> pr = entry.getValue();
      colocatedLocalRegions.put(entry.getKey(), context.getLocalDataSet(pr));
    }
    return colocatedLocalRegions;
  }

  public static Map<String, LocalDataSet<?, ?>> constructAndGetAllColocatedLocalDataSet(
      PartitionedRegion region, final Set<BucketId> buckets) {
    Map<String, LocalDataSet<?, ?>> colocatedLocalDataSets = new HashMap<>();
    if (region.getColocatedWith() == null && (!region.isColocatedBy())) {
      colocatedLocalDataSets.put(region.getFullPath(), new LocalDataSet<>(region, buckets));
      return colocatedLocalDataSets;
    }
    Map<String, PartitionedRegion> colocatedRegions =
        ColocationHelper.getAllColocationRegions(region);
    for (PartitionedRegion colocatedRegion : colocatedRegions.values()) {
      colocatedLocalDataSets.put(colocatedRegion.getFullPath(),
          new LocalDataSet<>(colocatedRegion, buckets));
    }
    colocatedLocalDataSets.put(region.getFullPath(), new LocalDataSet<>(region, buckets));
    return colocatedLocalDataSets;
  }

  public static Map<String, LocalDataSet<?, ?>> getColocatedLocalDataSetsForBuckets(
      PartitionedRegion region, Set<BucketId> bucketSet) {
    if (region.getColocatedWith() == null && (!region.isColocatedBy())) {
      return Collections.emptyMap();
    }
    Map<String, LocalDataSet<?, ?>> ret = new HashMap<>();
    Map<String, PartitionedRegion> colocatedRegions =
        ColocationHelper.getAllColocationRegions(region);
    for (PartitionedRegion colocatedRegion : colocatedRegions.values()) {
      ret.put(colocatedRegion.getFullPath(),
          new LocalDataSet<>(colocatedRegion, bucketSet));
    }
    return ret;
  }

  /**
   * A utility method to retrieve all child partitioned regions that are directly colocated to the
   * specified partitioned region.<br>
   * <p>
   * For example, shipmentPR is colocated with orderPR and orderPR is colocated with customerPR.
   * <br>
   * getColocatedChildRegions(customerPR) will return List{orderPR}<br>
   * getColocatedChildRegions(orderPR) will return List{shipmentPR}<br>
   * getColocatedChildRegions(shipmentPR) will return empty List{}<br>
   *
   * @return list of all child partitioned regions colocated with the region
   * @since GemFire 5.8Beta
   */
  public static @NotNull List<PartitionedRegion> getColocatedChildRegions(
      PartitionedRegion partitionedRegion) {
    List<PartitionedRegion> colocatedChildRegions = new ArrayList<>();
    Region<?, ?> prRoot = PartitionedRegionHelper.getPRRoot(partitionedRegion.getCache());
    PartitionRegionConfig prConf = null;
    // final List allPRNamesList = new ArrayList(prRoot.keySet());
    Iterator<?> itr = prRoot.keySet().iterator();
    while (itr.hasNext()) {
      try {
        String prName = (String) itr.next();
        if (prName.equals(partitionedRegion.getRegionIdentifier())) {
          // region can't be a child of itself
          continue;
        }
        try {
          prConf = (PartitionRegionConfig) prRoot.get(prName);
        } catch (EntryDestroyedException ignore) {
          continue;
        }
        if (prConf == null) {
          continue;
        }
        int prID = prConf.getPRId();
        PartitionedRegion prRegion = PartitionedRegion.getPRFromId(prID);
        if (prRegion != null) {
          if (prRegion.getColocatedWith() != null) {
            if (prRegion.getColocatedWith().equals(partitionedRegion.getFullPath())
                || (SEPARATOR + prRegion.getColocatedWith())
                    .equals(partitionedRegion.getFullPath())) {
              // only regions directly colocatedWith partitionedRegion are
              // added to the list...
              prRegion.waitOnBucketMetadataInitialization();
              colocatedChildRegions.add(prRegion);
            }
          }
        }
      } catch (PRLocallyDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("PRLocallyDestroyedException : Region ={} is locally destroyed on this node",
              prConf.getPRId(), e);
        }
      } catch (RegionDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("RegionDestroyedException : Region ={} is destroyed.", prConf.getPRId(), e);
        }
      }
    }

    // Make the list of colocated child regions
    // is always in the same order on all nodes.
    colocatedChildRegions.sort((o1, o2) -> {
      if (o1.isShadowPR() == o2.isShadowPR()) {
        return o1.getFullPath().compareTo(o2.getFullPath());
      }
      if (o1.isShadowPR()) {
        return 1;
      }
      return -1;
    });
    return colocatedChildRegions;
  }

  public static Function<?> getFunctionInstance(Serializable function) {
    final Function<?> functionInstance;
    if (function instanceof String) {
      functionInstance = FunctionService.getFunction((String) function);
      Assert.assertTrue(functionInstance != null,
          "Function " + function + " is not registered on this node ");
    } else {
      functionInstance = (Function<?>) function;
    }
    return functionInstance;
  }

  public static PartitionedRegion getLeaderRegion(PartitionedRegion prRegion) {
    PartitionedRegion parentRegion;

    while ((parentRegion = getColocatedRegion(prRegion)) != null) {
      prRegion = parentRegion;
    }

    return prRegion;
  }

  private static String getRegionIdentifier(String regionName) {
    if (regionName.startsWith(SEPARATOR)) {
      return regionName.replace(SEPARATOR, "#");
    } else {
      return "#" + regionName.replace(SEPARATOR, "#");
    }
  }

  /**
   * Test to see if there are any persistent child regions of a partitioned region.
   */
  public static boolean hasPersistentChildRegion(PartitionedRegion region) {
    for (PartitionedRegion child : getColocatedChildRegions(region)) {
      if (child.getDataPolicy().withPersistence()) {
        return true;
      }
    }
    return false;
  }
}
