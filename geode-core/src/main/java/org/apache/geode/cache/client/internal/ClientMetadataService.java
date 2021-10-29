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
package org.apache.geode.cache.client.internal;


import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.cache.EntryOperationImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.partitioned.BucketId;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Maintains {@link ClientPartitionAdvisor} for Partitioned Regions on servers Client operations
 * will consult this service to identify the server locations on which the data for the client
 * operation is residing
 *
 * @since GemFire 6.5
 */
public class ClientMetadataService {

  private static final Logger logger = LogService.getLogger();

  private final Cache cache;

  private final Set<String> nonPRs = new HashSet<>();

  private final boolean HONOUR_SERVER_GROUP_IN_PR_SINGLE_HOP = Boolean
      .getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop");

  public static final int SIZE_BYTES_ARRAY_RECEIVED = 2;

  public static final int INITIAL_VERSION = 0;

  /**
   * random number generator used in pruning
   */
  private final Random rand = new Random();

  private volatile boolean isMetadataStable = true;

  private boolean isMetadataRefreshed_TEST_ONLY = false;

  /** for testing - the current number of metadata refresh tasks running or queued to run */
  private int refreshTaskCount = 0;

  /** for testing - the total number of scheduled metadata refreshes */
  private long totalRefreshTaskCount = 0;

  private final Set<String> regionsBeingRefreshed = new HashSet<>();

  private final Object fetchTaskCountLock = new Object();

  public ClientMetadataService(Cache cache) {
    this.cache = cache;
  }

  private final Map<String, ClientPartitionAdvisor> clientPRAdvisors =
      new ConcurrentHashMap<>();
  private final Map<String, Set<ClientPartitionAdvisor>> colocatedPRAdvisors =
      new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  private <K, V> PartitionResolver<K, V> getResolver(Region<K, V> r, Object key,
      Object callbackArgument) {
    // First choice is one associated with the region
    final String regionFullPath = r.getFullPath();
    ClientPartitionAdvisor advisor = getClientPartitionAdvisor(regionFullPath);
    if (advisor != null) {
      final PartitionResolver<K, V> result = uncheckedCast(advisor.getPartitionResolver());
      if (result != null) {
        return result;
      }
    }

    // Second is the key
    if (key instanceof PartitionResolver) {
      return (PartitionResolver<K, V>) key;
    }

    // Third is the callback argument
    if (callbackArgument instanceof PartitionResolver) {
      return (PartitionResolver<K, V>) callbackArgument;
    }

    // There is no resolver.
    return null;
  }

  public <K, V> ServerLocation getBucketServerLocation(Region<K, V> region, Operation operation,
      K key,
      V value, Object callbackArg) {
    ClientPartitionAdvisor prAdvisor = getClientPartitionAdvisor(region.getFullPath());
    if (prAdvisor == null) {
      return null;
    }
    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();

    final PartitionResolver<K, V> resolver = getResolver(region, key, callbackArg);
    Object resolveKey;
    EntryOperation<K, V> entryOp = null;
    if (resolver == null) {
      // client has not registered PartitionResolver
      // Assuming even PR at server side is not using PartitionResolver
      resolveKey = key;
    } else {
      entryOp = new EntryOperationImpl<>(region, operation, key, value, callbackArg);
      resolveKey = resolver.getRoutingObject(entryOp);
      if (resolveKey == null) {
        throw new IllegalStateException(
            "The RoutingObject returned by PartitionResolver is null.");
      }
    }
    final BucketId bucketId;
    if (resolver instanceof FixedPartitionResolver) {
      String partition = ((FixedPartitionResolver<K, V>) resolver).getPartitionName(entryOp,
          prAdvisor.getFixedPartitionNames());
      if (partition == null) {
        throw new IllegalStateException(
            String.format("For region %s, partition resolver %s returned partition name null",
                region.getName(), resolver));
      } else {
        bucketId = prAdvisor.assignFixedBucketId(partition, resolveKey);
        if (bucketId == BucketId.UNKNOWN_BUCKET) {
          return null;
        }

      }
    } else {
      bucketId = PartitionedRegionHelper.getBucket(resolveKey, totalNumberOfBuckets);
    }

    ServerLocation bucketServerLocation = getServerLocation(region, operation, bucketId);
    ServerLocation location = null;
    if (bucketServerLocation != null) {
      location =
          new ServerLocation(bucketServerLocation.getHostName(), bucketServerLocation.getPort());
    }
    return location;
  }

  private ServerLocation getServerLocation(Region<?, ?> region, Operation operation,
      final BucketId bucketId) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = getClientPartitionAdvisor(regionFullPath);
    if (prAdvisor == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ClientMetadataService#getServerLocation : Region {} prAdvisor does not exist.",
            regionFullPath);
      }
      return null;
    }

    if (operation.isGet()) {
      return prAdvisor.adviseServerLocation(bucketId);
    } else {
      return prAdvisor.advisePrimaryServerLocation(bucketId);
    }
  }

  public <K, V> Map<ServerLocation, Set<K>> getServerToFilterMap(final Collection<K> routingKeys,
      final Region<K, V> region, boolean primaryMembersNeeded) {
    return getServerToFilterMap(routingKeys, region, primaryMembersNeeded, false);
  }

  public <K, V> Map<ServerLocation, Set<K>> getServerToFilterMap(final Collection<K> routingKeys,
      final Region<K, V> region, boolean primaryMembersNeeded, boolean bucketsAsFilter) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = getClientPartitionAdvisor(regionFullPath);
    if (prAdvisor == null || prAdvisor.adviseRandomServerLocation() == null) {
      scheduleGetPRMetaData((InternalRegion) region, false);
      return null;
    }
    Map<BucketId, Set<K>> bucketToKeysMap =
        groupByBucketOnClientSide(region, prAdvisor, routingKeys, bucketsAsFilter);

    Map<ServerLocation, Set<K>> serverToKeysMap = new HashMap<>();
    Map<ServerLocation, Set<BucketId>> serverToBuckets =
        groupByServerToBuckets(prAdvisor, bucketToKeysMap.keySet(), primaryMembersNeeded, region);

    if (serverToBuckets == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("One or more primary bucket locations are unknown "
            + "- scheduling metadata refresh for region {}", region.getFullPath());
      }
      scheduleGetPRMetaData((LocalRegion) region, false);
      return null;
    }

    for (Map.Entry<ServerLocation, Set<BucketId>> entry : serverToBuckets.entrySet()) {
      ServerLocation server = entry.getKey();
      Set<BucketId> buckets = entry.getValue();
      for (BucketId bucket : buckets) {
        // use LinkedHashSet to maintain the order of keys
        // the keys will be iterated several times
        Set<K> keys = serverToKeysMap.get(server);
        if (keys == null) {
          keys = new LinkedHashSet<>();
        }
        keys.addAll(bucketToKeysMap.get(bucket));
        serverToKeysMap.put(server, keys);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Returning server to keys map : {}", serverToKeysMap);
    }

    return serverToKeysMap;
  }

  public Map<ServerLocation, Set<BucketId>> groupByServerToAllBuckets(Region<?, ?> region,
      boolean primaryOnly) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = getClientPartitionAdvisor(regionFullPath);
    if (prAdvisor == null || prAdvisor.adviseRandomServerLocation() == null) {
      scheduleGetPRMetaData((InternalRegion) region, false);
      return null;
    }
    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();
    Set<BucketId> allBucketIds = new HashSet<>();
    for (int i = 0; i < totalNumberOfBuckets; i++) {
      allBucketIds.add(BucketId.valueOf(i));
    }
    return groupByServerToBuckets(prAdvisor, allBucketIds, primaryOnly, region);
  }

  /**
   * This function should make a map of server to buckets it is hosting. If for some bucket servers
   * are not available due to mismatch in metadata it should fill up a random server for it.
   */
  private Map<ServerLocation, Set<BucketId>> groupByServerToBuckets(
      ClientPartitionAdvisor prAdvisor, Set<BucketId> bucketSet, boolean primaryOnly,
      Region<?, ?> region) {
    if (primaryOnly) {
      Map<ServerLocation, Set<BucketId>> serverToBucketsMap = new HashMap<>();
      for (BucketId bucketId : bucketSet) {
        ServerLocation server = prAdvisor.advisePrimaryServerLocation(bucketId);
        if (server == null) {
          // If we don't have the metadata for some buckets, return
          // null, indicating that we don't have any metadata. This
          // will cause us to use the non-single hop path.
          logger.info("Primary for bucket {} is not known for Region {}.  "
              + "Known server locations: {}", bucketId, region.getFullPath(),
              prAdvisor.adviseServerLocations(bucketId));
          return null;
        }
        serverToBucketsMap.computeIfAbsent(server, k -> new HashSet<>()).add(bucketId);
      }

      if (logger.isDebugEnabled()) {
        logger.debug("ClientMetadataService: The server to bucket map is : {}", serverToBucketsMap);
      }

      return serverToBucketsMap;
    } else {
      return pruneNodes(prAdvisor, bucketSet);
    }
  }


  private Map<ServerLocation, Set<BucketId>> pruneNodes(ClientPartitionAdvisor prAdvisor,
      Set<BucketId> buckets) {

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("ClientMetadataService: The buckets to be pruned are: {}", buckets);
    }
    Map<ServerLocation, Set<BucketId>> serverToBucketsMap = new HashMap<>();
    Map<ServerLocation, Set<BucketId>> prunedServerToBucketsMap = new HashMap<>();

    for (BucketId bucketId : buckets) {
      List<BucketServerLocation66> serversList = prAdvisor.adviseServerLocations(bucketId);
      if (isDebugEnabled) {
        logger.debug("ClientMetadataService: For bucketId {} the server list is {}", bucketId,
            serversList);
      }
      if (serversList == null || serversList.size() == 0) {
        // If we don't have the metadata for some buckets, return
        // null, indicating that we don't have any metadata. This
        // will cause us to use the non-single hop path.
        return null;
      }

      if (isDebugEnabled) {
        logger.debug("ClientMetadataService: The buckets owners of the bucket: {} are: {}",
            bucketId, serversList);
      }

      for (ServerLocation server : serversList) {
        if (serverToBucketsMap.get(server) == null) {
          Set<BucketId> bucketSet = new HashSet<>();
          bucketSet.add(bucketId);
          serverToBucketsMap.put(server, bucketSet);
        } else {
          Set<BucketId> bucketSet = serverToBucketsMap.get(server);
          bucketSet.add(bucketId);
          serverToBucketsMap.put(server, bucketSet);
        }
      }
    }
    if (isDebugEnabled) {
      logger.debug("ClientMetadataService: The server to buckets map is : {}", serverToBucketsMap);
    }

    final ServerLocation randomFirstServer;
    if (serverToBucketsMap.isEmpty()) {
      return null;
    } else {
      int size = serverToBucketsMap.size();
      randomFirstServer =
          (ServerLocation) serverToBucketsMap.keySet().toArray()[rand.nextInt(size)];
    }
    Set<BucketId> bucketSet = serverToBucketsMap.get(randomFirstServer);
    if (isDebugEnabled) {
      logger.debug(
          "ClientMetadataService: Adding the server : {} which is random and buckets {} to prunedMap",
          randomFirstServer, bucketSet);
    }
    Set<BucketId> currentBucketSet = new HashSet<>(bucketSet);
    prunedServerToBucketsMap.put(randomFirstServer, bucketSet);
    serverToBucketsMap.remove(randomFirstServer);

    while (!currentBucketSet.equals(buckets)) {
      ServerLocation server = findNextServer(serverToBucketsMap.entrySet(), currentBucketSet);
      if (server == null) {
        break;
      }

      Set<BucketId> bucketSet2 = serverToBucketsMap.get(server);
      bucketSet2.removeAll(currentBucketSet);
      if (bucketSet2.isEmpty()) {
        serverToBucketsMap.remove(server);
        continue;
      }
      currentBucketSet.addAll(bucketSet2);
      prunedServerToBucketsMap.put(server, bucketSet2);
      if (isDebugEnabled) {
        logger.debug(
            "ClientMetadataService: Adding the server : {} and buckets {} to prunedServer.", server,
            bucketSet2);
      }
      serverToBucketsMap.remove(server);
    }

    if (isDebugEnabled) {
      logger.debug("ClientMetadataService: The final prunedServerToBucket calculated is : {}",
          prunedServerToBucketsMap);
    }

    return prunedServerToBucketsMap;
  }


  private ServerLocation findNextServer(Set<Map.Entry<ServerLocation, Set<BucketId>>> entrySet,
      Set<BucketId> currentBucketSet) {

    ServerLocation server = null;
    int max = -1;
    List<ServerLocation> nodesOfEqualSize = new ArrayList<>();
    for (Map.Entry<ServerLocation, Set<BucketId>> entry : entrySet) {
      Set<BucketId> buckets = new HashSet<>(entry.getValue());
      buckets.removeAll(currentBucketSet);

      if (max < buckets.size()) {
        max = buckets.size();
        server = entry.getKey();
        nodesOfEqualSize.clear();
        nodesOfEqualSize.add(server);
      } else if (max == buckets.size()) {
        nodesOfEqualSize.add(server);
      }
    }

    Random r = new Random();
    if (nodesOfEqualSize.size() > 0) {
      return nodesOfEqualSize.get(r.nextInt(nodesOfEqualSize.size()));
    }

    return null;
  }

  private <K, V> Map<BucketId, Set<K>> groupByBucketOnClientSide(Region<K, V> region,
      ClientPartitionAdvisor prAdvisor, Collection<K> routingKeys, boolean bucketsAsFilter) {

    Map<BucketId, Set<K>> bucketToKeysMap = new HashMap<>();
    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();
    for (final K key : routingKeys) {
      BucketId bucketId = bucketsAsFilter ? BucketId.valueOf((Integer) key)
          : extractBucketID(region, prAdvisor, totalNumberOfBuckets, key);
      Set<K> bucketKeys = bucketToKeysMap.computeIfAbsent(bucketId, k -> new HashSet<>());
      bucketKeys.add(key);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Bucket to keys map : {}", bucketToKeysMap);
    }
    return bucketToKeysMap;
  }

  private <K, V> BucketId extractBucketID(Region<K, V> region, ClientPartitionAdvisor prAdvisor,
      int totalNumberOfBuckets, K key) {
    final PartitionResolver<K, V> resolver = getResolver(region, key, null);
    final Object resolveKey;
    EntryOperation<K, V> entryOp = null;
    if (resolver == null) {
      // client has not registered PartitionResolver
      // Assuming even PR at server side is not using PartitionResolver
      resolveKey = key;
    } else {
      entryOp = new EntryOperationImpl<>(region, Operation.FUNCTION_EXECUTION, key, null, null);
      resolveKey = resolver.getRoutingObject(entryOp);
      if (resolveKey == null) {
        throw new IllegalStateException(
            "The RoutingObject returned by PartitionResolver is null.");
      }
    }

    final BucketId bucketId;
    if (resolver instanceof FixedPartitionResolver) {
      String partition = ((FixedPartitionResolver<K, V>) resolver).getPartitionName(entryOp,
          prAdvisor.getFixedPartitionNames());
      if (partition == null) {
        throw new IllegalStateException(
            String.format("For region %s, partition resolver %s returned partition name null",
                region.getName(), resolver));
      } else {
        bucketId = prAdvisor.assignFixedBucketId(partition, resolveKey);
        // This bucketId can be -1 in some circumstances where we don't have information about
        // all the partition on the server.
        // Do proactive scheduling of metadata fetch
        if (bucketId == BucketId.UNKNOWN_BUCKET) {
          scheduleGetPRMetaData((InternalRegion) region, true);
        }
      }
    } else {
      bucketId = PartitionedRegionHelper.getBucket(resolveKey, totalNumberOfBuckets);
    }
    return bucketId;
  }

  @VisibleForTesting
  public void scheduleGetPRMetaData(final LocalRegion region, final boolean isRecursive) {
    scheduleGetPRMetaData((InternalRegion) region, isRecursive);
  }

  public void scheduleGetPRMetaData(final InternalRegion region, final boolean isRecursive) {
    if (nonPRs.contains(region.getFullPath())) {
      return;
    }
    setMetadataStable(false);
    if (isRecursive) {
      try {
        getClientPRMetadata(region);
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable e) {
        SystemFailure.checkFailure();
        if (logger.isDebugEnabled()) {
          logger.debug("An exception occurred while fetching metadata", e);
        }
      }
    } else {
      synchronized (fetchTaskCountLock) {
        refreshTaskCount++;
        totalRefreshTaskCount++;
      }
      SingleHopClientExecutor.submitTask(() -> {
        try {
          getClientPRMetadata(region);
        } catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        } catch (Throwable e) {
          SystemFailure.checkFailure();
          if (logger.isDebugEnabled()) {
            logger.debug("An exception occurred while fetching metadata", e);
          }
        } finally {
          synchronized (fetchTaskCountLock) {
            refreshTaskCount--;
          }
        }
      });
    }
  }

  public void getClientPRMetadata(InternalRegion region) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor advisor;
    InternalPool pool = region.getServerProxy().getPool();
    // Acquires lock only if it is free, else a request to fetch metadata is in
    // progress, so just return
    if (region.getClientMetaDataLock().tryLock()) {
      try {
        advisor = getClientPartitionAdvisor(regionFullPath);
        if (advisor == null) {
          advisor = GetClientPartitionAttributesOp.execute(pool, regionFullPath);
          if (advisor == null) {
            nonPRs.add(regionFullPath);
            return;
          }
          addClientPartitionAdvisor(regionFullPath, advisor);
        } else {
          if (advisor.getFixedPAMap() != null && !advisor.isFPAAttrsComplete()) {
            ClientPartitionAdvisor newAdvisor =
                GetClientPartitionAttributesOp.execute(pool, regionFullPath);
            advisor.updateFixedPAMap(newAdvisor.getFixedPAMap());
          }
        }
        String colocatedWith = advisor.getColocatedWith();
        if (colocatedWith == null) {
          isMetadataRefreshed_TEST_ONLY = true;
          GetClientPRMetaDataOp.execute(pool, regionFullPath, this);
          region.getCachePerfStats().incMetaDataRefreshCount();
        } else {
          ClientPartitionAdvisor colocatedAdvisor = getClientPartitionAdvisor(colocatedWith);
          InternalRegion leaderRegion = (InternalRegion) region.getCache().getRegion(colocatedWith);
          if (colocatedAdvisor == null) {
            scheduleGetPRMetaData(leaderRegion, true);
          } else {
            isMetadataRefreshed_TEST_ONLY = true;
            GetClientPRMetaDataOp.execute(pool, colocatedWith, this);
            leaderRegion.getCachePerfStats().incMetaDataRefreshCount();
          }
        }
      } finally {
        region.getClientMetaDataLock().unlock();
      }
    }
  }

  public void scheduleGetPRMetaData(final InternalRegion region, final boolean isRecursive,
      byte nwHopType) {
    if (nonPRs.contains(region.getFullPath())) {
      return;
    }
    ClientPartitionAdvisor advisor = getClientPartitionAdvisor(region.getFullPath());
    if (advisor != null && advisor.getServerGroup().length() != 0
        && HONOUR_SERVER_GROUP_IN_PR_SINGLE_HOP) {
      if (logger.isDebugEnabled()) {
        logger.debug("Scheduling metadata refresh: {} region: {}", nwHopType, region.getName());
      }
      if (nwHopType == PartitionedRegion.NETWORK_HOP_TO_DIFFERENT_GROUP) {
        return;
      }
    }
    synchronized (fetchTaskCountLock) {
      if (regionsBeingRefreshed.contains(region.getFullPath())) {
        return;
      }
    }
    if (isRecursive) {
      try {
        getClientPRMetadata(region);
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable e) {
        SystemFailure.checkFailure();
        if (logger.isDebugEnabled()) {
          logger.debug("An exception occurred while fetching metadata", e);
        }
      }
    } else {
      synchronized (fetchTaskCountLock) {
        if (regionsBeingRefreshed.contains(region.getFullPath())) {
          return;
        }
        regionsBeingRefreshed.add(region.getFullPath());
        refreshTaskCount++;
        totalRefreshTaskCount++;
      }
      SingleHopClientExecutor.submitTask(() -> {
        try {
          getClientPRMetadata(region);
        } catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        } catch (Throwable e) {
          SystemFailure.checkFailure();
          if (logger.isDebugEnabled()) {
            logger.debug("An exception occurred while fetching metadata", e);
          }
        } finally {
          synchronized (fetchTaskCountLock) {
            regionsBeingRefreshed.remove(region.getFullPath());
            refreshTaskCount--;
          }
        }
      });
    }
  }

  public void removeBucketServerLocation(ServerLocation serverLocation) {
    Set<String> keys = getAllRegionFullPaths();
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("ClientMetadataService removing a ServerLocation :{}{}", serverLocation, keys);
    }
    if (keys != null) {
      for (String regionPath : keys) {
        ClientPartitionAdvisor prAdvisor = getClientPartitionAdvisor(regionPath);
        if (isDebugEnabled) {
          logger.debug("ClientMetadataService removing from {}{}", regionPath, prAdvisor);
        }
        if (prAdvisor != null) {
          prAdvisor.removeBucketServerLocation(serverLocation);
        }
      }
    }
  }

  public <K, V> byte getMetaDataVersion(Region<K, V> region, Operation operation, K key, V value,
      Object callbackArg) {
    ClientPartitionAdvisor prAdvisor = getClientPartitionAdvisor(region.getFullPath());
    if (prAdvisor == null) {
      return 0;
    }

    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();

    final PartitionResolver<K, V> resolver = getResolver(region, key, callbackArg);
    Object resolveKey;
    EntryOperation<K, V> entryOp = null;
    if (resolver == null) {
      // client has not registered PartitionResolver
      // Assuming even PR at server side is not using PartitionResolver
      resolveKey = key;
    } else {
      entryOp = new EntryOperationImpl<>(region, operation, key, value, callbackArg);
      resolveKey = resolver.getRoutingObject(entryOp);
      if (resolveKey == null) {
        throw new IllegalStateException(
            "The RoutingObject returned by PartitionResolver is null.");
      }
    }

    final BucketId bucketId;
    if (resolver instanceof FixedPartitionResolver) {
      String partition = ((FixedPartitionResolver<K, V>) resolver).getPartitionName(entryOp,
          prAdvisor.getFixedPartitionNames());
      if (partition == null) {
        throw new IllegalStateException(
            String.format("For region %s, partition resolver %s returned partition name null",
                region.getName(), resolver));
      } else {
        bucketId = prAdvisor.assignFixedBucketId(partition, resolveKey);
      }
    } else {
      bucketId = PartitionedRegionHelper.getBucket(resolveKey, totalNumberOfBuckets);
    }

    BucketServerLocation66 bsl =
        (BucketServerLocation66) getPrimaryServerLocation(region, bucketId);
    if (bsl == null) {
      return 0;
    }
    return bsl.getVersion();
  }

  private ServerLocation getPrimaryServerLocation(Region<?, ?> region,
      final @NotNull BucketId bucketId) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = getClientPartitionAdvisor(regionFullPath);
    if (prAdvisor == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ClientMetadataService#getServerLocation : Region {} prAdvisor does not exist.",
            regionFullPath);
      }
      return null;
    }

    if (prAdvisor.getColocatedWith() != null) {
      prAdvisor = getClientPartitionAdvisor(prAdvisor.getColocatedWith());
      if (prAdvisor == null) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "ClientMetadataService#getServerLocation : Region {} prAdvisor does not exist.",
              regionFullPath);
        }
        return null;
      }
    }
    return prAdvisor.advisePrimaryServerLocation(bucketId);
  }

  private void addClientPartitionAdvisor(String regionFullPath, ClientPartitionAdvisor advisor) {
    if (cache.isClosed()) {
      return;
    }
    try {
      clientPRAdvisors.put(regionFullPath, advisor);
      if (advisor.getColocatedWith() != null) {
        String parentRegionPath = advisor.getColocatedWith();
        Set<ClientPartitionAdvisor> colocatedAdvisors =
            colocatedPRAdvisors.get(parentRegionPath);
        if (colocatedAdvisors == null) {
          colocatedAdvisors = new CopyOnWriteArraySet<>();
          colocatedPRAdvisors.put(parentRegionPath, colocatedAdvisors);
        }
        colocatedAdvisors.add(advisor);
      }
    } catch (Exception npe) {
      // ignore, shutdown case
    }

  }

  public ClientPartitionAdvisor getClientPartitionAdvisor(String regionFullPath) {
    if (cache.isClosed()) {
      return null;
    }
    try {
      return clientPRAdvisors.get(regionFullPath);
    } catch (Exception ignored) {
    }
    return null;
  }

  public Set<ClientPartitionAdvisor> getColocatedClientPartitionAdvisor(String regionFullPath) {
    if (cache.isClosed()) {
      return null;
    }
    return colocatedPRAdvisors.get(regionFullPath);
  }

  private Set<String> getAllRegionFullPaths() {
    if (cache.isClosed()) {
      return null;
    }
    try {
      return clientPRAdvisors.keySet();
    } catch (Exception ignored) {
    }
    return null;
  }

  public void close() {
    clientPRAdvisors.clear();
    colocatedPRAdvisors.clear();
  }

  @VisibleForTesting
  public boolean isRefreshMetadataTestOnly() {
    return isMetadataRefreshed_TEST_ONLY;
  }

  @VisibleForTesting
  public void satisfyRefreshMetadata_TEST_ONLY(boolean isRefreshMetadataTestOnly) {
    isMetadataRefreshed_TEST_ONLY = isRefreshMetadataTestOnly;
  }

  @VisibleForTesting
  public Map<String, ClientPartitionAdvisor> getClientPRMetadata_TEST_ONLY() {
    return clientPRAdvisors;
  }

  public Map<String, ClientPartitionAdvisor> getClientPartitionAttributesMap() {
    return clientPRAdvisors;
  }

  public boolean honourServerGroup() {
    return HONOUR_SERVER_GROUP_IN_PR_SINGLE_HOP;
  }

  public boolean isMetadataStable() {
    return isMetadataStable;
  }

  public void setMetadataStable(boolean isMetadataStable) {
    this.isMetadataStable = isMetadataStable;
  }

  /** For Testing */
  @VisibleForTesting
  public int getRefreshTaskCount_TEST_ONLY() {
    synchronized (fetchTaskCountLock) {
      return refreshTaskCount;
    }
  }

  /** for testing */
  @VisibleForTesting
  public long getTotalRefreshTaskCount_TEST_ONLY() {
    synchronized (fetchTaskCountLock) {
      return totalRefreshTaskCount;
    }
  }
}
