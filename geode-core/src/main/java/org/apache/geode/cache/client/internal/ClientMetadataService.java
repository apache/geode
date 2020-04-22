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

import static org.apache.geode.internal.cache.util.UncheckedUtils.cast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.logging.log4j.Logger;

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

  private final Set<String> nonPRs = new HashSet<String>();

  private boolean HONOUR_SERVER_GROUP_IN_PR_SINGLE_HOP = Boolean
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

  private Set<String> regionsBeingRefreshed = new HashSet<>();

  private final Object fetchTaskCountLock = new Object();

  public ClientMetadataService(Cache cache) {
    this.cache = cache;
  }

  private final Map<String, ClientPartitionAdvisor> clientPRAdvisors =
      new ConcurrentHashMap<String, ClientPartitionAdvisor>();
  private final Map<String, Set<ClientPartitionAdvisor>> colocatedPRAdvisors =
      new ConcurrentHashMap<String, Set<ClientPartitionAdvisor>>();

  private PartitionResolver getResolver(Region r, Object key, Object callbackArgument) {
    // First choice is one associated with the region
    final String regionFullPath = r.getFullPath();
    ClientPartitionAdvisor advisor = this.getClientPartitionAdvisor(regionFullPath);
    PartitionResolver result = null;
    if (advisor != null) {
      result = advisor.getPartitionResolver();
    }

    if (result != null) {
      return result;
    }

    // Second is the key
    if (key != null && key instanceof PartitionResolver) {
      return (PartitionResolver) key;
    }

    // Third is the callback argument
    if (callbackArgument != null && callbackArgument instanceof PartitionResolver) {
      return (PartitionResolver) callbackArgument;
    }
    // There is no resolver.
    return null;
  }

  public ServerLocation getBucketServerLocation(Region region, Operation operation, Object key,
      Object value, Object callbackArg) {
    ClientPartitionAdvisor prAdvisor = this.getClientPartitionAdvisor(region.getFullPath());
    if (prAdvisor == null) {
      return null;
    }
    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();

    final PartitionResolver resolver = getResolver(region, key, callbackArg);
    Object resolveKey;
    EntryOperation entryOp = null;
    if (resolver == null) {
      // client has not registered PartitionResolver
      // Assuming even PR at server side is not using PartitionResolver
      resolveKey = key;
    } else {
      entryOp = new EntryOperationImpl(region, operation, key, value, callbackArg);
      resolveKey = resolver.getRoutingObject(entryOp);
      if (resolveKey == null) {
        throw new IllegalStateException(
            "The RoutingObject returned by PartitionResolver is null.");
      }
    }
    int bucketId;
    if (resolver instanceof FixedPartitionResolver) {
      if (entryOp == null) {
        entryOp = new EntryOperationImpl(region, Operation.FUNCTION_EXECUTION, key, null, null);
      }
      String partition = ((FixedPartitionResolver) resolver).getPartitionName(entryOp,
          prAdvisor.getFixedPartitionNames());
      if (partition == null) {
        Object[] prms = new Object[] {region.getName(), resolver};
        throw new IllegalStateException(
            String.format("For region %s, partition resolver %s returned partition name null",
                prms));
      } else {
        bucketId = prAdvisor.assignFixedBucketId(region, partition, resolveKey);
        if (bucketId == -1) {
          return null;
        }

      }
    } else {
      bucketId = PartitionedRegionHelper.getHashKey(resolveKey, totalNumberOfBuckets);
    }

    ServerLocation bucketServerLocation = getServerLocation(region, operation, bucketId);
    ServerLocation location = null;
    if (bucketServerLocation != null) {
      location =
          new ServerLocation(bucketServerLocation.getHostName(), bucketServerLocation.getPort());
    }
    return location;
  }

  private ServerLocation getServerLocation(Region region, Operation operation, int bucketId) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = this.getClientPartitionAdvisor(regionFullPath);
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

  public Map<ServerLocation, Set> getServerToFilterMap(final Collection routingKeys,
      final Region region, boolean primaryMembersNeeded) {
    return getServerToFilterMap(routingKeys, region, primaryMembersNeeded, false);
  }

  public Map<ServerLocation, Set> getServerToFilterMap(final Collection routingKeys,
      final Region region, boolean primaryMembersNeeded, boolean bucketsAsFilter) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = this.getClientPartitionAdvisor(regionFullPath);
    if (prAdvisor == null || prAdvisor.adviseRandomServerLocation() == null) {
      scheduleGetPRMetaData((InternalRegion) region, false);
      return null;
    }
    Map<Integer, Set> bucketToKeysMap =
        groupByBucketOnClientSide(region, prAdvisor, routingKeys, bucketsAsFilter);

    Map<ServerLocation, Set> serverToKeysMap = new HashMap<>();
    Map<ServerLocation, Set<Integer>> serverToBuckets =
        groupByServerToBuckets(prAdvisor, bucketToKeysMap.keySet(), primaryMembersNeeded, region);

    if (serverToBuckets == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("One or more primary bucket locations are unknown "
            + "- scheduling metadata refresh for region {}", region.getFullPath());
      }
      scheduleGetPRMetaData((LocalRegion) region, false);
      return null;
    }

    for (Map.Entry entry : serverToBuckets.entrySet()) {
      ServerLocation server = (ServerLocation) entry.getKey();
      Set<Integer> buckets = cast(entry.getValue());
      for (Integer bucket : buckets) {
        // use LinkedHashSet to maintain the order of keys
        // the keys will be iterated several times
        Set keys = serverToKeysMap.get(server);
        if (keys == null) {
          keys = new LinkedHashSet();
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

  public Map<ServerLocation, Set<Integer>> groupByServerToAllBuckets(Region region,
      boolean primaryOnly) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = this.getClientPartitionAdvisor(regionFullPath);
    if (prAdvisor == null || prAdvisor.adviseRandomServerLocation() == null) {
      scheduleGetPRMetaData((InternalRegion) region, false);
      return null;
    }
    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();
    Set<Integer> allBucketIds = new HashSet<Integer>();
    for (int i = 0; i < totalNumberOfBuckets; i++) {
      allBucketIds.add(i);
    }
    return groupByServerToBuckets(prAdvisor, allBucketIds, primaryOnly, region);
  }

  /**
   * This function should make a map of server to buckets it is hosting. If for some bucket servers
   * are not available due to mismatch in metadata it should fill up a random server for it.
   */
  private Map<ServerLocation, Set<Integer>> groupByServerToBuckets(
      ClientPartitionAdvisor prAdvisor, Set<Integer> bucketSet, boolean primaryOnly,
      Region region) {
    if (primaryOnly) {
      Map<ServerLocation, Set<Integer>> serverToBucketsMap = new HashMap<>();
      for (Integer bucketId : bucketSet) {
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
        Set<Integer> buckets = serverToBucketsMap.get(server);
        if (buckets == null) {
          buckets = new HashSet<>(); // faster if this was an ArrayList
          serverToBucketsMap.put(server, buckets);
        }
        buckets.add(bucketId);
      }

      if (logger.isDebugEnabled()) {
        logger.debug("ClientMetadataService: The server to bucket map is : {}", serverToBucketsMap);
      }

      return serverToBucketsMap;
    } else {
      return pruneNodes(prAdvisor, bucketSet);
    }
  }


  private Map<ServerLocation, Set<Integer>> pruneNodes(ClientPartitionAdvisor prAdvisor,
      Set<Integer> buckets) {

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("ClientMetadataService: The buckets to be pruned are: {}", buckets);
    }
    Map<ServerLocation, Set<Integer>> serverToBucketsMap = new HashMap<>();
    Map<ServerLocation, Set<Integer>> prunedServerToBucketsMap = new HashMap<>();

    for (Integer bucketId : buckets) {
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
          Set<Integer> bucketSet = new HashSet<>();
          bucketSet.add(bucketId);
          serverToBucketsMap.put(server, bucketSet);
        } else {
          Set<Integer> bucketSet = serverToBucketsMap.get(server);
          bucketSet.add(bucketId);
          serverToBucketsMap.put(server, bucketSet);
        }
      }
    }
    if (isDebugEnabled) {
      logger.debug("ClientMetadataService: The server to buckets map is : {}", serverToBucketsMap);
    }

    ServerLocation randomFirstServer = null;
    if (serverToBucketsMap.isEmpty()) {
      return null;
    } else {
      int size = serverToBucketsMap.size();
      randomFirstServer =
          (ServerLocation) serverToBucketsMap.keySet().toArray()[rand.nextInt(size)];
    }
    Set<Integer> bucketSet = serverToBucketsMap.get(randomFirstServer);
    if (isDebugEnabled) {
      logger.debug(
          "ClientMetadataService: Adding the server : {} which is random and buckets {} to prunedMap",
          randomFirstServer, bucketSet);
    }
    Set<Integer> currentBucketSet = new HashSet<>(bucketSet);
    prunedServerToBucketsMap.put(randomFirstServer, bucketSet);
    serverToBucketsMap.remove(randomFirstServer);

    while (!currentBucketSet.equals(buckets)) {
      ServerLocation server = findNextServer(serverToBucketsMap.entrySet(), currentBucketSet);
      if (server == null) {
        break;
      }

      Set<Integer> bucketSet2 = serverToBucketsMap.get(server);
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


  private ServerLocation findNextServer(Set<Map.Entry<ServerLocation, Set<Integer>>> entrySet,
      Set<Integer> currentBucketSet) {

    ServerLocation server = null;
    int max = -1;
    List<ServerLocation> nodesOfEqualSize = new ArrayList<>();
    for (Map.Entry<ServerLocation, Set<Integer>> entry : entrySet) {
      Set<Integer> buckets = new HashSet<>(entry.getValue());
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

  private Map<Integer, Set> groupByBucketOnClientSide(Region region,
      ClientPartitionAdvisor prAdvisor, Collection routingKeys, boolean bucketsAsFilter) {

    Map<Integer, Set> bucketToKeysMap = new HashMap<>();
    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();
    Iterator i = routingKeys.iterator();
    while (i.hasNext()) {
      Object key = i.next();
      int bucketId = bucketsAsFilter ? ((Integer) key).intValue()
          : extractBucketID(region, prAdvisor, totalNumberOfBuckets, key);
      Set bucketKeys = bucketToKeysMap.get(bucketId);
      if (bucketKeys == null) {
        bucketKeys = new HashSet(); // faster if this was an ArrayList
        bucketToKeysMap.put(bucketId, bucketKeys);
      }
      bucketKeys.add(key);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Bucket to keys map : {}", bucketToKeysMap);
    }
    return bucketToKeysMap;
  }

  private int extractBucketID(Region region, ClientPartitionAdvisor prAdvisor,
      int totalNumberOfBuckets, Object key) {
    int bucketId = -1;
    final PartitionResolver resolver = getResolver(region, key, null);
    Object resolveKey;
    EntryOperation entryOp = null;
    if (resolver == null) {
      // client has not registered PartitionResolver
      // Assuming even PR at server side is not using PartitionResolver
      resolveKey = key;
    } else {
      entryOp = new EntryOperationImpl(region, Operation.FUNCTION_EXECUTION, key, null, null);
      resolveKey = resolver.getRoutingObject(entryOp);
      if (resolveKey == null) {
        throw new IllegalStateException(
            "The RoutingObject returned by PartitionResolver is null.");
      }
    }

    if (resolver instanceof FixedPartitionResolver) {
      if (entryOp == null) {
        entryOp = new EntryOperationImpl(region, Operation.FUNCTION_EXECUTION, key, null, null);
      }
      String partition = ((FixedPartitionResolver) resolver).getPartitionName(entryOp,
          prAdvisor.getFixedPartitionNames());
      if (partition == null) {
        Object[] prms = new Object[] {region.getName(), resolver};
        throw new IllegalStateException(
            String.format("For region %s, partition resolver %s returned partition name null",
                prms));
      } else {
        bucketId = prAdvisor.assignFixedBucketId(region, partition, resolveKey);
        // This bucketid can be -1 in some circumstances where we don't have information about
        // all the partition on the server.
        // Do proactive scheduling of metadata fetch
        if (bucketId == -1) {
          scheduleGetPRMetaData((InternalRegion) region, true);
        }
      }
    } else {
      bucketId = PartitionedRegionHelper.getHashKey(resolveKey, totalNumberOfBuckets);
    }
    return bucketId;
  }

  @VisibleForTesting
  public void scheduleGetPRMetaData(final LocalRegion region, final boolean isRecursive) {
    scheduleGetPRMetaData((InternalRegion) region, isRecursive);
  }

  public void scheduleGetPRMetaData(final InternalRegion region, final boolean isRecursive) {
    if (this.nonPRs.contains(region.getFullPath())) {
      return;
    }
    this.setMetadataStable(false);
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
      Runnable fetchTask = new Runnable() {
        @Override
        @SuppressWarnings("synthetic-access")
        public void run() {
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
        }
      };
      SingleHopClientExecutor.submitTask(fetchTask);
    }
  }

  public void getClientPRMetadata(InternalRegion region) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor advisor = null;
    InternalPool pool = region.getServerProxy().getPool();
    // Acquires lock only if it is free, else a request to fetch meta data is in
    // progress, so just return
    if (region.getClientMetaDataLock().tryLock()) {
      try {
        advisor = this.getClientPartitionAdvisor(regionFullPath);
        if (advisor == null) {
          advisor = GetClientPartitionAttributesOp.execute(pool, regionFullPath);
          if (advisor == null) {
            this.nonPRs.add(regionFullPath);
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
          ClientPartitionAdvisor colocatedAdvisor = this.getClientPartitionAdvisor(colocatedWith);
          InternalRegion leaderRegion = (InternalRegion) region.getCache().getRegion(colocatedWith);
          if (colocatedAdvisor == null) {
            scheduleGetPRMetaData(leaderRegion, true);
            return;
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
    if (this.nonPRs.contains(region.getFullPath())) {
      return;
    }
    ClientPartitionAdvisor advisor = this.getClientPartitionAdvisor(region.getFullPath());
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
      Runnable fetchTask = new Runnable() {
        @Override
        @SuppressWarnings("synthetic-access")
        public void run() {
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
        }
      };
      SingleHopClientExecutor.submitTask(fetchTask);
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
        ClientPartitionAdvisor prAdvisor = this.getClientPartitionAdvisor(regionPath);
        if (isDebugEnabled) {
          logger.debug("ClientMetadataService removing from {}{}", regionPath, prAdvisor);
        }
        if (prAdvisor != null) {
          prAdvisor.removeBucketServerLocation(serverLocation);
        }
      }
    }
  }

  public byte getMetaDataVersion(Region region, Operation operation, Object key, Object value,
      Object callbackArg) {
    ClientPartitionAdvisor prAdvisor = this.getClientPartitionAdvisor(region.getFullPath());
    if (prAdvisor == null) {
      return 0;
    }

    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();

    final PartitionResolver resolver = getResolver(region, key, callbackArg);
    Object resolveKey;
    EntryOperation entryOp = null;
    if (resolver == null) {
      // client has not registered PartitionResolver
      // Assuming even PR at server side is not using PartitionResolver
      resolveKey = key;
    } else {
      entryOp = new EntryOperationImpl(region, operation, key, value, callbackArg);
      resolveKey = resolver.getRoutingObject(entryOp);
      if (resolveKey == null) {
        throw new IllegalStateException(
            "The RoutingObject returned by PartitionResolver is null.");
      }
    }

    int bucketId;
    if (resolver instanceof FixedPartitionResolver) {
      if (entryOp == null) {
        entryOp = new EntryOperationImpl(region, Operation.FUNCTION_EXECUTION, key, null, null);
      }
      String partition = ((FixedPartitionResolver) resolver).getPartitionName(entryOp,
          prAdvisor.getFixedPartitionNames());
      if (partition == null) {
        Object[] prms = new Object[] {region.getName(), resolver};
        throw new IllegalStateException(
            String.format("For region %s, partition resolver %s returned partition name null",
                prms));
      } else {
        bucketId = prAdvisor.assignFixedBucketId(region, partition, resolveKey);
      }
    } else {
      bucketId = PartitionedRegionHelper.getHashKey(resolveKey, totalNumberOfBuckets);
    }

    BucketServerLocation66 bsl =
        (BucketServerLocation66) getPrimaryServerLocation(region, bucketId);
    if (bsl == null) {
      return 0;
    }
    return bsl.getVersion();
  }

  private ServerLocation getPrimaryServerLocation(Region region, int bucketId) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = this.getClientPartitionAdvisor(regionFullPath);
    if (prAdvisor == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ClientMetadataService#getServerLocation : Region {} prAdvisor does not exist.",
            regionFullPath);
      }
      return null;
    }

    if (prAdvisor.getColocatedWith() != null) {
      prAdvisor = this.getClientPartitionAdvisor(prAdvisor.getColocatedWith());
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
    if (this.cache.isClosed()) {
      return;
    }
    try {
      this.clientPRAdvisors.put(regionFullPath, advisor);
      if (advisor.getColocatedWith() != null) {
        String parentRegionPath = advisor.getColocatedWith();
        Set<ClientPartitionAdvisor> colocatedAdvisors =
            this.colocatedPRAdvisors.get(parentRegionPath);
        if (colocatedAdvisors == null) {
          colocatedAdvisors = new CopyOnWriteArraySet<ClientPartitionAdvisor>();
          this.colocatedPRAdvisors.put(parentRegionPath, colocatedAdvisors);
        }
        colocatedAdvisors.add(advisor);
      }
    } catch (Exception npe) {
      // ignore, shutdown case
    }

  }

  public ClientPartitionAdvisor getClientPartitionAdvisor(String regionFullPath) {
    if (this.cache.isClosed()) {
      return null;
    }
    ClientPartitionAdvisor prAdvisor = null;
    try {
      prAdvisor = this.clientPRAdvisors.get(regionFullPath);
    } catch (Exception npe) {
      return null;
    }
    return prAdvisor;
  }

  public Set<ClientPartitionAdvisor> getColocatedClientPartitionAdvisor(String regionFullPath) {
    if (this.cache.isClosed()) {
      return null;
    }
    return this.colocatedPRAdvisors.get(regionFullPath);
  }

  private Set<String> getAllRegionFullPaths() {
    if (this.cache.isClosed()) {
      return null;
    }
    Set<String> keys = null;
    try {
      keys = this.clientPRAdvisors.keySet();
    } catch (Exception npe) {
      return null;
    }
    return keys;
  }

  public void close() {
    this.clientPRAdvisors.clear();
    this.colocatedPRAdvisors.clear();
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
