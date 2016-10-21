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

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.*;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Maintains {@link ClientPartitionAdvisor} for Partitioned Regions on servers Client operations
 * will consult this service to identify the server locations on which the data for the client
 * operation is residing
 * 
 * 
 * @since GemFire 6.5
 * 
 */
public final class ClientMetadataService {

  private static final Logger logger = LogService.getLogger();

  private final Cache cache;

  private final Set<String> nonPRs = new HashSet<String>();

  private boolean HONOUR_SERVER_GROUP_IN_PR_SINGLE_HOP = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "PoolImpl.honourServerGroupsInPRSingleHop");

  public static final int SIZE_BYTES_ARRAY_RECEIVED = 2;

  public static final int INITIAL_VERSION = 0;

  /** random number generator used in pruning */
  private final Random rand = new Random();

  private volatile boolean isMetadataStable = true;

  private boolean isMetadataRefreshed_TEST_ONLY = false;

  private int refreshTaskCount = 0;

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
            LocalizedStrings.PartitionedRegionHelper_THE_ROUTINGOBJECT_RETURNED_BY_PARTITIONRESOLVER_IS_NULL
                .toLocalizedString());
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
            LocalizedStrings.PartitionedRegionHelper_FOR_REGION_0_PARTITIONRESOLVER_1_RETURNED_PARTITION_NAME_NULL
                .toLocalizedString(prms));
      } else {
        bucketId = prAdvisor.assignFixedBucketId(region, partition, resolveKey);
        if (bucketId == -1) {
          // scheduleGetPRMetaData((LocalRegion)region);
          return null;
        }

      }
    } else {
      bucketId = PartitionedRegionHelper.getHashKey(resolveKey, totalNumberOfBuckets);
    }

    ServerLocation bucketServerLocation = getServerLocation(region, operation, bucketId);
    ServerLocation location = null;
    if (bucketServerLocation != null)
      location =
          new ServerLocation(bucketServerLocation.getHostName(), bucketServerLocation.getPort());
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

    // if (prAdvisor.getColocatedWith() != null) {
    // prAdvisor = this.getClientPartitionAdvisor(prAdvisor.getColocatedWith());
    // if (prAdvisor == null) {
    // if (this.logger.fineEnabled()) {
    // this.logger.fine(
    // "ClientMetadataService#getServerLocation : Region "
    // + regionFullPath + "prAdvisor does not exist.");
    // }
    // return null;
    // }
    // }

    if (operation.isGet()) {
      return prAdvisor.adviseServerLocation(bucketId);
    } else {
      return prAdvisor.advisePrimaryServerLocation(bucketId);
    }
  }

  public Map<ServerLocation, HashSet> getServerToFilterMap(final Collection routingKeys,
      final Region region, boolean primaryMembersNeeded) {
    return getServerToFilterMap(routingKeys, region, primaryMembersNeeded, false);
  }

  public Map<ServerLocation, HashSet> getServerToFilterMap(final Collection routingKeys,
      final Region region, boolean primaryMembersNeeded, boolean bucketsAsFilter) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = this.getClientPartitionAdvisor(regionFullPath);
    if (prAdvisor == null || prAdvisor.adviseRandomServerLocation() == null) {
      scheduleGetPRMetaData((LocalRegion) region, false);
      return null;
    }
    HashMap<Integer, HashSet> bucketToKeysMap =
        groupByBucketOnClientSide(region, prAdvisor, routingKeys, bucketsAsFilter);

    HashMap<ServerLocation, HashSet> serverToKeysMap = new HashMap<ServerLocation, HashSet>();
    HashMap<ServerLocation, HashSet<Integer>> serverToBuckets =
        groupByServerToBuckets(prAdvisor, bucketToKeysMap.keySet(), primaryMembersNeeded);

    if (serverToBuckets == null) {
      return null;
    }

    for (Map.Entry entry : serverToBuckets.entrySet()) {
      ServerLocation server = (ServerLocation) entry.getKey();
      HashSet<Integer> buckets = (HashSet) entry.getValue();
      for (Integer bucket : buckets) {
        // use LinkedHashSet to maintain the order of keys
        // the keys will be iterated several times
        LinkedHashSet keys = (LinkedHashSet) serverToKeysMap.get(server);
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

  public HashMap<ServerLocation, HashSet<Integer>> groupByServerToAllBuckets(Region region,
      boolean primaryOnly) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor prAdvisor = this.getClientPartitionAdvisor(regionFullPath);
    if (prAdvisor == null || prAdvisor.adviseRandomServerLocation() == null) {
      scheduleGetPRMetaData((LocalRegion) region, false);
      return null;
    }
    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();
    HashSet<Integer> allBucketIds = new HashSet<Integer>();
    for (int i = 0; i < totalNumberOfBuckets; i++) {
      allBucketIds.add(i);
    }
    return groupByServerToBuckets(prAdvisor, allBucketIds, primaryOnly);
  }

  /**
   * This function should make a map of server to buckets it is hosting. If for some bucket servers
   * are not available due to mismatch in metadata it should fill up a random server for it.
   */
  private HashMap<ServerLocation, HashSet<Integer>> groupByServerToBuckets(
      ClientPartitionAdvisor prAdvisor, Set<Integer> bucketSet, boolean primaryOnly) {
    if (primaryOnly) {
      HashMap<ServerLocation, HashSet<Integer>> serverToBucketsMap =
          new HashMap<ServerLocation, HashSet<Integer>>();
      for (Integer bucketId : bucketSet) {
        ServerLocation server = prAdvisor.advisePrimaryServerLocation(bucketId);
        if (server == null) {
          // If we don't have the metadata for some buckets, return
          // null, indicating that we don't have any metadata. This
          // will cause us to use the non-single hop path.
          return null;
        }
        HashSet<Integer> buckets = serverToBucketsMap.get(server);
        if (buckets == null) {
          buckets = new HashSet<Integer>(); // faster if this was an ArrayList
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


  private HashMap<ServerLocation, HashSet<Integer>> pruneNodes(ClientPartitionAdvisor prAdvisor,
      Set<Integer> buckets) {

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("ClientMetadataService: The buckets to be pruned are: {}", buckets);
    }
    HashMap<ServerLocation, HashSet<Integer>> serverToBucketsMap =
        new HashMap<ServerLocation, HashSet<Integer>>();
    HashMap<ServerLocation, HashSet<Integer>> prunedServerToBucketsMap =
        new HashMap<ServerLocation, HashSet<Integer>>();

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
          HashSet<Integer> bucketSet = new HashSet<Integer>();
          bucketSet.add(bucketId);
          serverToBucketsMap.put(server, bucketSet);
        } else {
          HashSet<Integer> bucketSet = serverToBucketsMap.get(server);
          bucketSet.add(bucketId);
          serverToBucketsMap.put(server, bucketSet);
        }
      }
    }
    if (isDebugEnabled) {
      logger.debug("ClientMetadataService: The server to buckets map is : {}", serverToBucketsMap);
    }

    HashSet<Integer> currentBucketSet = new HashSet<Integer>();
    // ServerLocation randomFirstServer =
    // prAdvisor.adviseRandomServerLocation(); // get a random server here
    ServerLocation randomFirstServer = null;
    if (serverToBucketsMap.isEmpty()) {
      return null;
    } else {
      int size = serverToBucketsMap.size();
      randomFirstServer =
          (ServerLocation) serverToBucketsMap.keySet().toArray()[rand.nextInt(size)];
    }
    HashSet<Integer> bucketSet = serverToBucketsMap.get(randomFirstServer);
    if (isDebugEnabled) {
      logger.debug(
          "ClientMetadataService: Adding the server : {} which is random and buckets {} to prunedMap",
          randomFirstServer, bucketSet);
    }
    currentBucketSet.addAll(bucketSet);
    prunedServerToBucketsMap.put(randomFirstServer, bucketSet);
    serverToBucketsMap.remove(randomFirstServer);

    while (!currentBucketSet.equals(buckets)) {
      ServerLocation server = findNextServer(serverToBucketsMap.entrySet(), currentBucketSet);
      if (server == null) {
        // HashSet<Integer> rBuckets = prunedServerToBucketsMap
        // .get(randomFirstServer);
        // HashSet<Integer> remainingBuckets = new HashSet<Integer>(buckets);
        // remainingBuckets.removeAll(currentBucketSet);
        // rBuckets.addAll(remainingBuckets);
        // prunedServerToBucketsMap.put(randomFirstServer, rBuckets);
        break;
      }

      HashSet<Integer> bucketSet2 = serverToBucketsMap.get(server);
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


  private ServerLocation findNextServer(Set<Map.Entry<ServerLocation, HashSet<Integer>>> entrySet,
      HashSet<Integer> currentBucketSet) {

    ServerLocation server = null;
    int max = -1;
    ArrayList<ServerLocation> nodesOfEqualSize = new ArrayList<ServerLocation>();
    for (Map.Entry<ServerLocation, HashSet<Integer>> entry : entrySet) {
      HashSet<Integer> buckets = new HashSet<Integer>();
      buckets.addAll(entry.getValue());
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

    // return node;
    Random r = new Random();
    if (nodesOfEqualSize.size() > 0)
      return nodesOfEqualSize.get(r.nextInt(nodesOfEqualSize.size()));

    return null;
  }

  private HashMap<Integer, HashSet> groupByBucketOnClientSide(Region region,
      ClientPartitionAdvisor prAdvisor, Collection routingKeys, boolean bucketsAsFilter) {

    HashMap<Integer, HashSet> bucketToKeysMap = new HashMap();
    int totalNumberOfBuckets = prAdvisor.getTotalNumBuckets();
    Iterator i = routingKeys.iterator();
    while (i.hasNext()) {
      Object key = i.next();
      int bucketId = bucketsAsFilter ? ((Integer) key).intValue()
          : extractBucketID(region, prAdvisor, totalNumberOfBuckets, key);
      HashSet bucketKeys = bucketToKeysMap.get(bucketId);
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
            LocalizedStrings.PartitionedRegionHelper_THE_ROUTINGOBJECT_RETURNED_BY_PARTITIONRESOLVER_IS_NULL
                .toLocalizedString());
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
            LocalizedStrings.PartitionedRegionHelper_FOR_REGION_0_PARTITIONRESOLVER_1_RETURNED_PARTITION_NAME_NULL
                .toLocalizedString(prms));
      } else {
        bucketId = prAdvisor.assignFixedBucketId(region, partition, resolveKey);
        // This bucketid can be -1 in some circumstances where we don't have information about
        // all the partition on the server.
        // Do proactive scheduling of metadata fetch
        if (bucketId == -1) {
          scheduleGetPRMetaData((LocalRegion) region, true);
        }
      }
    } else {
      bucketId = PartitionedRegionHelper.getHashKey(resolveKey, totalNumberOfBuckets);
    }
    return bucketId;
  }



  public void scheduleGetPRMetaData(final LocalRegion region, final boolean isRecursive) {
    if (this.nonPRs.contains(region.getFullPath())) {
      return;
    }
    this.setMetadataStable(false);
    region.getCachePerfStats().incNonSingleHopsCount();
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
      }
      Runnable fetchTask = new Runnable() {
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

  public final void getClientPRMetadata(LocalRegion region) {
    final String regionFullPath = region.getFullPath();
    ClientPartitionAdvisor advisor = null;
    InternalPool pool = region.getServerProxy().getPool();
    // Acquires lock only if it is free, else a request to fetch meta data is in
    // progress, so just return
    if (region.clientMetaDataLock.tryLock()) {
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
          LocalRegion leaderRegion = (LocalRegion) region.getCache().getRegion(colocatedWith);
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
        region.clientMetaDataLock.unlock();
      }
    }
  }

  public void scheduleGetPRMetaData(final LocalRegion region, final boolean isRecursive,
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
      region.getCachePerfStats().incNonSingleHopsCount();
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
        region.getCachePerfStats().incNonSingleHopsCount();
        regionsBeingRefreshed.add(region.getFullPath());
        refreshTaskCount++;
      }
      Runnable fetchTask = new Runnable() {
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
            LocalizedStrings.PartitionedRegionHelper_THE_ROUTINGOBJECT_RETURNED_BY_PARTITIONRESOLVER_IS_NULL
                .toLocalizedString());
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
            LocalizedStrings.PartitionedRegionHelper_FOR_REGION_0_PARTITIONRESOLVER_1_RETURNED_PARTITION_NAME_NULL
                .toLocalizedString(prms));
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
    if (this.cache.isClosed() || this.clientPRAdvisors == null) {
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
    if (this.cache.isClosed() || this.clientPRAdvisors == null) {
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
    if (this.cache.isClosed() || this.clientPRAdvisors == null
        || this.colocatedPRAdvisors == null) {
      return null;
    }
    return this.colocatedPRAdvisors.get(regionFullPath);
  }

  private Set<String> getAllRegionFullPaths() {
    if (this.cache.isClosed() || this.clientPRAdvisors == null) {
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

  public boolean isRefreshMetadataTestOnly() {
    return isMetadataRefreshed_TEST_ONLY;
  }

  public void satisfyRefreshMetadata_TEST_ONLY(boolean isRefreshMetadataTestOnly) {
    isMetadataRefreshed_TEST_ONLY = isRefreshMetadataTestOnly;
  }

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

  public int getRefreshTaskCount() {
    synchronized (fetchTaskCountLock) {
      return refreshTaskCount;
    }
  }
}
