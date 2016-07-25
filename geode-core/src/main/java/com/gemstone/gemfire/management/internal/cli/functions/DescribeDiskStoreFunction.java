/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.File;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.ArrayUtils;
import com.gemstone.gemfire.management.internal.cli.domain.DiskStoreDetails;
import com.gemstone.gemfire.management.internal.cli.util.DiskStoreNotFoundException;

import org.apache.logging.log4j.Logger;

/**
 * The DescribeDiskStoreFunction class is an implementation of a GemFire Function used to collect information
 * and details about a particular disk store for a particular GemFire distributed system member.
 *
 * @see com.gemstone.gemfire.cache.DiskStore
 * @see com.gemstone.gemfire.cache.execute.Function
 * @see com.gemstone.gemfire.cache.execute.FunctionAdapter
 * @see com.gemstone.gemfire.cache.execute.FunctionContext
 * @see com.gemstone.gemfire.internal.InternalEntity
 * @see com.gemstone.gemfire.management.internal.cli.domain.DiskStoreDetails
 * @since GemFire 7.0
 */
public class DescribeDiskStoreFunction extends FunctionAdapter implements InternalEntity {

  private static final Logger logger = LogService.getLogger();

  private static final Set<DataPolicy> PERSISTENT_DATA_POLICIES = new HashSet<>(2);

  static {
    PERSISTENT_DATA_POLICIES.add(DataPolicy.PERSISTENT_PARTITION);
    PERSISTENT_DATA_POLICIES.add(DataPolicy.PERSISTENT_REPLICATE);
  }

  protected static void assertState(final boolean condition, final String message, final Object... args) {
    if (!condition) {
      throw new IllegalStateException(String.format(message, args));
    }
  }

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public String getId() {
    return getClass().getName();
  }

  @SuppressWarnings("unused")
  public void init(final Properties props) {
  }

  public void execute(final FunctionContext context) {
    Cache cache = getCache();

    try {
      if (cache instanceof InternalCache) {
        InternalCache gemfireCache = (InternalCache) cache;

        DistributedMember member = gemfireCache.getMyId();

        String diskStoreName = (String) context.getArguments();
        String memberId = member.getId();
        String memberName = member.getName();

        DiskStore diskStore = gemfireCache.findDiskStore(diskStoreName);

        if (diskStore != null) {
          DiskStoreDetails diskStoreDetails = new DiskStoreDetails(diskStore.getDiskStoreUUID(),
            diskStore.getName(), memberId, memberName);

          diskStoreDetails.setAllowForceCompaction(diskStore.getAllowForceCompaction());
          diskStoreDetails.setAutoCompact(diskStore.getAutoCompact());
          diskStoreDetails.setCompactionThreshold(diskStore.getCompactionThreshold());
          diskStoreDetails.setMaxOplogSize(diskStore.getMaxOplogSize());
          diskStoreDetails.setQueueSize(diskStore.getQueueSize());
          diskStoreDetails.setTimeInterval(diskStore.getTimeInterval());
          diskStoreDetails.setWriteBufferSize(diskStore.getWriteBufferSize());
          diskStoreDetails.setDiskUsageWarningPercentage(diskStore.getDiskUsageWarningPercentage());
          diskStoreDetails.setDiskUsageCriticalPercentage(diskStore.getDiskUsageCriticalPercentage());
          
          setDiskDirDetails(diskStore, diskStoreDetails);
          setRegionDetails(gemfireCache, diskStore, diskStoreDetails);
          setCacheServerDetails(gemfireCache, diskStore, diskStoreDetails);
          setGatewayDetails(gemfireCache, diskStore, diskStoreDetails);
          setPdxSerializationDetails(gemfireCache, diskStore, diskStoreDetails);
          setAsyncEventQueueDetails(gemfireCache, diskStore, diskStoreDetails);

          context.getResultSender().lastResult(diskStoreDetails);
        }
        else {
          context.getResultSender().sendException(new DiskStoreNotFoundException(String.format(
            "A disk store with name (%1$s) was not found on member (%2$s).",
              diskStoreName, memberName)));
        }
      }
    }
    catch (Exception e) {
      logger.error("Error occurred while executing 'describe disk-store': {}!", e.getMessage(), e);
      context.getResultSender().sendException(e);
    }
  }

  private void setDiskDirDetails(final DiskStore diskStore, final DiskStoreDetails diskStoreDetails) {
    File[] diskDirs = diskStore.getDiskDirs();
    Integer[] diskDirSizes = ArrayUtils.toIntegerArray(diskStore.getDiskDirSizes());

    assertState(diskDirs.length == diskDirSizes.length,
      "The number of disk directories with a specified size (%1$d) does not match the number of disk directories (%2$d)!",
        diskDirSizes.length, diskDirs.length);

    for (int index = 0; index < diskDirs.length; index++) {
      diskStoreDetails.add(new DiskStoreDetails.DiskDirDetails(diskDirs[index].getAbsolutePath(),
        ArrayUtils.getElementAtIndex(diskDirSizes, index, 0)));
    }
  }

  protected String getDiskStoreName(final Region region) {
    return StringUtils.defaultIfBlank(region.getAttributes().getDiskStoreName(),
      DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
  }

  protected boolean isOverflowToDisk(final Region region) {
    return (region.getAttributes().getEvictionAttributes() != null && EvictionAction.OVERFLOW_TO_DISK.equals(
      region.getAttributes().getEvictionAttributes().getAction()));
  }

  protected boolean isPersistent(final Region region) {
    return region.getAttributes().getDataPolicy().withPersistence();
  }

  protected boolean isUsingDiskStore(final Region region, final DiskStore diskStore) {
    return ((isPersistent(region) || isOverflowToDisk(region)) && ObjectUtils.equals(getDiskStoreName(region), diskStore.getName()));
  }

  protected void setRegionDetails(final InternalCache cache, final DiskStore diskStore, final DiskStoreDetails diskStoreDetails) {
    for (Region<?, ?> region : cache.rootRegions()) {
      setRegionDetails(region, diskStore, diskStoreDetails);
    }
  }

  private void setRegionDetails(final Region<?, ?> region, final DiskStore diskStore, final DiskStoreDetails diskStoreDetails) {
    if (isUsingDiskStore(region, diskStore)) {
      String regionFullPath = region.getFullPath();
      DiskStoreDetails.RegionDetails regionDetails = new DiskStoreDetails.RegionDetails(regionFullPath,
        StringUtils.defaultIfBlank(region.getName(), regionFullPath));
      regionDetails.setOverflowToDisk(isOverflowToDisk(region));
      regionDetails.setPersistent(isPersistent(region));
      diskStoreDetails.add(regionDetails);
    }

    for (Region<?, ?> subregion : region.subregions(false)) {
      setRegionDetails(subregion, diskStore, diskStoreDetails); // depth-first, recursive strategy
    }
  }

  protected String getDiskStoreName(final CacheServer cacheServer) {
    return (cacheServer.getClientSubscriptionConfig() == null ? null : StringUtils.defaultIfBlank(
      cacheServer.getClientSubscriptionConfig().getDiskStoreName(), DiskStoreDetails.DEFAULT_DISK_STORE_NAME));
  }

  protected boolean isUsingDiskStore(final CacheServer cacheServer, final DiskStore diskStore) {
    return ObjectUtils.equals(getDiskStoreName(cacheServer), diskStore.getName());
  }

  protected void setCacheServerDetails(final InternalCache cache, final DiskStore diskStore, final DiskStoreDetails diskStoreDetails) {
    for (CacheServer cacheServer : cache.getCacheServers()) {
      if (isUsingDiskStore(cacheServer, diskStore)) {
        DiskStoreDetails.CacheServerDetails cacheServerDetails = new DiskStoreDetails.CacheServerDetails(
          cacheServer.getBindAddress(), cacheServer.getPort());
        cacheServerDetails.setHostName(cacheServer.getHostnameForClients());
        diskStoreDetails.add(cacheServerDetails);
      }
    }
  }

  protected String getDiskStoreName(final GatewaySender gateway) {
    return StringUtils.defaultIfBlank(gateway.getDiskStoreName(), DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
  }
  
  protected boolean isPersistent(final GatewaySender gateway) {
    return gateway.isPersistenceEnabled();
  }
  
  protected boolean isUsingDiskStore(final GatewaySender gateway, final DiskStore diskStore) {
    return ObjectUtils.equals(getDiskStoreName(gateway), diskStore.getName());
  }

  protected void setGatewayDetails(final InternalCache cache, final DiskStore diskStore, final DiskStoreDetails diskStoreDetails) {
    for (GatewaySender gatewaySender : cache.getGatewaySenders()) {
      if (isUsingDiskStore(gatewaySender, diskStore)) {
        DiskStoreDetails.GatewayDetails gatewayDetails = new DiskStoreDetails.GatewayDetails(gatewaySender.getId());
        gatewayDetails.setPersistent(isPersistent(gatewaySender));
        diskStoreDetails.add(gatewayDetails);
      }
    }
  }

  protected void setPdxSerializationDetails(final InternalCache cache, final DiskStore diskStore, final DiskStoreDetails diskStoreDetails) {
    if (cache.getPdxPersistent()) {
      String diskStoreName = StringUtils.defaultIfBlank(cache.getPdxDiskStore(), DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
      diskStoreDetails.setPdxSerializationMetaDataStored(ObjectUtils.equals(diskStoreName, diskStore.getName()));
    }
  }

  protected String getDiskStoreName(final AsyncEventQueue queue) {
    return StringUtils.defaultIfBlank(queue.getDiskStoreName(), DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
  }

  protected boolean isUsingDiskStore(final AsyncEventQueue queue, final DiskStore diskStore) {
    return (queue.isPersistent() && ObjectUtils.equals(getDiskStoreName(queue), diskStore.getName()));
  }

  protected void setAsyncEventQueueDetails(final InternalCache cache, final DiskStore diskStore, final DiskStoreDetails diskStoreDetails) {
    for (AsyncEventQueue queue : cache.getAsyncEventQueues()) {
      if (isUsingDiskStore(queue, diskStore)) {
        diskStoreDetails.add(new DiskStoreDetails.AsyncEventQueueDetails(queue.getId()));
      }
    }
  }

}
