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

package org.apache.geode.management.internal.cli.functions;

import java.io.File;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.util.ArrayUtils;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;

/**
 * The DescribeDiskStoreFunction class is an implementation of a GemFire Function used to collect
 * information and details about a particular disk store for a particular GemFire distributed system
 * member.
 *
 * @see org.apache.geode.cache.DiskStore
 * @see org.apache.geode.cache.execute.Function
 * @see org.apache.geode.cache.execute.FunctionContext
 * @see org.apache.geode.internal.InternalEntity
 * @see org.apache.geode.management.internal.cli.domain.DiskStoreDetails
 * @since GemFire 7.0
 */
public class DescribeDiskStoreFunction implements InternalFunction<String> {

  private static final Logger logger = LogService.getLogger();

  protected static void assertState(final boolean condition, final String message,
      final Object... args) {
    if (!condition) {
      throw new IllegalStateException(String.format(message, args));
    }
  }

  @Override
  public String getId() {
    return getClass().getName();
  }

  @SuppressWarnings("unused")
  public void init(final Properties props) {}

  @Override
  public void execute(final FunctionContext<String> context) {
    Cache cache = context.getCache();

    try {
      if (cache instanceof InternalCache) {
        InternalCache gemfireCache = (InternalCache) cache;

        DistributedMember member = gemfireCache.getMyId();

        String diskStoreName = context.getArguments();
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
          diskStoreDetails
              .setDiskUsageCriticalPercentage(diskStore.getDiskUsageCriticalPercentage());

          setDiskDirDetails(diskStore, diskStoreDetails);
          setRegionDetails(gemfireCache, diskStore, diskStoreDetails);
          setCacheServerDetails(gemfireCache, diskStore, diskStoreDetails);
          setGatewayDetails(gemfireCache, diskStore, diskStoreDetails);
          setPdxSerializationDetails(gemfireCache, diskStore, diskStoreDetails);
          setAsyncEventQueueDetails(gemfireCache, diskStore, diskStoreDetails);

          context.getResultSender().lastResult(diskStoreDetails);
        } else {
          context.getResultSender()
              .sendException(new EntityNotFoundException(
                  String.format("A disk store with name '%1$s' was not found on member '%2$s'.",
                      diskStoreName, memberName)));
        }
      }
    } catch (Exception e) {
      logger.error("Error occurred while executing 'describe disk-store': {}!", e.getMessage(), e);
      context.getResultSender().sendException(e);
    }
  }

  private void setDiskDirDetails(final DiskStore diskStore,
      final DiskStoreDetails diskStoreDetails) {
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

  protected String getDiskStoreName(final Region<?, ?> region) {
    return StringUtils.defaultIfBlank(region.getAttributes().getDiskStoreName(),
        DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
  }

  protected boolean isOverflowToDisk(final Region<?, ?> region) {
    return (region.getAttributes().getEvictionAttributes() != null
        && EvictionAction.OVERFLOW_TO_DISK
            .equals(region.getAttributes().getEvictionAttributes().getAction()));
  }

  protected boolean isPersistent(final Region<?, ?> region) {
    return region.getAttributes().getDataPolicy().withPersistence();
  }

  protected boolean isUsingDiskStore(final Region<?, ?> region, final DiskStore diskStore) {
    return ((isPersistent(region) || isOverflowToDisk(region))
        && ObjectUtils.equals(getDiskStoreName(region), diskStore.getName()));
  }

  protected void setRegionDetails(final InternalCache cache, final DiskStore diskStore,
      final DiskStoreDetails diskStoreDetails) {
    for (Region<?, ?> region : cache.rootRegions()) {
      setRegionDetails(region, diskStore, diskStoreDetails);
    }
  }

  private void setRegionDetails(final Region<?, ?> region, final DiskStore diskStore,
      final DiskStoreDetails diskStoreDetails) {
    if (isUsingDiskStore(region, diskStore)) {
      String regionFullPath = region.getFullPath();
      DiskStoreDetails.RegionDetails regionDetails = new DiskStoreDetails.RegionDetails(
          regionFullPath, StringUtils.defaultIfBlank(region.getName(), regionFullPath));
      regionDetails.setOverflowToDisk(isOverflowToDisk(region));
      regionDetails.setPersistent(isPersistent(region));
      diskStoreDetails.add(regionDetails);
    }

    for (Region<?, ?> subregion : region.subregions(false)) {
      setRegionDetails(subregion, diskStore, diskStoreDetails); // depth-first, recursive strategy
    }
  }

  protected String getDiskStoreName(final CacheServer cacheServer) {
    return (cacheServer.getClientSubscriptionConfig() == null ? null
        : StringUtils.defaultIfBlank(cacheServer.getClientSubscriptionConfig().getDiskStoreName(),
            DiskStoreDetails.DEFAULT_DISK_STORE_NAME));
  }

  protected boolean isUsingDiskStore(final CacheServer cacheServer, final DiskStore diskStore) {
    return ObjectUtils.equals(getDiskStoreName(cacheServer), diskStore.getName());
  }

  protected void setCacheServerDetails(final InternalCache cache, final DiskStore diskStore,
      final DiskStoreDetails diskStoreDetails) {
    for (CacheServer cacheServer : cache.getCacheServers()) {
      if (isUsingDiskStore(cacheServer, diskStore)) {
        DiskStoreDetails.CacheServerDetails cacheServerDetails =
            new DiskStoreDetails.CacheServerDetails(cacheServer.getBindAddress(),
                cacheServer.getPort());
        cacheServerDetails.setHostName(cacheServer.getHostnameForClients());
        diskStoreDetails.add(cacheServerDetails);
      }
    }
  }

  protected String getDiskStoreName(final GatewaySender gateway) {
    return StringUtils.defaultIfBlank(gateway.getDiskStoreName(),
        DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
  }

  protected boolean isPersistent(final GatewaySender gateway) {
    return gateway.isPersistenceEnabled();
  }

  protected boolean isUsingDiskStore(final GatewaySender gateway, final DiskStore diskStore) {
    return ObjectUtils.equals(getDiskStoreName(gateway), diskStore.getName());
  }

  protected void setGatewayDetails(final InternalCache cache, final DiskStore diskStore,
      final DiskStoreDetails diskStoreDetails) {
    for (GatewaySender gatewaySender : cache.getGatewaySenders()) {
      if (isUsingDiskStore(gatewaySender, diskStore)) {
        DiskStoreDetails.GatewayDetails gatewayDetails =
            new DiskStoreDetails.GatewayDetails(gatewaySender.getId());
        gatewayDetails.setPersistent(isPersistent(gatewaySender));
        diskStoreDetails.add(gatewayDetails);
      }
    }
  }

  protected void setPdxSerializationDetails(final InternalCache cache, final DiskStore diskStore,
      final DiskStoreDetails diskStoreDetails) {
    if (cache.getPdxPersistent()) {
      String diskStoreName = StringUtils.defaultIfBlank(cache.getPdxDiskStore(),
          DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
      diskStoreDetails.setPdxSerializationMetaDataStored(
          ObjectUtils.equals(diskStoreName, diskStore.getName()));
    }
  }

  protected String getDiskStoreName(final AsyncEventQueue queue) {
    return StringUtils.defaultIfBlank(queue.getDiskStoreName(),
        DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
  }

  protected boolean isUsingDiskStore(final AsyncEventQueue queue, final DiskStore diskStore) {
    return (queue.isPersistent()
        && ObjectUtils.equals(getDiskStoreName(queue), diskStore.getName()));
  }

  protected void setAsyncEventQueueDetails(final InternalCache cache, final DiskStore diskStore,
      final DiskStoreDetails diskStoreDetails) {
    for (AsyncEventQueue queue : cache.getAsyncEventQueues()) {
      if (isUsingDiskStore(queue, diskStore)) {
        diskStoreDetails.add(new DiskStoreDetails.AsyncEventQueueDetails(queue.getId()));
      }
    }
  }

}
