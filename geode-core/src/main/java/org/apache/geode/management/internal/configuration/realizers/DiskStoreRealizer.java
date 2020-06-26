/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.management.internal.configuration.realizers;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.DiskStore;
import org.apache.geode.management.runtime.DiskStoreInfo;

public class DiskStoreRealizer implements ConfigurationRealizer<DiskStore, DiskStoreInfo> {
  @Override
  public RealizationResult create(DiskStore config, InternalCache cache) throws Exception {
    DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();

    if (config.isAllowForceCompaction() != null) {
      diskStoreAttributes.allowForceCompaction = config.isAllowForceCompaction();
    }
    if (config.isAutoCompact() != null) {
      diskStoreAttributes.autoCompact = config.isAutoCompact();
    }
    if (config.getCompactionThreshold() != null) {
      diskStoreAttributes.compactionThreshold = config.getCompactionThreshold();
    }
    if (config.getDiskUsageCriticalPercentage() != null) {
      diskStoreAttributes.setDiskUsageCriticalPercentage(config.getDiskUsageCriticalPercentage());
    }
    if (config.getDiskUsageWarningPercentage() != null) {
      diskStoreAttributes.setDiskUsageWarningPercentage(config.getDiskUsageWarningPercentage());
    }
    if (config.getMaxOplogSizeInBytes() != null) {
      diskStoreAttributes.maxOplogSizeInBytes = config.getMaxOplogSizeInBytes();
    }
    if (config.getQueueSize() != null) {
      diskStoreAttributes.queueSize = config.getQueueSize();
    }
    if (config.getTimeInterval() != null) {
      diskStoreAttributes.timeInterval = config.getTimeInterval();
    }
    if (config.getWriteBufferSize() != null) {
      diskStoreAttributes.writeBufferSize = config.getWriteBufferSize();
    }

    List<File> fileList =
        config.getDirectories().stream().map(diskDir -> new File(diskDir.getName()))
            .collect(Collectors.toList());
    diskStoreAttributes.diskDirs = fileList.toArray(diskStoreAttributes.diskDirs);
    diskStoreAttributes.diskDirSizes = config.getDirectories().stream().mapToInt(diskDir -> {
      if (diskDir.getDirSize() != null) {
        return diskDir.getDirSize();
      } else {
        return Integer.MAX_VALUE;
      }
    }).toArray();

    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory(diskStoreAttributes);
    diskStoreFactory.create(config.getName());

    return new RealizationResult()
        .setMessage("DiskStore " + config.getName() + " created successfully.");
  }

  @Override
  public boolean exists(DiskStore config, InternalCache cache) {
    return cache.listDiskStores().stream()
        .anyMatch(diskStore -> diskStore.getName().equals(config.getName()));
  }

  @Override
  public DiskStoreInfo get(DiskStore config, InternalCache cache) {
    return new DiskStoreInfo();
  }

  @Override
  public RealizationResult update(DiskStore config, InternalCache cache) throws Exception {
    return null;
  }

  @Override
  public RealizationResult delete(DiskStore config, InternalCache cache) throws Exception {
    org.apache.geode.cache.DiskStore diskStore = cache.findDiskStore(config.getName());
    if (diskStore != null) {
      diskStore.destroy();
      return new RealizationResult()
          .setMessage("DiskStore " + config.getName() + " deleted successfully.");
    } else {
      return new RealizationResult().setMessage("DiskStore " + config.getName() + " not found.")
          .setSuccess(false);
    }
  }

  @Override
  public boolean isReadyOnly() {
    return false;
  }
}
