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

package org.apache.geode.management.internal.configuration.converters;

import java.util.stream.Collectors;

import org.apache.geode.cache.configuration.DiskDirType;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.management.configuration.DiskDir;
import org.apache.geode.management.configuration.DiskStore;

public class DiskStoreConverter extends ConfigurationConverter<DiskStore, DiskStoreType> {
  @Override
  protected DiskStore fromNonNullXmlObject(DiskStoreType xmlObject) {
    DiskStore diskStore = new DiskStore();

    diskStore.setName(xmlObject.getName());
    if (xmlObject.isAllowForceCompaction() != null) {
      diskStore.setAllowForceCompaction(xmlObject.isAllowForceCompaction());
    }
    if (xmlObject.isAutoCompact() != null) {
      diskStore.setAutoCompact(xmlObject.isAutoCompact());
    }

    if (xmlObject.getCompactionThreshold() != null) {
      diskStore.setCompactionThreshold(Integer.parseInt(xmlObject.getCompactionThreshold()));
    }
    if (xmlObject.getDiskUsageCriticalPercentage() != null) {
      diskStore.setDiskUsageCriticalPercentage(
          Float.parseFloat(xmlObject.getDiskUsageCriticalPercentage()));
    }
    if (xmlObject.getDiskUsageWarningPercentage() != null) {
      diskStore
          .setDiskUsageWarningPercentage(
              Float.parseFloat(xmlObject.getDiskUsageWarningPercentage()));
    }
    if (xmlObject.getMaxOplogSize() != null) {
      diskStore.setMaxOplogSizeInBytes(Long.parseLong(xmlObject.getMaxOplogSize()));
    }

    if (xmlObject.getQueueSize() != null) {
      diskStore.setQueueSize(Integer.parseInt(xmlObject.getQueueSize()));
    }
    if (xmlObject.getTimeInterval() != null) {
      diskStore.setTimeInterval(Long.parseLong(xmlObject.getTimeInterval()));
    }
    if (xmlObject.getWriteBufferSize() != null) {
      diskStore.setWriteBufferSize(Integer.parseInt(xmlObject.getWriteBufferSize()));
    }


    diskStore.setDirectories(xmlObject.getDiskDirs().stream().map(diskDirType -> {
      DiskDir diskDir = new DiskDir();
      if (diskDirType.getDirSize() != null) {
        diskDir.setDirSize(Integer.parseInt(diskDirType.getDirSize()));
      }
      diskDir.setName(diskDirType.getContent());
      return diskDir;
    }).collect(Collectors.toList()));

    return diskStore;
  }

  @Override
  protected DiskStoreType fromNonNullConfigObject(DiskStore configObject) {
    DiskStoreType diskStoreType = new DiskStoreType();

    diskStoreType.setName(configObject.getName());
    if (configObject.isAllowForceCompaction() != null) {
      diskStoreType.setAllowForceCompaction(configObject.isAllowForceCompaction());
    }
    if (configObject.isAutoCompact() != null) {
      diskStoreType.setAutoCompact(configObject.isAutoCompact());
    }
    if (configObject.getCompactionThreshold() != null) {
      diskStoreType.setCompactionThreshold(configObject.getCompactionThreshold().toString());
    }
    if (configObject.getDiskUsageCriticalPercentage() != null) {
      diskStoreType
          .setDiskUsageCriticalPercentage(configObject.getDiskUsageCriticalPercentage().toString());
    }
    if (configObject.getDiskUsageWarningPercentage() != null) {
      diskStoreType
          .setDiskUsageWarningPercentage(configObject.getDiskUsageWarningPercentage().toString());
    }
    if (configObject.getMaxOplogSizeInBytes() != null) {
      diskStoreType.setMaxOplogSize(configObject.getMaxOplogSizeInBytes().toString());
    }
    if (configObject.getQueueSize() != null) {
      diskStoreType.setQueueSize(configObject.getQueueSize().toString());
    }
    if (configObject.getTimeInterval() != null) {
      diskStoreType.setTimeInterval(configObject.getTimeInterval().toString());
    }
    if (configObject.getWriteBufferSize() != null) {
      diskStoreType.setWriteBufferSize(configObject.getWriteBufferSize().toString());
    }
    diskStoreType.setDiskDirs(configObject.getDirectories().stream().map(diskDir -> {
      DiskDirType diskDirType = new DiskDirType();
      diskDirType.setContent(diskDir.getName());
      if (diskDir.getDirSize() != null) {
        diskDirType.setDirSize(diskDir.getDirSize().toString());
      }
      return diskDirType;
    }).collect(Collectors.toList()));

    return diskStoreType;
  }
}
