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

package org.apache.geode.management.internal.configuration.validators;

import static org.apache.geode.internal.cache.DiskStoreAttributes.checkMinOplogSize;
import static org.apache.geode.internal.cache.DiskStoreAttributes.checkQueueSize;
import static org.apache.geode.internal.cache.DiskStoreAttributes.checkWriteBufferSize;
import static org.apache.geode.internal.cache.DiskStoreAttributes.verifyNonNegativeDirSize;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.internal.cache.DiskStoreMonitor;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.DiskStore;
import org.apache.geode.management.internal.CacheElementOperation;

public class DiskStoreValidator implements ConfigurationValidator<DiskStore> {
  @Override
  public void validate(CacheElementOperation operation, DiskStore config)
      throws IllegalArgumentException {
    switch (operation) {
      case CREATE:
      case UPDATE:
        checkRequiredItems(config);
        checkValueRanges(config);
        break;
      case DELETE:
        validateDelete(config);
    }
  }

  private void validateDelete(AbstractConfiguration config) {
    if (StringUtils.isNotBlank(config.getGroup())) {
      throw new IllegalArgumentException(
          "Group is an invalid option when deleting disk store.");
    }
  }

  private void checkValueRanges(DiskStore config) {
    if (config.getDiskUsageCriticalPercentage() != null) {
      DiskStoreMonitor.checkCritical(config.getDiskUsageCriticalPercentage());
    }
    if (config.getDiskUsageWarningPercentage() != null) {
      DiskStoreMonitor.checkWarning(config.getDiskUsageWarningPercentage());
    }
    if (config.getCompactionThreshold() != null) {
      if (0 > config.getCompactionThreshold() || config.getCompactionThreshold() > 100) {
        throw new IllegalArgumentException(
            "CompactionThreshold has to be set to a value between 0-100.");
      }
    }
    if (config.getMaxOplogSizeInBytes() != null) {
      checkMinOplogSize(config.getMaxOplogSizeInBytes());
    }
    if (config.getQueueSize() != null) {
      checkQueueSize(config.getQueueSize());
    }
    if (config.getWriteBufferSize() != null) {
      checkWriteBufferSize(config.getWriteBufferSize());
    }
    verifyNonNegativeDirSize(config.getDirectories().stream().mapToInt(diskDir -> {
      if (diskDir.getDirSize() != null) {
        return diskDir.getDirSize();
      } else {
        return Integer.MAX_VALUE;
      }
    }).toArray());
  }

  private void checkRequiredItems(DiskStore config) {
    if (StringUtils.isEmpty(config.getName())) {
      throw new IllegalArgumentException("Diskstore name is required.");
    }
    if (config.getDirectories() != null && config.getDirectories().size() > 0) {
      if (config.getDirectories()
          .stream()
          .anyMatch(diskDir -> StringUtils.isEmpty(diskDir.getName()))) {
        throw new IllegalArgumentException("Diskdir name is required.");
      }
    } else {
      throw new IllegalArgumentException("At least one DiskDir element required.");
    }
  }
}
