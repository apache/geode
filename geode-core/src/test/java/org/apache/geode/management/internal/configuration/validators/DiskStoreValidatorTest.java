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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.configuration.DiskDir;
import org.apache.geode.management.configuration.DiskStore;
import org.apache.geode.management.internal.CacheElementOperation;

public class DiskStoreValidatorTest {
  private final DiskStoreValidator diskStoreValidator = new DiskStoreValidator();
  private DiskStore diskStore;
  private DiskDir diskDir;

  @Before
  public void init() {
    diskStore = new DiskStore();
    String storeName = "diskstore";
    diskStore.setName(storeName);
    diskDir = new DiskDir();
    String dirName = "diskdir";
    diskDir.setName(dirName);
    int dirSizeInteger = 1024;
    diskDir.setDirSize(dirSizeInteger);
    diskStore.setDirectories(Collections.singletonList(diskDir));
  }

  @Test
  public void diskStoreNameIsRequired() {
    diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore);

    diskStore.setName(null);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Diskstore name is required");
  }

  @Test
  public void atLeastOneDirectoryIsDefined() {
    diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore);

    diskStore.setDirectories(null);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("At least one DiskDir element required");
  }

  @Test
  public void diskUsageCriticalPercentageMustBeBetweenZeroAndOneHundred() {
    diskStore.setDiskUsageCriticalPercentage(95F);
    diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore);

    diskStore.setDiskUsageCriticalPercentage(-1F);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Disk usage critical percentage must be set to a value between 0-100");

    diskStore.setDiskUsageCriticalPercentage(101F);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Disk usage critical percentage must be set to a value between 0-100");
  }

  @Test
  public void diskUsageWarningPercentageMustBeBetweenZeroAndOneHundred() {
    diskStore.setDiskUsageWarningPercentage(95F);
    diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore);

    diskStore.setDiskUsageWarningPercentage(-1F);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Disk usage warning percentage must be set to a value between 0-100");

    diskStore.setDiskUsageWarningPercentage(101F);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Disk usage warning percentage must be set to a value between 0-100");
  }

  @Test
  public void compactionThresholdMustBeBetweenZeroAndOneHundres() {
    diskStore.setCompactionThreshold(95);
    diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore);

    diskStore.setCompactionThreshold(-1);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("CompactionThreshold has to be set to a value between 0-100.");

    diskStore.setCompactionThreshold(101);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("CompactionThreshold has to be set to a value between 0-100.");
  }

  @Test
  public void maxOplogSizeInBytesMustBePositiveNumber() {
    diskStore.setMaxOplogSizeInBytes(Long.MAX_VALUE);
    diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore);

    diskStore.setMaxOplogSizeInBytes(-1L);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Maximum Oplog size specified has to be a non-negative number and the value given");
  }

  @Test
  public void queueSizeMustBePositiveNumber() {
    diskStore.setQueueSize(Integer.MAX_VALUE);
    diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore);

    diskStore.setQueueSize(-1);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Queue size specified has to be a non-negative number and the value given");
  }

  @Test
  public void writeBufferSizeMustBePositiveNumber() {
    diskStore.setWriteBufferSize(Integer.MAX_VALUE);
    diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore);

    diskStore.setWriteBufferSize(-1);
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Write buffer size specified has to be a non-negative number and the value given");
  }

  @Test
  public void dirSizesMustBePositiveNumber() {
    diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore);

    diskDir.setDirSize(-1);
    diskStore.setDirectories(Collections.singletonList(diskDir));
    assertThatThrownBy(() -> diskStoreValidator.validate(CacheElementOperation.CREATE, diskStore))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Dir size cannot be negative :");
  }
}
