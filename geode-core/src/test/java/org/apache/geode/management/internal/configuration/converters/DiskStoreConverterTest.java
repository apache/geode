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

package org.apache.geode.management.internal.configuration.converters;

import static org.assertj.core.api.SoftAssertions.assertSoftly;

import java.util.ArrayList;

import org.junit.Test;

import org.apache.geode.cache.configuration.DiskDirType;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.management.configuration.DiskDir;
import org.apache.geode.management.configuration.DiskStore;

public class DiskStoreConverterTest {
  private final DiskStoreConverter diskStoreConverter = new DiskStoreConverter();

  @Test
  public void fromNonNullConfigObjectCopiesPropertiesCorrectly() {
    DiskStore config = new DiskStore();
    config.setName("name");
    config.setAllowForceCompaction(false);
    config.setAutoCompact(false);
    config.setCompactionThreshold(50);
    config.setDiskUsageCriticalPercentage(80F);
    config.setDiskUsageWarningPercentage(70F);
    config.setMaxOplogSizeInBytes(10L);
    config.setQueueSize(5);
    config.setTimeInterval(1L);
    config.setWriteBufferSize(1);

    ArrayList<DiskDir> directories = new ArrayList<>();
    directories.add(new DiskDir("directoryName", 1));
    config.setDirectories(directories);

    DiskStoreType diskStoreType = diskStoreConverter.fromNonNullConfigObject(config);

    assertSoftly(softly -> {
      softly.assertThat(diskStoreType.isAllowForceCompaction())
          .isEqualTo(config.isAllowForceCompaction());
      softly.assertThat(diskStoreType.isAutoCompact())
          .isEqualTo(config.isAutoCompact());
      softly.assertThat(diskStoreType.getCompactionThreshold())
          .isEqualTo(config.getCompactionThreshold().toString());
      softly.assertThat(diskStoreType.getDiskUsageCriticalPercentage())
          .isEqualTo(config.getDiskUsageCriticalPercentage().toString());
      softly.assertThat(diskStoreType.getDiskUsageWarningPercentage())
          .isEqualTo(config.getDiskUsageWarningPercentage().toString());
      softly.assertThat(diskStoreType.getMaxOplogSize())
          .isEqualTo(config.getMaxOplogSizeInBytes().toString());
      softly.assertThat(diskStoreType.getQueueSize())
          .isEqualTo(config.getQueueSize().toString());
      softly.assertThat(diskStoreType.getTimeInterval())
          .isEqualTo(config.getTimeInterval().toString());
      softly.assertThat(diskStoreType.getWriteBufferSize())
          .isEqualTo(config.getWriteBufferSize().toString());
      softly.assertThat(diskStoreType.getDiskDirs().size())
          .isEqualTo(config.getDirectories().size());

    });
  }

  @Test
  public void fromNonNullXmlObjectCopiesPropertiesCorrectly() {
    DiskStoreType diskStoreType = new DiskStoreType();
    diskStoreType.setName("name");
    diskStoreType.setAllowForceCompaction(false);
    diskStoreType.setAutoCompact(false);
    diskStoreType.setCompactionThreshold("50");
    diskStoreType.setDiskUsageCriticalPercentage("80");
    diskStoreType.setDiskUsageWarningPercentage("70");
    diskStoreType.setMaxOplogSize("10");
    diskStoreType.setQueueSize("5");
    diskStoreType.setTimeInterval("1");
    diskStoreType.setWriteBufferSize("1");

    ArrayList<DiskDirType> diskDirs = new ArrayList<>();
    diskDirs.add(new DiskDirType());
    diskStoreType.setDiskDirs(diskDirs);

    DiskStore config = diskStoreConverter.fromNonNullXmlObject(diskStoreType);

    assertSoftly(softly -> {
      softly.assertThat(config.isAllowForceCompaction())
          .isEqualTo(diskStoreType.isAllowForceCompaction());
      softly.assertThat(config.isAutoCompact())
          .isEqualTo(diskStoreType.isAutoCompact());
      softly.assertThat(config.getCompactionThreshold().toString())
          .isEqualTo(diskStoreType.getCompactionThreshold());
      softly.assertThat(config.getDiskUsageCriticalPercentage().toString())
          .contains(diskStoreType.getDiskUsageCriticalPercentage());
      softly.assertThat(config.getDiskUsageWarningPercentage().toString())
          .contains(diskStoreType.getDiskUsageWarningPercentage());
      softly.assertThat(config.getMaxOplogSizeInBytes().toString())
          .isEqualTo(diskStoreType.getMaxOplogSize());
      softly.assertThat(config.getQueueSize().toString())
          .isEqualTo(diskStoreType.getQueueSize());
      softly.assertThat(config.getTimeInterval().toString())
          .isEqualTo(diskStoreType.getTimeInterval());
      softly.assertThat(config.getWriteBufferSize().toString())
          .isEqualTo(diskStoreType.getWriteBufferSize());
      softly.assertThat(config.getDirectories().size())
          .isEqualTo(diskStoreType.getDiskDirs().size());
    });
  }
}
