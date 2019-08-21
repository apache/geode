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

package org.apache.geode.internal.cache;

import static org.apache.geode.internal.cache.PartitionedRegionHelper.MAX_PARTITIONED_REGION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class PartitionedRegionIntegrationTest {

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withNoCacheServer();

  @Test
  public void bucketSorterShutdownAfterRegionDestroy() {
    server.startServer();
    PartitionedRegion region =
        (PartitionedRegion) server.createRegion(RegionShortcut.PARTITION, "PR1",
            f -> f.setEvictionAttributes(
                EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.LOCAL_DESTROY)));

    ScheduledExecutorService bucketSorter = region.getBucketSorter();
    assertThat(bucketSorter).isNotNull();

    region.destroyRegion();

    assertThat(bucketSorter.isShutdown()).isTrue();
  }

  @Test
  public void bucketSorterIsNotCreatedIfNoEviction() {
    server.startServer();
    PartitionedRegion region =
        (PartitionedRegion) server.createRegion(RegionShortcut.PARTITION, "PR1",
            rf -> rf.setOffHeap(false));
    ScheduledExecutorService bucketSorter = region.getBucketSorter();
    assertThat(bucketSorter).isNull();
  }

  @Test
  // See GEODE-7106
  public void generatePRIdShouldNotThrowNumberFormatExceptionIfAnErrorOccursWhileReleasingTheLock()
      throws IOException {
    String methodName = testName.getMethodName();
    File customLogFile = temporaryFolder.newFile(methodName + ".log");
    DistributedLockService mockLockService = mock(DistributedLockService.class);
    doReturn(true).when(mockLockService).lock(any(), anyLong(), anyLong());
    doThrow(new RuntimeException("Mock Exception")).when(mockLockService).unlock(any());

    server.withProperty("log-level", "FINE")
        .withProperty("log-file", customLogFile.getAbsolutePath()).startServer();
    PartitionedRegion region =
        spy((PartitionedRegion) server.createRegion(RegionShortcut.PARTITION, methodName));
    doReturn(mockLockService).when(region).getPartitionedRegionLockService();

    assertThatCode(() -> region.generatePRId(server.getCache().getInternalDistributedSystem()))
        .doesNotThrowAnyException();
    assertThat(Files.lines(customLogFile.toPath())
        .anyMatch(line -> line.contains("java.lang.NumberFormatException"))).isFalse();
    assertThat(Files.lines(customLogFile.toPath())
        .anyMatch(line -> line.contains(
            "releasePRIDLock: unlocking " + MAX_PARTITIONED_REGION_ID + " caught an exception")))
                .isTrue();
  }
}
