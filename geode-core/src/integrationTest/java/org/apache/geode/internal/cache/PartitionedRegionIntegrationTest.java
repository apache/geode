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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ScheduledExecutorService;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class PartitionedRegionIntegrationTest {

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withNoCacheServer().withAutoStart();

  @Test
  public void bucketSorterShutdownAfterRegionDestroy() {
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
    PartitionedRegion region =
        (PartitionedRegion) server.createRegion(RegionShortcut.PARTITION, "PR1",
            rf -> rf.setOffHeap(false));
    ScheduledExecutorService bucketSorter = region.getBucketSorter();
    assertThat(bucketSorter).isNull();
  }
}
