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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.test.junit.categories.RegionsTest;

@Category(RegionsTest.class)
public class PersistentColocatedPartitionedRegionIntegrationTest {

  private Cache cache;
  private String diskStoreName;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    cache = new CacheFactory().set(LOCATORS, "").create();

    Path diskDirPath = temporaryFolder.getRoot().toPath();
    File[] diskDirs = new File[] {diskDirPath.toFile()};

    diskStoreName = "disk";
    cache.createDiskStoreFactory().setDiskDirs(diskDirs).create(diskStoreName);

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(0);
    RegionFactory regionFactory = cache.createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    regionFactory.create("persistentLeader");

    partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(0);
    regionFactory = cache.createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create("nonPersistentLeader");
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void persistentPR_cannotColocateWith_nonPersistentPR() {
    // Try to colocate a persistent PR with the non persistent PR. This should fail.
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setColocatedWith("nonPersistentLeader");
    partitionAttributesFactory.setRedundantCopies(0);

    RegionFactory regionFactory = cache.createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    Throwable thrown = catchThrowable(() -> regionFactory.create("colocated"));
    assertThat(thrown).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void persistentPR_canColocateWith_persistentPR() {
    // Try to colocate a persistent PR with another persistent PR. This should work.
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setColocatedWith("persistentLeader");
    partitionAttributesFactory.setRedundantCopies(0);

    RegionFactory regionFactory = cache.createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    assertThatCode(() -> regionFactory.create("colocated")).doesNotThrowAnyException();
  }

  @Test
  public void nonPersistentPR_canColocateWith_persistentPR() {
    // We should also be able to colocate a non persistent region with a persistent region.
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setColocatedWith("persistentLeader");
    partitionAttributesFactory.setRedundantCopies(0);

    RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    assertThatCode(() -> regionFactory.create("colocated")).doesNotThrowAnyException();
  }
}
