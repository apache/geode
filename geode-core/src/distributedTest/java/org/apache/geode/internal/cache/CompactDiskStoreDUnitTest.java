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

import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class CompactDiskStoreDUnitTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

//  @Rule
//  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  private static final String DISK_STORE_NAME = "DEFAULT";

  private static final String REGION_NAME = "testRegion";

  private static final int NUM_ENTRIES = 1000;

  private File serverDir;

  private File diskStoreDir;

  @Before
  public void setup() throws IOException {
//    serverDir = temporaryFolder.newFolder();
//    diskStoreDir = temporaryFolder.newFolder();
    diskStoreDir = new File("/Users/jchen/workspace/geode/geode-core/build/distributedTest/test-worker-000001/dunit/vm1");
  }

  @Test
  public void testDuplicateDiskStoreCompaction() throws Exception {

    MemberVM locator = clusterStartupRule.startLocatorVM(0);

    MemberVM server = clusterStartupRule.startServerVM(1, locator.getPort());

//    server.invoke(() -> createDiskStore());

    server.invoke(() -> createRegion());

    server.invoke(() -> populateRegion());

    server.stop(false);

    compactOfflineDiskStore();

    clusterStartupRule.startServerVM(1, locator.getPort());

  }

  private void createDiskStore() {
    getCache().createDiskStoreFactory().setAutoCompact(true).setAllowForceCompaction(true).setDiskDirs(new File[]{diskStoreDir}).create(DISK_STORE_NAME);
  }

  private void createRegion() {
    getCache().createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).create(REGION_NAME);
  }

  private void populateRegion() {
    Region region = getCache().getRegion(REGION_NAME);
    IntStream.range(0, NUM_ENTRIES).forEach(i -> region.put(i, i));
    IntStream.range(0, NUM_ENTRIES).forEach(i -> region.put(i, i + 1));
  }

  private void closeCache() {
    getCache().close();
  }

  private void compactOfflineDiskStore() throws Exception {
    DiskStoreImpl.offlineCompact(DISK_STORE_NAME, new File[]{diskStoreDir}, false/* upgrade */, -1);
  }

  private void restartCache() {
    getCache().initialize();
  }

}
