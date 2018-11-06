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

import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.cache.SearchLoadAndWriteProcessor.NetSearchRequestMessage;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * The Clear operation during a NetSearchMessage.doGet() in progress can cause DiskAccessException
 * by accessing cleared oplogs and eventually destroy region. The Test verifies that fix prevents
 * this.
 *
 * <p>
 * Test must be DistributedTest because it requires ClusterDistributionManager.
 *
 * <p>
 * TRAC #40299: Suspect String - DiskAccessException : Data for DiskEntry could not be obtained
 * from Disk. A clear operation may have deleted the oplogs (logged as error)
 */

public class ClearDuringNetSearchOplogRegressionTest extends CacheTestCase {

  private String uniqueName;
  private String regionName;
  private File[] diskDirs;
  private transient CacheObserver observer;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName;

    diskDirs = new File[] {temporaryFolder.newFolder(uniqueName)};

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    observer = spy(CacheObserver.class);

    addIgnoredException("Entry has been cleared and is not present on disk");
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * The Clear operation during a NetSearchMessage.doGet() in progress can cause DiskAccessException
   * by accessing cleared oplogs and eventually destroy region. The Test verifies that fix prevents
   * this.
   */
  @Test
  public void testQueryGetWithClear() throws Exception {
    // create region
    createCacheForVM0();

    // Do puts to region
    putSevenEntries();

    // call NetSearchMessage.doGet() after region.clear()
    concurrentNetSearchGetAndClear();

    // verify that region is not destroyed
    verifyRegionNotDestroyed();
  }

  private void createCacheForVM0() {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirsAndSizes(diskDirs, new int[] {Integer.MAX_VALUE});
    diskStoreFactory.setQueueSize(1);
    diskStoreFactory.setMaxOplogSize(60); // does the test want 60 bytes or 60M?
    diskStoreFactory.setAutoCompact(false).setTimeInterval(1000);

    DiskStore diskStore = diskStoreFactory.create(uniqueName);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(diskStore.getName());
    factory.setEvictionAttributes(createLRUEntryAttributes(2, EvictionAction.OVERFLOW_TO_DISK));

    getCache().createRegion(regionName, factory.create());
  }

  private void putSevenEntries() {
    Region<String, Integer> region = getCache().getRegion(regionName);
    for (int i = 0; i < 7; i++) {
      region.put("key" + i, i);
    }
  }

  private void concurrentNetSearchGetAndClear() throws InterruptedException {
    InternalRegion region = (InternalRegion) getCache().getRegion(regionName);
    assertThat(region.size()).isEqualTo(7);

    Thread getter = new Thread(new Getter(region));

    region.getDiskRegion().acquireWriteLock();
    try {
      doConcurrentNetSearchGetAndClear(getter, region);
    } finally {
      region.getDiskRegion().releaseWriteLock();
    }

    // allow getThread to join to set getAfterClearSuccessful
    getter.join();
  }

  private void doConcurrentNetSearchGetAndClear(Thread getter, InternalRegion region) {
    CacheObserverHolder.setInstance(observer);

    // start getThread
    getter.start();

    await()
        .untilAsserted(() -> verify(observer, times(1)).afterSettingDiskRef());

    // This test appears to be testing a problem with the non-RVV
    // based clear. So we'll use that functionality here.
    // Region.clear uses an RVV, and will deadlock if called while
    // the write lock is held.
    RegionEventImpl regionEvent = new RegionEventImpl(region, Operation.REGION_CLEAR, null, false,
        region.getMyId(), region.generateEventID());

    // clearRegion to remove entry that getter has reference of
    ((LocalRegion) region).cmnClearRegion(regionEvent, true, false);
  }

  private void verifyRegionNotDestroyed() {
    Region region = getCache().getRegion(regionName);
    assertThat(region).isNotNull();
    assertThat(region.isDestroyed()).isFalse();
  }

  private static class Getter implements Runnable {

    private final InternalRegion region;

    Getter(InternalRegion region) {
      super();
      this.region = region;
    }

    @Override
    public void run() {
      SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
      processor.initialize((LocalRegion) region, "key1", null);
      sendNetSearchRequestMessage(processor, "key1", 1500, 1500, 1500);
    }

    private void sendNetSearchRequestMessage(SearchLoadAndWriteProcessor processor, Object key,
        int timeoutMillis, int ttlMillis, int idleMillis) {
      NetSearchRequestMessage message = new SearchLoadAndWriteProcessor.NetSearchRequestMessage();
      message.initialize(processor, region.getName(), key, timeoutMillis, ttlMillis, idleMillis);
      message.doGet((ClusterDistributionManager) region.getDistributionManager());
    }
  }
}
