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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.StampedLock;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.DistributionConfig;

/**
 * junit test for detecting read conflicts
 */
public class TXDetectReadConflictJUnitTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName name = new TestName();

  protected Cache cache = null;
  protected Region region = null;
  protected Region regionpr = null;


  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "detectReadConflicts", "true");
    createCache();
  }

  protected void createCache() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(LOCATORS, "");
    cache = new CacheFactory(props).create();
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("testRegionRR");
  }

  protected void createCachePR() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(LOCATORS, "");
    cache = new CacheFactory(props).create();
    regionpr = cache.createRegionFactory(RegionShortcut.PARTITION).create("testRegionPR");
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  @Test
  public void testReadConflictsRR() throws Exception {
    cache.close();
    createCache();
    region.put("key", "value");
    region.put("key1", "value1");
    TXManagerImpl mgr = (TXManagerImpl) cache.getCacheTransactionManager();
    mgr.begin();
    assertEquals("value", region.get("key"));
    assertEquals("value1", region.get("key1"));
    mgr.commit();
  }

  @Test
  public void testReadConflictsPR() throws Exception {
    cache.close();
    createCachePR();
    regionpr.put("key", "value");
    regionpr.put("key1", "value1");
    TXManagerImpl mgr = (TXManagerImpl) cache.getCacheTransactionManager();
    mgr.begin();
    assertEquals("value", regionpr.get("key"));
    assertEquals("value1", regionpr.get("key1"));
    mgr.commit();
  }


  /**
   * Test that two transactions with only read operations don't produce CommitConflictException
   * This test fill some initial value on startup and after that create different threads with
   * region#get operations
   * Sync two different threads commit time through lock
   */
  @Test(/* no exception expected */)
  public void testDetectReadConflict()
      throws CacheException, ExecutionException, InterruptedException {
    cache.close();
    createCache();
    TXManagerImpl txMgrImpl = (TXManagerImpl) cache.getCacheTransactionManager();
    // fill some initial key-value on start
    txMgrImpl.begin();
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.commit();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    for (int i = 0; i < 20; i++) {
      final StampedLock lock = new StampedLock();
      lock.asWriteLock().lock();
      //
      Future<?> future1 = executor.submit(() -> {
        CacheTransactionManager txMgr2 = cache.getCacheTransactionManager();
        txMgr2.begin();
        region.get("key1"); // non-tx
        lock.asReadLock().lock();
        txMgr2.commit();
      });
      Future<?> future2 = executor.submit(() -> {
        CacheTransactionManager txMgr3 = cache.getCacheTransactionManager();
        txMgr3.begin();
        region.get("key1"); // non-tx
        lock.asReadLock().lock();
        txMgr3.commit();
      });
      pause(100);
      lock.asWriteLock().unlock();
      future1.get();
      future2.get();
    }
  }

  /**
   * Test that four transactions with only read operations don't produce CommitConflictException
   * This test fill some initial value on startup and after that create different threads with
   * region#get operations
   * Sync two different threads commit time through lock
   */
  @Test(/* no exception expected */)
  public void testDetectReadConflict4RPR()
      throws CacheException, ExecutionException, InterruptedException {
    cache.close();
    createCachePR();
    TXManagerImpl txMgrImpl = (TXManagerImpl) cache.getCacheTransactionManager();
    // fill some initial key-value on start
    txMgrImpl.begin();
    this.regionpr.put("key1", "value1"); // non-tx
    txMgrImpl.commit();
    ExecutorService executor = Executors.newFixedThreadPool(4);
    for (int i = 0; i < 20; i++) {
      final StampedLock lock = new StampedLock();
      lock.asWriteLock().lock();
      //
      Future<?> future1 = executor.submit(() -> {
        CacheTransactionManager txMgr2 = cache.getCacheTransactionManager();
        txMgr2.begin();
        regionpr.get("key1"); // non-tx
        regionpr.put("key2", "value2");

        lock.asReadLock().lock();
        txMgr2.commit();
      });
      Future<?> future2 = executor.submit(() -> {
        CacheTransactionManager txMgr3 = cache.getCacheTransactionManager();
        txMgr3.begin();
        regionpr.get("key1"); // non-tx
        regionpr.put("key3", "value3");

        lock.asReadLock().lock();
        txMgr3.commit();
      });
      Future<?> future3 = executor.submit(() -> {
        CacheTransactionManager txMgr4 = cache.getCacheTransactionManager();
        txMgr4.begin();
        regionpr.get("key1"); // non-tx
        regionpr.put("key4", "value4");

        lock.asReadLock().lock();
        txMgr4.commit();
      });
      Future<?> future4 = executor.submit(() -> {
        CacheTransactionManager txMgr5 = cache.getCacheTransactionManager();
        txMgr5.begin();
        regionpr.get("key1"); // non-tx
        regionpr.put("key5", "value5");

        lock.asReadLock().lock();
        txMgr5.commit();
      });
      pause(100);
      lock.asWriteLock().unlock();
      future1.get();
      future2.get();
      future3.get();
      future4.get();
    }
  }

  /**
   * Test that four transactions with only mixed operations and read operations on same key
   * don't produce CommitConflictException
   * This test fill some initial value on startup and after that create different threads with
   * region#get operations
   * Sync two different threads commit time through lock
   */
  @Test(/* no exception expected */)
  public void testDetectReadConflict4RR()
      throws CacheException, ExecutionException, InterruptedException {
    cache.close();
    createCache();
    TXManagerImpl txMgrImpl = (TXManagerImpl) cache.getCacheTransactionManager();
    // fill some initial key-value on start
    txMgrImpl.begin();
    this.region.put("key1", "value1"); // non-tx
    txMgrImpl.commit();
    ExecutorService executor = Executors.newFixedThreadPool(4);
    for (int i = 0; i < 20; i++) {
      final StampedLock lock = new StampedLock();
      lock.asWriteLock().lock();
      //
      Future<?> future1 = executor.submit(() -> {
        CacheTransactionManager txMgr2 = cache.getCacheTransactionManager();
        txMgr2.begin();
        region.get("key1"); // non-tx
        region.put("key2", "value2");
        lock.asReadLock().lock();
        txMgr2.commit();
      });
      Future<?> future2 = executor.submit(() -> {
        CacheTransactionManager txMgr3 = cache.getCacheTransactionManager();
        txMgr3.begin();
        region.get("key1"); // non-tx
        region.put("key3", "value3");
        lock.asReadLock().lock();
        txMgr3.commit();
      });
      Future<?> future3 = executor.submit(() -> {
        CacheTransactionManager txMgr4 = cache.getCacheTransactionManager();
        txMgr4.begin();
        region.get("key1"); // non-tx
        region.put("key4", "value4");
        lock.asReadLock().lock();
        txMgr4.commit();
      });
      Future<?> future4 = executor.submit(() -> {
        CacheTransactionManager txMgr5 = cache.getCacheTransactionManager();
        txMgr5.begin();
        region.get("key1"); // non-tx
        lock.asReadLock().lock();
        txMgr5.commit();
      });
      pause(100);
      lock.asWriteLock().unlock();
      future1.get();
      future2.get();
      future3.get();
      future4.get();
    }
  }

  private void pause(int msWait) {
    try {
      Thread.sleep(msWait);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }
  }
}
