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
package org.apache.geode.distributed.internal.locks;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.test.junit.categories.DLockTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category(DLockTest.class)
public class DLockServiceLeakTest {

  private Cache cache;
  private DistributedRegion testRegion;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");

    cache = new CacheFactory(properties).create();
    testRegion = (DistributedRegion) cache.createRegionFactory(RegionShortcut.REPLICATE)
        .setScope(Scope.GLOBAL)
        .setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY))
        .create("testRegion");
    testRegion.becomeLockGrantor();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void basicDLockUsage() throws InterruptedException, ExecutionException, TimeoutException {
    Lock lock = testRegion.getDistributedLock("testLockName");
    lock.lockInterruptibly();

    Future<Boolean> future = executorServiceRule.submit(() -> lock.tryLock());
    assertFalse("should not be able to get lock from another thread",
        future.get(5, TimeUnit.SECONDS));

    assertTrue("Lock is reentrant", lock.tryLock());
    // now locked twice.

    future = executorServiceRule.submit(() -> lock.tryLock());
    assertFalse("should not be able to get lock from another thread",
        future.get(5, TimeUnit.SECONDS));

    lock.unlock();

    future = executorServiceRule.submit(() -> lock.tryLock());
    assertFalse("should not be able to get lock from another thread", future.get());

    lock.unlock();

    future = executorServiceRule.submit(() -> {
      boolean locked = lock.tryLock();
      if (!locked) {
        return false;
      }
      lock.unlock();
      return true;
    });
    assertTrue("Another thread can now take out the lock", future.get(5, TimeUnit.SECONDS));

    DLockService lockService = (DLockService) testRegion.getLockService();
    Collection<DLockToken> tokens = lockService.getTokens();

    for (DLockToken token : tokens) {
      assertEquals(0, token.getUsageCount());
    }
  }

  @Test
  public void singleThreadWithCache() {
    putTestKey();

    DLockService lockService = (DLockService) testRegion.getLockService();
    Collection<DLockToken> tokens = lockService.getTokens();

    assertEquals(1, tokens.size());

    for (DLockToken token : tokens) {
      assertEquals(0, token.getUsageCount());
    }
  }

  @Test
  public void multipleThreadsWithCache() {
    LinkedList<Future> futures = new LinkedList<>();
    for (int i = 0; i < 5; i++) {
      futures.add(executorServiceRule.submit(this::putTestKey));
    }

    futures.forEach(future -> {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    });

    DLockService lockService = (DLockService) testRegion.getLockService();
    Collection<DLockToken> tokens = lockService.getTokens();

    assertTrue(tokens.size() < 2);

    for (DLockToken token : tokens) {
      assertEquals(0, token.getUsageCount());
    }
  }

  private void putTestKey() {
    for (int i = 0; i < 1000; i++) {
      testRegion.put("testKey", "testValue");
    }
  }
}
