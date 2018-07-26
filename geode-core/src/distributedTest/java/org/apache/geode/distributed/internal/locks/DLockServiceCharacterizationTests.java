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
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.internal.cache.DistributedRegion;

public class DLockServiceCharacterizationTests {
  private Cache cache;
  private DistributedRegion testRegion;
  private DistributedLockService dLockService;

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

    dLockService = DLockService.create("testService", cache.getDistributedSystem());
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void reentrantLockIncreasesReentrancy() {
    assertTrue(dLockService.lock("key1", -1, -1));
    DLockToken key1 = ((DLockService) dLockService).getToken("key1");

    assertEquals(0, key1.getRecursion());
    assertEquals(1, key1.getUsageCount());
    // reentrancy + 1
    assertTrue(dLockService.lock("key1", -1, -1));

    assertEquals(1, key1.getRecursion());
    assertEquals(2, key1.getUsageCount());

    dLockService.unlock("key1");
    assertEquals(0, key1.getRecursion());
    assertEquals(1, key1.getUsageCount());

    dLockService.unlock("key1");
    assertTokenIsUnused(key1);
  }

  @Test
  public void threadWaitingOnLockIncreasesUsageCount() {
    assertTrue(dLockService.lock("key1", -1, -1));
    DLockToken key1 = ((DLockService) dLockService).getToken("key1");

    assertEquals(0, key1.getRecursion());
    assertEquals(1, key1.getUsageCount());
    assertEquals(Thread.currentThread(), key1.getThread());

    Thread otherThread = new Thread(() -> dLockService.lock("key1", -1, -1));
    otherThread.start();

    // otherThread should be waiting for lock.

    await("other thread is waiting on this lock").atMost(3, TimeUnit.SECONDS)
        .until(() -> key1.getUsageCount() == 2);
    assertEquals(0, key1.getRecursion());
    assertEquals(Thread.currentThread(), key1.getThread());

    dLockService.unlock("key1");

    await("other thread has acquired this lock").atMost(3, TimeUnit.SECONDS)
        .until(() -> key1.getThread() == otherThread);

    assertEquals(0, key1.getRecursion());
    assertEquals(1, key1.getUsageCount());

    // We can unlock from a different thread than locked it.
    dLockService.unlock("key1");

    assertTokenIsUnused(key1);
  }

  private void assertTokenIsUnused(DLockToken dLockToken) {
    assertEquals(0, dLockToken.getRecursion());
    assertEquals(0, dLockToken.getUsageCount());
    assertEquals(null, dLockToken.getThread());
    assertEquals(null, dLockToken.getLesseeThread());
    assertEquals(-1, dLockToken.getLeaseId());
  }
}
