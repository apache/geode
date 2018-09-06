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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.test.junit.rules.ConcurrencyRule;

public class DLockServiceCharacterizationTests {
  private Cache cache;
  private DistributedRegion testRegion;
  private DistributedLockService dLockService;
  private DistributedSystem distributedSystem;

  @Rule
  public ConcurrencyRule concurrencyRule = new ConcurrencyRule();

  @Before
  public void setUp() {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");

    cache = new CacheFactory(properties).create();
    distributedSystem = cache.getDistributedSystem();
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
  public void testBasic() {
    String serviceName = "serviceName";
    String objectName = "object";

    // Create service
    DistributedLockService service = DistributedLockService.create(serviceName, distributedSystem);

    // Not locked initially
    assertFalse(service.isHeldByCurrentThread(objectName));

    // Get lock
    assertTrue(service.lock(objectName, 3000, -1));
    assertTrue(service.isHeldByCurrentThread(objectName));
    assertTrue(service.lock(objectName, 3000, -1));
    assertTrue(service.isHeldByCurrentThread(objectName));

    // Release lock
    service.unlock(objectName);
    assertTrue(service.isHeldByCurrentThread(objectName));
    service.unlock(objectName);
    assertFalse(service.isHeldByCurrentThread(objectName));

    // Destroy service
    DistributedLockService.destroy(serviceName);
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

  @Test
  public void whatHappensAfterDestroyingTheDLockService() throws Exception {
    final String serviceName = "Somestring";

    DistributedLockService.create(serviceName, distributedSystem);
    DistributedLockService.destroy(serviceName);

    assertThatThrownBy(() -> DistributedLockService.becomeLockGrantor(serviceName))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(DistributedLockService.getServiceNamed(serviceName)).isNull();
    assertThatThrownBy(() -> DistributedLockService.destroy(serviceName))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> DistributedLockService.isLockGrantor(serviceName))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whatHappensWhenTheDLockServiceWasNeverCreated() throws Exception {
    final String serviceName =
        "Please don't use this anywhere else " + this.getClass().getCanonicalName();

    assertThatThrownBy(() -> DistributedLockService.becomeLockGrantor(serviceName))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(DistributedLockService.getServiceNamed(serviceName)).isNull();
    assertThatThrownBy(() -> DistributedLockService.destroy(serviceName))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> DistributedLockService.isLockGrantor(serviceName))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void serviceCanBeRecreatedAndUsed() {
    final String serviceName = "someString";
    final String lockName = "lock name";

    DistributedLockService.create(serviceName, distributedSystem);
    DistributedLockService.getServiceNamed(serviceName).lock(lockName, 10000, -1);

    DistributedLockService.destroy(serviceName);

    DistributedLockService.create(serviceName, distributedSystem);
    assertThat(DistributedLockService.getServiceNamed(serviceName).lock(lockName, 10000, -1))
        .isTrue();
    DistributedLockService.getServiceNamed(serviceName).unlock(lockName);
    DistributedLockService.destroy(serviceName);
  }

  @Test
  public void createAndDestroy() throws Exception {
    final String serviceName = "service name";

    final Cache cache = new CacheFactory().create();
    final DistributedSystem distributedSystem = cache.getDistributedSystem();

    assertNull(DistributedLockService.getServiceNamed(serviceName));
    DistributedLockService service = DistributedLockService.create(serviceName, distributedSystem);
    assertSame(service, DistributedLockService.getServiceNamed(serviceName));
    DistributedLockService.destroy(serviceName);
  }

  /**
   * Start a DLS in a thread, lock something from it in another thread, destroy it in yet a third.
   */
  @Test
  public void destroyedLockServiceThrowsLockServiceDestroyedException() throws Exception {
    final String serviceName = "service name";
    final String lockName = "abc";

    final Cache cache = new CacheFactory().create();
    final DistributedSystem distributedSystem = cache.getDistributedSystem();

    final DistributedLockService service =
        DistributedLockService.create(serviceName, distributedSystem);
    assertThat(((DLockService) service).isDestroyed()).isFalse();

    final AtomicBoolean lockAcquired = new AtomicBoolean(false);
    final AtomicBoolean serviceDestroyed = new AtomicBoolean(false);

    // get the same dls from another thread and hold a lock.
    Callable<Void> getLock = () -> {
      assertThat(catchThrowable(() -> {
        service.lock(lockName, -1, -1);
        lockAcquired.set(true);
      })).isNull();

      Awaitility.await().atMost(5, TimeUnit.MINUTES).until(serviceDestroyed::get);
      service.unlock(lockName);
      return null;
    };

    // start a new thread to wait for lock
    Callable<Void> getBlocked = () -> {
      Awaitility.await("wait for lock to be acquired").atMost(5, TimeUnit.MINUTES).until(
          lockAcquired::get);
      service.lock(lockName, -1, -1);
      return null;
    };

    Callable<Void> destroyLockService = () -> {
      Awaitility.await("lock acquired; we don't want to break that").atMost(25, TimeUnit.SECONDS)
          .until(lockAcquired::get);
      DistributedLockService.destroy(serviceName);
      serviceDestroyed.set(true);
      return null;
    };

    concurrencyRule.add(getLock).expectExceptionType(LockServiceDestroyedException.class);
    concurrencyRule.add(getBlocked).expectExceptionType(LockServiceDestroyedException.class);
    concurrencyRule.add(destroyLockService);

    concurrencyRule.executeInParallel();
  }

  private void assertTokenIsUnused(DLockToken dLockToken) {
    assertEquals(0, dLockToken.getRecursion());
    assertEquals(0, dLockToken.getUsageCount());
    assertNull(dLockToken.getThread());
    assertNull(dLockToken.getLesseeThread());
    assertEquals(-1, dLockToken.getLeaseId());
  }
}
