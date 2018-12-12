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

import static java.lang.Thread.currentThread;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.DistributedLockService.getServiceNamed;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

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
    assertThat(service.isHeldByCurrentThread(objectName)).isFalse();

    // Get lock
    assertThat(service.lock(objectName, 3000, -1)).isTrue();
    assertThat(service.isHeldByCurrentThread(objectName)).isTrue();
    assertThat(service.lock(objectName, 3000, -1)).isTrue();
    assertThat(service.isHeldByCurrentThread(objectName)).isTrue();

    // Release lock
    service.unlock(objectName);
    assertThat(service.isHeldByCurrentThread(objectName)).isTrue();
    service.unlock(objectName);
    assertThat(service.isHeldByCurrentThread(objectName)).isFalse();

    // Destroy service
    DistributedLockService.destroy(serviceName);
  }

  @Test
  public void reentrantLockIncreasesReentrancy() {
    assertThat(dLockService.lock("key1", -1, -1)).isTrue();
    DLockToken key1 = ((DLockService) dLockService).getToken("key1");

    assertThat(key1.getRecursion()).isEqualTo(0);
    assertThat(key1.getUsageCount()).isEqualTo(1);
    // reentrancy + 1
    assertThat(dLockService.lock("key1", -1, -1)).isTrue();

    assertThat(key1.getRecursion()).isEqualTo(1);
    assertThat(key1.getUsageCount()).isEqualTo(2);

    dLockService.unlock("key1");
    assertThat(key1.getRecursion()).isEqualTo(0);
    assertThat(key1.getUsageCount()).isEqualTo(1);

    dLockService.unlock("key1");
    assertTokenIsUnused(key1);
  }

  @Test
  public void threadWaitingOnLockIncreasesUsageCount() {
    assertThat(dLockService.lock("key1", -1, -1)).isTrue();
    DLockToken key1 = ((DLockService) dLockService).getToken("key1");

    assertThat(key1.getRecursion()).isEqualTo(0);
    assertThat(key1.getUsageCount()).isEqualTo(1);
    assertThat(key1.getThread()).isEqualTo(currentThread());

    Thread otherThread = new Thread(() -> dLockService.lock("key1", -1, -1));
    otherThread.start();

    // otherThread should be waiting for lock.

    await("other thread is waiting on this lock")
        .until(() -> key1.getUsageCount() == 2);
    assertThat(key1.getRecursion()).isEqualTo(0);
    assertThat(key1.getThread()).isEqualTo(currentThread());

    dLockService.unlock("key1");

    await("other thread has acquired this lock")
        .until(() -> key1.getThread() == otherThread);

    assertThat(key1.getRecursion()).isEqualTo(0);
    assertThat(key1.getUsageCount()).isEqualTo(1);

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

    assertThat(getServiceNamed(serviceName)).isNull();
    DistributedLockService service = DistributedLockService.create(serviceName, distributedSystem);
    assertThat(getServiceNamed(serviceName)).isSameAs(service);
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

      await().until(serviceDestroyed::get);
      service.unlock(lockName);
      return null;
    };

    // start a new thread to wait for lock
    Callable<Void> getBlocked = () -> {
      await("wait for lock to be acquired").until(
          lockAcquired::get);
      service.lock(lockName, -1, -1);
      return null;
    };

    Callable<Void> destroyLockService = () -> {
      await("lock acquired; we don't want to break that")
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
    assertThat(dLockToken.getRecursion()).isEqualTo(0);
    assertThat(dLockToken.getUsageCount()).isEqualTo(0);
    assertThat(dLockToken.getThread()).isNull();
    assertThat(dLockToken.getLesseeThread()).isNull();
    assertThat(dLockToken.getLeaseId()).isEqualTo(-1);
  }
}
