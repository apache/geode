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

import static org.apache.geode.internal.lang.ThrowableUtils.getRootCause;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.admin.AdminDistributedSystemFactory;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CacheLifecycleListener;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.RegionsTest;

/**
 * Tests the basic use cases for PR persistence.
 */
@Category({RegionsTest.class})
public class ShutdownAllDUnitTest extends JUnit4CacheTestCase {

  private static HangingCacheListener listener;

  private static final String expectedExceptions = InternalGemFireError.class.getName()
      + "||ShutdownAllRequest: disconnect distributed without response";

  private static final int MAX_WAIT = 600 * 1000;

  @Override
  public final void postSetUp() throws Exception {
    // Get rid of any existing distributed systems. We want
    // to make assertions about the number of distributed systems
    // we shut down, so we need to start with a clean slate.
    disconnectAllFromDS();
  }

  @Test
  public void testShutdownAll2Servers() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int numBuckets = 50;

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    createData(vm0, 0, numBuckets, "a", "region");

    Set<Integer> vm0Buckets = getBucketList(vm0, "region");
    Set<Integer> vm1Buckets = getBucketList(vm1, "region");
    assertEquals(vm0Buckets, vm1Buckets);

    shutDownAllMembers(vm2, 2);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());

    // restart vm0
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);

    // restart vm1
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);

    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);

    assertEquals(vm0Buckets, getBucketList(vm0, "region"));

    // checkRecoveredFromDisk(vm0, 0, true);
    // checkRecoveredFromDisk(vm1, 0, false);

    checkData(vm0, 0, numBuckets, "a", "region");
    checkData(vm1, 0, numBuckets, "a", "region");

    createData(vm0, numBuckets, 113, "b", "region");
    checkData(vm0, numBuckets, 113, "b", "region");
  }

  @Test
  public void testShutdownAllWithEncounterIGE1() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    int numBuckets = 50;
    createRegion(vm0, "region", "disk", true, 1);
    createData(vm0, 0, numBuckets, "a", "region");

    vm0.invoke(addExceptionTag1(expectedExceptions));
    Invoke.invokeInEveryVM(new SerializableRunnable("set TestInternalGemFireError") {
      public void run() {
        System.setProperty("TestInternalGemFireError", "true");
      }
    });
    shutDownAllMembers(vm0, 1);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());

    Invoke.invokeInEveryVM(new SerializableRunnable("reset TestInternalGemFireError") {
      public void run() {
        System.setProperty("TestInternalGemFireError", "false");
      }
    });
    vm0.invoke(removeExceptionTag1(expectedExceptions));
  }

  @Test
  public void testShutdownAllWithEncounterIGE2() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int numBuckets = 50;

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    createData(vm0, 0, numBuckets, "a", "region");

    Set<Integer> vm0Buckets = getBucketList(vm0, "region");
    Set<Integer> vm1Buckets = getBucketList(vm1, "region");
    assertEquals(vm0Buckets, vm1Buckets);

    vm0.invoke(addExceptionTag1(expectedExceptions));
    vm1.invoke(addExceptionTag1(expectedExceptions));
    Invoke.invokeInEveryVM(new SerializableRunnable("set TestInternalGemFireError") {
      public void run() {
        System.setProperty("TestInternalGemFireError", "true");
      }
    });
    shutDownAllMembers(vm2, 0);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());

    Invoke.invokeInEveryVM(new SerializableRunnable("reset TestInternalGemFireError") {
      public void run() {
        System.setProperty("TestInternalGemFireError", "false");
      }
    });
    vm0.invoke(removeExceptionTag1(expectedExceptions));
    vm1.invoke(removeExceptionTag1(expectedExceptions));
  }

  private static final AtomicBoolean calledCreateCache = new AtomicBoolean();
  private static final AtomicBoolean calledCloseCache = new AtomicBoolean();
  private static CacheLifecycleListener cll;

  @Test
  public void testShutdownAllInterruptsCacheCreation()
      throws ExecutionException, InterruptedException, TimeoutException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);
    closeAllCache();
    // in vm0 create the cache in a way that hangs until
    // it sees that a shutDownAll is in progress
    AsyncInvocation<?> asyncCreate = vm0.invokeAsync(() -> {
      cll = new CacheLifecycleListener() {
        @Override
        public void cacheCreated(InternalCache cache) {
          calledCreateCache.set(true);
          await().until(() -> cache.isCacheAtShutdownAll());
        }

        @Override
        public void cacheClosed(InternalCache cache) {
          calledCloseCache.set(true);
        }
      };
      GemFireCacheImpl.addCacheLifecycleListener(cll);
      getCache();
    });
    try {
      boolean vm0CalledCreateCache = vm0.invoke(() -> {
        await().until(() -> calledCreateCache.get());
        return calledCreateCache.get();
      });
      assertTrue(vm0CalledCreateCache);
      shutDownAllMembers(vm2, 1);
      asyncCreate.get(60, TimeUnit.SECONDS);
      boolean vm0CalledCloseCache = vm0.invoke(() -> {
        await().until(() -> calledCloseCache.get());
        return calledCloseCache.get();
      });
      assertTrue(vm0CalledCloseCache);
    } finally {
      vm0.invoke(() -> {
        calledCreateCache.set(false);
        calledCloseCache.set(false);
        GemFireCacheImpl.removeCacheLifecycleListener(cll);
      });
    }
  }

  @Test
  public void testShutdownAllOneServerAndRecover() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);

    createRegion(vm0, "region", "disk", true, 0);

    createData(vm0, 0, 1, "a", "region");

    shutDownAllMembers(vm2, 1);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());

    // restart vm0
    createRegion(vm0, "region", "disk", true, 0);

    checkPRRecoveredFromDisk(vm0, "region", 0, true);

    createData(vm0, 1, 10, "b", "region");
  }

  @Test
  public void testPRWithDR() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);

    createRegion(vm0, "region_pr", "disk", true, 0);
    createRegion(vm0, "region_dr", "disk", false, 0);

    createData(vm0, 0, 1, "a", "region_pr");
    createData(vm0, 0, 1, "c", "region_dr");

    shutDownAllMembers(vm2, 1);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());

    // restart vm0
    createRegion(vm0, "region_pr", "disk", true, 0);
    createRegion(vm0, "region_dr", "disk", false, 0);

    checkPRRecoveredFromDisk(vm0, "region_pr", 0, true);

    checkData(vm0, 0, 1, "a", "region_pr");
    checkData(vm0, 0, 1, "c", "region_dr");
  }

  @Test
  public void testShutdownAllFromServer() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int numBuckets = 50;

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);
    createRegion(vm2, "region", "disk", true, 1);

    createData(vm0, 0, numBuckets, "a", "region");

    shutDownAllMembers(vm2, 3);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());

    // restart vm0, vm1, vm2
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);

    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);

    AsyncInvocation a2 = createRegionAsync(vm2, "region", "disk", true, 1);

    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);
    a2.getResult(MAX_WAIT);

    createData(vm0, 0, numBuckets, "a", "region");
    createData(vm1, 0, numBuckets, "a", "region");
    createData(vm2, 0, numBuckets, "a", "region");

    createData(vm0, numBuckets, 113, "b", "region");
    checkData(vm0, numBuckets, 113, "b", "region");
  }

  // shutdownAll, then restart to verify
  @Test
  public void testCleanStop() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    createData(vm0, 0, 1, "a", "region");

    shutDownAllMembers(vm2, 2);

    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);
    // [dsmith] Make sure that vm0 is waiting for vm1 to recover
    // If VM(0) recovers early, that is a problem, because we
    // are no longer doing a clean recovery.
    Thread.sleep(500);
    assertTrue(a0.isAlive());
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);
    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);

    checkData(vm0, 0, 1, "a", "region");
    checkData(vm1, 0, 1, "a", "region");

    checkPRRecoveredFromDisk(vm0, "region", 0, true);
    checkPRRecoveredFromDisk(vm1, "region", 0, true);

    closeRegion(vm0, "region");
    closeRegion(vm1, "region");

    a0 = createRegionAsync(vm0, "region", "disk", true, 1);
    a1 = createRegionAsync(vm1, "region", "disk", true, 1);
    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);

    checkData(vm0, 0, 1, "a", "region");
    checkData(vm1, 0, 1, "a", "region");

    checkPRRecoveredFromDisk(vm0, "region", 0, false);
    checkPRRecoveredFromDisk(vm1, "region", 0, true);
  }

  // shutdownAll, then restart to verify
  @Test
  public void testCleanStopWithConflictCachePort() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    // 2 vms use the same port no to conflict
    addCacheServer(vm0, 34505);
    addCacheServer(vm1, 34505);

    createData(vm0, 0, 1, "a", "region");

    shutDownAllMembers(vm2, 2);

    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);
    // [dsmith] Make sure that vm0 is waiting for vm1 to recover
    // If VM(0) recovers early, that is a problem, because we
    // are no longer doing a clean recovery.
    Thread.sleep(500);
    assertTrue(a0.isAlive());
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);
    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);

    addCacheServer(vm0, 34505);
    addCacheServer(vm1, 34506);

    checkData(vm0, 0, 1, "a", "region");
    checkData(vm1, 0, 1, "a", "region");

    checkPRRecoveredFromDisk(vm0, "region", 0, true);
    checkPRRecoveredFromDisk(vm1, "region", 0, true);
  }

  @Test
  public void testMultiPRDR() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm2 = host.getVM(2);

    createRegion(vm0, "region_pr1", "disk1", true, 0);
    createRegion(vm0, "region_pr2", "disk1", true, 0);
    createRegion(vm0, "region_pr3", "disk1", true, 0);
    createRegion(vm0, "region_dr1", "disk2", false, 0);
    createRegion(vm0, "region_dr2", "disk2", false, 0);

    createData(vm0, 0, 1, "a", "region_pr1");
    createData(vm0, 0, 1, "b", "region_pr2");
    createData(vm0, 0, 1, "c", "region_pr3");
    createData(vm0, 0, 1, "d", "region_dr1");
    createData(vm0, 0, 1, "e", "region_dr2");

    shutDownAllMembers(vm2, 1);

    assertTrue(InternalDistributedSystem.getExistingSystems().isEmpty());

    // restart vm0
    createRegion(vm0, "region_pr1", "disk1", true, 0);
    createRegion(vm0, "region_pr2", "disk1", true, 0);
    createRegion(vm0, "region_pr3", "disk1", true, 0);
    createRegion(vm0, "region_dr1", "disk2", false, 0);
    createRegion(vm0, "region_dr2", "disk2", false, 0);

    // checkPRRecoveredFromDisk(vm0, "region_pr1", 0, true);
    // checkPRRecoveredFromDisk(vm0, "region_pr2", 0, true);
    // checkPRRecoveredFromDisk(vm0, "region_pr3", 0, true);

    checkData(vm0, 0, 1, "a", "region_pr1");
    checkData(vm0, 0, 1, "b", "region_pr2");
    checkData(vm0, 0, 1, "c", "region_pr3");
    checkData(vm0, 0, 1, "d", "region_dr1");
    checkData(vm0, 0, 1, "e", "region_dr2");
  }


  @Test
  public void testShutdownAllTimeout() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final int numBuckets = 50;

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    createData(vm0, 0, numBuckets, "a", "region");

    Set<Integer> vm0Buckets = getBucketList(vm0, "region");
    Set<Integer> vm1Buckets = getBucketList(vm1, "region");
    assertEquals(vm0Buckets, vm1Buckets);

    // Add a cache listener that will cause the system to hang up.
    // Then do some puts to get us stuck in a put.
    AsyncInvocation async1 = vm0.invokeAsync(new SerializableRunnable() {
      public void run() {
        Region<Object, Object> region = getCache().getRegion("region");
        listener = new HangingCacheListener();
        region.getAttributesMutator().addCacheListener(listener);

        // get us stuck doing a put.
        for (int i = 0; i < numBuckets; i++) {
          region.put(i, "a");
        }
      }
    });

    // Make sure the we do get stuck
    async1.join(1000);
    assertTrue(async1.isAlive());


    // Do a shutdownall with a timeout.
    // This will hit the timeout, because the in progress put will
    // prevent us from gracefully shutting down.
    long start = System.nanoTime();
    shutDownAllMembers(vm2, 0, 2000);
    long end = System.nanoTime();

    // Make sure we waited for the timeout.
    assertTrue(end - start > TimeUnit.MILLISECONDS.toNanos(1500));


    // clean up our stuck thread
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        listener.latch.countDown();
        listener = null;
      }
    });

    // wait for shutdown to finish
    Wait.pause(10000);

    // restart vm0
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);

    // restart vm1
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);

    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);

    assertEquals(vm0Buckets, getBucketList(vm0, "region"));

    checkData(vm0, 0, numBuckets, "a", "region");
    checkData(vm1, 0, numBuckets, "a", "region");

    createData(vm0, numBuckets, 113, "b", "region");
    checkData(vm0, numBuckets, 113, "b", "region");
  }

  /**
   * Test for 43551. Do a shutdown all with some members waiting on recovery.
   */
  @Test
  public void testShutdownAllWithMembersWaiting() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final int numBuckets = 5;

    createRegion(vm0, "region", "disk", true, 1);
    createRegion(vm1, "region", "disk", true, 1);

    createData(vm0, 0, numBuckets, "a", "region");

    Set<Integer> vm0Buckets = getBucketList(vm0, "region");
    Set<Integer> vm1Buckets = getBucketList(vm1, "region");


    // shutdown all the members
    shutDownAllMembers(vm2, 2);

    // restart one of the members (this will hang, waiting for the other members)
    // restart vm0
    AsyncInvocation a0 = createRegionAsync(vm0, "region", "disk", true, 1);

    // Wait a bit for the initialization to get stuck
    Wait.pause(20000);
    assertTrue(a0.isAlive());

    // Do another shutdown all, with a member offline and another stuck
    shutDownAllMembers(vm2, 1);

    // This should complete (but maybe it will throw an exception?)
    try {
      a0.getResult(MAX_WAIT);
      fail("should have received a cache closed exception");
    } catch (AssertionError e) {
      if (!CacheClosedException.class.isInstance(getRootCause(e))) {
        throw e;
      }
    }

    // now restart both members. This should work, but
    // no guarantee they'll do a clean recovery
    a0 = createRegionAsync(vm0, "region", "disk", true, 1);
    AsyncInvocation a1 = createRegionAsync(vm1, "region", "disk", true, 1);

    a0.getResult(MAX_WAIT);
    a1.getResult(MAX_WAIT);

    assertEquals(vm0Buckets, getBucketList(vm0, "region"));

    checkData(vm0, 0, numBuckets, "a", "region");
    checkData(vm1, 0, numBuckets, "a", "region");

    createData(vm0, numBuckets, numBuckets * 2, "b", "region");
    checkData(vm0, numBuckets, numBuckets * 2, "b", "region");
  }

  // TODO prpersist
  // test move bucket
  // test async put
  // test create a new bucket by put async


  private void shutDownAllMembers(VM vm, final int expnum) {
    vm.invoke(new SerializableRunnable("Shutdown all the members") {

      public void run() {
        DistributedSystemConfig config;
        AdminDistributedSystemImpl adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = (AdminDistributedSystemImpl) AdminDistributedSystemFactory
              .getDistributedSystem(config);
          adminDS.connect();
          Set members = adminDS.shutDownAllMembers();
          int num = members == null ? 0 : members.size();
          assertEquals(expnum, num);
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });

    // clean up for this vm
    System.setProperty("TestInternalGemFireError", "false");
  }

  private SerializableRunnable getCreateDRRunnable(final String regionName,
      final String diskStoreName) {
    return new SerializableRunnable("create DR") {
      public void run() {
        Cache cache = ShutdownAllDUnitTest.this.getCache();

        DiskStore ds = cache.findDiskStore(diskStoreName);
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create(diskStoreName);
        }
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);

        af.setDiskStoreName(diskStoreName);
        cache.createRegion(regionName, af.create());
      }
    };
  }

  protected void addCacheServer(VM vm, final int port) {
    vm.invoke(new SerializableRunnable("add Cache Server") {
      public void run() {
        Cache cache = getCache();
        CacheServer cs = cache.addCacheServer();
        cs.setPort(port);
        try {
          cs.start();
        } catch (IOException e) {
          System.out.println("Received expected " + e.getMessage());
        }
      }
    });
  }

  protected void createRegion(VM vm, final String regionName, final String diskStoreName,
      final boolean isPR, final int redundancy) {
    if (isPR) {
      SerializableRunnable createPR = getCreatePRRunnable(regionName, diskStoreName, redundancy);
      vm.invoke(createPR);
    } else {
      SerializableRunnable createPR = getCreateDRRunnable(regionName, diskStoreName);
      vm.invoke(createPR);
    }
  }

  protected AsyncInvocation createRegionAsync(VM vm, final String regionName,
      final String diskStoreName, final boolean isPR, final int redundancy) {
    if (isPR) {
      SerializableRunnable createPR = getCreatePRRunnable(regionName, diskStoreName, redundancy);
      return vm.invokeAsync(createPR);
    } else {
      SerializableRunnable createDR = getCreateDRRunnable(regionName, diskStoreName);
      return vm.invokeAsync(createDR);
    }
  }

  private SerializableRunnable getCreatePRRunnable(final String regionName,
      final String diskStoreName, final int redundancy) {
    return new SerializableRunnable("create pr") {
      @Override
      public void run() {
        final CountDownLatch recoveryDone;
        if (redundancy > 0) {
          recoveryDone = new CountDownLatch(1);

          ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
            @Override
            public void recoveryFinished(Region region) {
              recoveryDone.countDown();
            }
          };
          InternalResourceManager.setResourceObserver(observer);
        } else {
          recoveryDone = null;
        }

        Cache cache = ShutdownAllDUnitTest.this.getCache();

        if (diskStoreName != null) {
          DiskStore ds = cache.findDiskStore(diskStoreName);
          if (ds == null) {
            ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create(diskStoreName);
          }
        }
        AttributesFactory af = new AttributesFactory();
        af.setDiskSynchronous(false); // use async to trigger flush
        af.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy);
        af.setPartitionAttributes(paf.create());
        if (diskStoreName != null) {
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName(diskStoreName);
        } else {
          af.setDataPolicy(DataPolicy.PARTITION);
        }
        cache.createRegion(regionName, af.create());
        if (recoveryDone != null) {
          try {
            recoveryDone.await();
          } catch (InterruptedException e) {
            Assert.fail("Interrupted", e);
          }
        }
      }
    };
  }

  protected void createData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable createData = new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }

  protected Set<Integer> getBucketList(VM vm, final String regionName) {
    SerializableCallable getBuckets = new SerializableCallable("get buckets") {

      public Object call() throws Exception {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        if (region instanceof PartitionedRegion) {
          PartitionedRegion pr = (PartitionedRegion) region;
          return new TreeSet<Integer>(pr.getDataStore().getAllLocalBucketIds());
        } else {
          return null;
        }
      }
    };

    return (Set<Integer>) vm.invoke(getBuckets);
  }

  protected void checkData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable checkData = new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
          assertEquals(value, region.get(i));
        }
      }
    };

    vm.invoke(checkData);
  }

  protected void checkPRRecoveredFromDisk(VM vm, final String regionName, final int bucketId,
      final boolean recoveredLocally) {
    vm.invoke(new SerializableRunnable("check PR recovered from disk") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        DiskRegion disk = region.getRegionAdvisor().getBucket(bucketId).getDiskRegion();
        if (recoveredLocally) {
          assertEquals(0, disk.getStats().getRemoteInitializations());
          assertEquals(1, disk.getStats().getLocalInitializations());
        } else {
          assertEquals(1, disk.getStats().getRemoteInitializations());
          assertEquals(0, disk.getStats().getLocalInitializations());
        }
      }
    });
  }

  protected void closeRegion(VM vm, final String regionName) {
    SerializableRunnable close = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        region.close();
      }
    };

    vm.invoke(close);
  }

  private void shutDownAllMembers(VM vm, final int expnum, final long timeout) {
    vm.invoke(new SerializableRunnable("Shutdown all the members") {

      public void run() {
        DistributedSystemConfig config;
        AdminDistributedSystemImpl adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = (AdminDistributedSystemImpl) AdminDistributedSystemFactory
              .getDistributedSystem(config);
          adminDS.connect();
          Set members = adminDS.shutDownAllMembers(timeout);
          int num = members == null ? 0 : members.size();
          assertEquals(expnum, num);
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  private static class HangingCacheListener extends CacheListenerAdapter {
    CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void afterUpdate(EntryEvent event) {
      try {
        latch.await();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
