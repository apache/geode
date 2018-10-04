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
package org.apache.geode.internal.offheap;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.OutOfOffHeapMemoryException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.OffHeapTestUtil;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OffHeapTest;

/**
 * Test behavior of region when running out of off-heap memory.
 */
@Category({OffHeapTest.class})
@SuppressWarnings("serial")
public class OutOfOffHeapMemoryDUnitTest extends JUnit4CacheTestCase {

  protected static final AtomicReference<Cache> cache = new AtomicReference<Cache>();
  protected static final AtomicReference<DistributedSystem> system =
      new AtomicReference<DistributedSystem>();
  protected static final AtomicBoolean isSmallerVM = new AtomicBoolean();

  @Override
  public final void preSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void postSetUp() throws Exception {
    IgnoredException.addIgnoredException(OutOfOffHeapMemoryException.class.getSimpleName());
  }

  @Override
  public final void preTearDownAssertions() throws Exception {
    final SerializableRunnable checkOrphans = new SerializableRunnable() {
      @Override
      public void run() {
        if (hasCache()) {
          OffHeapTestUtil.checkOrphans(getCache());
        }
      }
    };
    Invoke.invokeInEveryVM(checkOrphans);
    checkOrphans.run();
  }

  @SuppressWarnings("unused") // invoked by reflection from tearDown2()
  private static void cleanup() {
    disconnectFromDS();
    MemoryAllocatorImpl.freeOffHeapMemory();
    cache.set(null);
    system.set(null);
    isSmallerVM.set(false);
  }

  protected String getOffHeapMemorySize() {
    return "2m";
  }

  protected String getSmallerOffHeapMemorySize() {
    return "1m";
  }

  protected RegionShortcut getRegionShortcut() {
    return RegionShortcut.REPLICATE;
  }

  protected String getRegionName() {
    return "region1";
  }

  @Override
  public Properties getDistributedSystemProperties() {
    final Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(STATISTIC_SAMPLING_ENABLED, "true");
    if (isSmallerVM.get()) {
      props.setProperty(OFF_HEAP_MEMORY_SIZE, getSmallerOffHeapMemorySize());
    } else {
      props.setProperty(OFF_HEAP_MEMORY_SIZE, getOffHeapMemorySize());
    }
    return props;
  }

  @Test
  public void testSimpleOutOfOffHeapMemoryMemberDisconnects() {
    final DistributedSystem system = getSystem();
    final Cache cache = getCache();
    final ClusterDistributionManager dm =
        (ClusterDistributionManager) ((InternalDistributedSystem) system).getDistributionManager();

    Region<Object, Object> region =
        cache.createRegionFactory(getRegionShortcut()).setOffHeap(true).create(getRegionName());
    OutOfOffHeapMemoryException ooohme;
    try {
      Object value = new byte[1024];
      for (int i = 0; true; i++) {
        region.put("key-" + i, value);
      }
    } catch (OutOfOffHeapMemoryException e) {
      ooohme = e;
    }
    assertNotNull(ooohme);

    await()
        .until(() -> cache.isClosed() && !system.isConnected() && dm.isClosed());

    // wait for cache instance to be nulled out
    await()
        .until(() -> cache.isClosed() && !system.isConnected());

    // verify system was closed out due to OutOfOffHeapMemoryException
    assertFalse(system.isConnected());
    InternalDistributedSystem ids = (InternalDistributedSystem) system;
    try {
      ids.getDistributionManager();
      fail(
          "InternalDistributedSystem.getDistributionManager() should throw DistributedSystemDisconnectedException");
    } catch (DistributedSystemDisconnectedException expected) {
      assertRootCause(expected, OutOfOffHeapMemoryException.class);
    }

    // verify dm was closed out due to OutOfOffHeapMemoryException
    assertTrue(dm.isClosed());
    try {
      dm.throwIfDistributionStopped();
      fail(
          "DistributionManager.throwIfDistributionStopped() should throw DistributedSystemDisconnectedException");
    } catch (DistributedSystemDisconnectedException expected) {
      assertRootCause(expected, OutOfOffHeapMemoryException.class);
    }

    // verify cache was closed out due to OutOfOffHeapMemoryException
    assertTrue(cache.isClosed());
    try {
      cache.getCancelCriterion().checkCancelInProgress(null);
      fail(
          "GemFireCacheImpl.getCancelCriterion().checkCancelInProgress should throw DistributedSystemDisconnectedException");
    } catch (DistributedSystemDisconnectedException expected) {
      assertRootCause(expected, OutOfOffHeapMemoryException.class);
    }
  }

  private void assertRootCause(Throwable throwable, Class<?> expected) {
    boolean passed = false;
    Throwable cause = throwable.getCause();
    while (cause != null) {
      if (cause.getClass().equals(expected)) {
        passed = true;
        break;
      }
      cause = cause.getCause();
    }
    if (!passed) {
      throw new AssertionError("Throwable does not contain expected root cause " + expected,
          throwable);
    }
  }

  @Test
  public void testOtherMembersSeeOutOfOffHeapMemoryMemberDisconnects() {
    final int vmCount = Host.getHost(0).getVMCount();

    final String name = getRegionName();
    final RegionShortcut shortcut = getRegionShortcut();
    final int biggerVM = 0;
    final int smallerVM = 1;

    Host.getHost(0).getVM(smallerVM).invoke(new SerializableRunnable() {
      public void run() {
        OutOfOffHeapMemoryDUnitTest.isSmallerVM.set(true);
      }
    });

    // create off-heap region in all members
    for (int i = 0; i < vmCount; i++) {
      Host.getHost(0).getVM(i).invoke(new SerializableRunnable() {
        public void run() {
          OutOfOffHeapMemoryDUnitTest.cache.set(getCache());
          OutOfOffHeapMemoryDUnitTest.system.set(getSystem());

          final Region<Object, Object> region = OutOfOffHeapMemoryDUnitTest.cache.get()
              .createRegionFactory(shortcut).setOffHeap(true).create(name);
          assertNotNull(region);
        }
      });
    }

    // make sure there are vmCount+1 members total
    for (int i = 0; i < vmCount; i++) {
      Host.getHost(0).getVM(i).invoke(new SerializableRunnable() {
        public void run() {
          assertFalse(OutOfOffHeapMemoryDUnitTest.cache.get().isClosed());
          assertTrue(OutOfOffHeapMemoryDUnitTest.system.get().isConnected());

          final int countMembersPlusLocator = vmCount + 1; // +1 for locator
          final int countOtherMembers = vmCount - 1; // -1 one for self

          assertEquals(countMembersPlusLocator,
              ((InternalDistributedSystem) OutOfOffHeapMemoryDUnitTest.system.get())
                  .getDistributionManager().getDistributionManagerIds().size());
          assertEquals(countOtherMembers,
              ((DistributedRegion) OutOfOffHeapMemoryDUnitTest.cache.get().getRegion(name))
                  .getDistributionAdvisor().getNumProfiles());
        }
      });
    }

    // perform puts in bigger member until smaller member goes OOOHME
    Host.getHost(0).getVM(biggerVM).invoke(new SerializableRunnable() {
      public void run() {
        final long TIME_LIMIT = 30 * 1000;
        final StopWatch stopWatch = new StopWatch(true);

        int countOtherMembers = vmCount - 1; // -1 for self
        final int countOtherMembersMinusSmaller = vmCount - 1 - 1; // -1 for self, -1 for smallerVM

        final Region<Object, Object> region =
            OutOfOffHeapMemoryDUnitTest.cache.get().getRegion(name);
        for (int i = 0; countOtherMembers > countOtherMembersMinusSmaller; i++) {
          region.put("key-" + i, new byte[1024]);
          countOtherMembers =
              ((DistributedRegion) OutOfOffHeapMemoryDUnitTest.cache.get().getRegion(name))
                  .getDistributionAdvisor().getNumProfiles();
          assertTrue("puts failed to push member out of off-heap memory within time limit",
              stopWatch.elapsedTimeMillis() < TIME_LIMIT);
        }
        assertEquals("Member did not depart from OutOfOffHeapMemory", countOtherMembersMinusSmaller,
            countOtherMembers);
      }
    });

    // verify that member with OOOHME closed
    Host.getHost(0).getVM(smallerVM).invoke(new SerializableRunnable() {
      public void run() {
        assertTrue(OutOfOffHeapMemoryDUnitTest.cache.get().isClosed());
        assertFalse(OutOfOffHeapMemoryDUnitTest.system.get().isConnected());
      }
    });

    // verify that all other members noticed smaller member closed
    for (int i = 0; i < vmCount; i++) {
      if (i == smallerVM) {
        continue;
      }
      Host.getHost(0).getVM(i).invoke(new SerializableRunnable() {
        public void run() {
          final int countMembersPlusLocator = vmCount + 1 - 1; // +1 for locator, -1 for OOOHME
                                                               // member
          final int countOtherMembers = vmCount - 1 - 1; // -1 for self, -1 for OOOHME member

          await()
              .until(numDistributionManagers(), equalTo(countMembersPlusLocator));
          await()
              .until(numProfiles(), equalTo(countOtherMembers));

        }

        private Callable<Integer> numProfiles() {
          return () -> {
            DistributedRegion dr =
                (DistributedRegion) OutOfOffHeapMemoryDUnitTest.cache.get().getRegion(name);
            return dr.getDistributionAdvisor().getNumProfiles();
          };
        }

        private Callable<Integer> numDistributionManagers() {
          return () -> {
            InternalDistributedSystem ids =
                (InternalDistributedSystem) OutOfOffHeapMemoryDUnitTest.system.get();
            return ids.getDistributionManager().getDistributionManagerIds().size();
          };
        }
      });
    }
  }

}
