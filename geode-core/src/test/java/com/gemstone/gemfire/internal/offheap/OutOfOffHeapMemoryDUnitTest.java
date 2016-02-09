/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.offheap;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.OffHeapTestUtil;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;


import static com.jayway.awaitility.Awaitility.with;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Test behavior of region when running out of off-heap memory.
 * 
 * @author Kirk Lund
 */
@SuppressWarnings("serial")
public class OutOfOffHeapMemoryDUnitTest extends CacheTestCase {
  private static final Logger logger = LogService.getLogger();
  
  protected static final AtomicReference<Cache> cache = new AtomicReference<Cache>();
  protected static final AtomicReference<DistributedSystem> system = new AtomicReference<DistributedSystem>();
  protected static final AtomicBoolean isSmallerVM = new AtomicBoolean();
  
  public OutOfOffHeapMemoryDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    disconnectAllFromDS();
    super.setUp();
    IgnoredException.addIgnoredException(OutOfOffHeapMemoryException.class.getSimpleName());
  }

  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    final SerializableRunnable checkOrphans = new SerializableRunnable() {
      @Override
      public void run() {
        if(hasCache()) {
          OffHeapTestUtil.checkOrphans();
        }
      }
    };
    Invoke.invokeInEveryVM(checkOrphans);
    checkOrphans.run();
  }

  @SuppressWarnings("unused") // invoked by reflection from tearDown2()
  private static void cleanup() {
    disconnectFromDS();
    SimpleMemoryAllocatorImpl.freeOffHeapMemory();
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
    props.put(DistributionConfig.MCAST_PORT_NAME, "0");
    props.put(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    if (isSmallerVM.get()) {
      props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, getSmallerOffHeapMemorySize());
    } else {
      props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, getOffHeapMemorySize());
    }
    return props;
  }
  
  public void testSimpleOutOfOffHeapMemoryMemberDisconnects() {
    final DistributedSystem system = getSystem();
    final Cache cache = getCache();
    final DistributionManager dm = (DistributionManager)((InternalDistributedSystem)system).getDistributionManager();

    Region<Object, Object> region = cache.createRegionFactory(getRegionShortcut()).setOffHeap(true).create(getRegionName());
    OutOfOffHeapMemoryException ooohme;
    try {
      Object value = new byte[1024];
      for (int i = 0; true; i++) {
        region.put("key-"+i, value);
      }
    } catch (OutOfOffHeapMemoryException e) {
      ooohme = e;
    }
    assertNotNull(ooohme);

    with().pollInterval(100, TimeUnit.MILLISECONDS).await().atMost(10, TimeUnit.SECONDS).until(() ->
        cache.isClosed() && !system.isConnected() && dm.isClosed());

    // wait for cache instance to be nulled out
    with().pollInterval(100, TimeUnit.MILLISECONDS).await().atMost(10, TimeUnit.SECONDS).until(()->
        GemFireCacheImpl.getInstance() == null && InternalDistributedSystem.getAnyInstance() == null);

    assertNull(GemFireCacheImpl.getInstance());
    
    // verify system was closed out due to OutOfOffHeapMemoryException
    assertFalse(system.isConnected());
    InternalDistributedSystem ids = (InternalDistributedSystem)system;
    try {
      ids.getDistributionManager();
      fail("InternalDistributedSystem.getDistributionManager() should throw DistributedSystemDisconnectedException");
    } catch (DistributedSystemDisconnectedException expected) {
      assertRootCause(expected, OutOfOffHeapMemoryException.class);
    }

    // verify dm was closed out due to OutOfOffHeapMemoryException
    assertTrue(dm.isClosed());
    try {
      dm.throwIfDistributionStopped();
      fail("DistributionManager.throwIfDistributionStopped() should throw DistributedSystemDisconnectedException");
    } catch (DistributedSystemDisconnectedException expected) {
      assertRootCause(expected, OutOfOffHeapMemoryException.class);
    }
    
    // verify cache was closed out due to OutOfOffHeapMemoryException
    assertTrue(cache.isClosed());
    try {
      cache.getCancelCriterion().checkCancelInProgress(null);
      fail("GemFireCacheImpl.getCancelCriterion().checkCancelInProgress should throw DistributedSystemDisconnectedException");
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
      throw new AssertionError("Throwable does not contain expected root cause " + expected, throwable);
    }
  }
  
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
          
          final Region<Object, Object> region = OutOfOffHeapMemoryDUnitTest.cache.get().createRegionFactory(shortcut).setOffHeap(true).create(name);
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

          final int countMembersPlusLocator = vmCount+1; // +1 for locator
          final int countOtherMembers = vmCount-1; // -1 one for self
          
          assertEquals(countMembersPlusLocator, ((InternalDistributedSystem)OutOfOffHeapMemoryDUnitTest
              .system.get()).getDistributionManager().getDistributionManagerIds().size());
          assertEquals(countOtherMembers, ((DistributedRegion)OutOfOffHeapMemoryDUnitTest
              .cache.get().getRegion(name)).getDistributionAdvisor().getNumProfiles());
        }
      });
    }
    
    // perform puts in bigger member until smaller member goes OOOHME
    Host.getHost(0).getVM(biggerVM).invoke(new SerializableRunnable() {
      public void run() {
        final long TIME_LIMIT = 30 * 1000;
        final StopWatch stopWatch = new StopWatch(true);
        
        int countOtherMembers = vmCount-1; // -1 for self
        final int countOtherMembersMinusSmaller = vmCount-1-1; // -1 for self, -1 for smallerVM 

        final Region<Object, Object> region = OutOfOffHeapMemoryDUnitTest.cache.get().getRegion(name);
        for (int i = 0; countOtherMembers > countOtherMembersMinusSmaller; i++) {
          region.put("key-"+i, new byte[1024]);
          countOtherMembers = ((DistributedRegion)OutOfOffHeapMemoryDUnitTest
              .cache.get().getRegion(name)).getDistributionAdvisor().getNumProfiles();
          assertTrue("puts failed to push member out of off-heap memory within time limit", stopWatch.elapsedTimeMillis() < TIME_LIMIT);
        }
        assertEquals("Member did not depart from OutOfOffHeapMemory", countOtherMembersMinusSmaller, countOtherMembers);
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
          final int countMembersPlusLocator = vmCount+1-1; // +1 for locator, -1 for OOOHME member
          final int countOtherMembers = vmCount-1-1; // -1 for self, -1 for OOOHME member

          with().pollInterval(10, TimeUnit.MILLISECONDS).await().atMost(30, TimeUnit.SECONDS).until(numDistributionManagers(), equalTo(countMembersPlusLocator));
          with().pollInterval(10, TimeUnit.MILLISECONDS).await().atMost(30, TimeUnit.SECONDS).until(numProfiles(), equalTo(countOtherMembers));

        }

        private Callable<Integer> numProfiles() {
          return () -> {
            DistributedRegion dr = (DistributedRegion)OutOfOffHeapMemoryDUnitTest.cache.get().getRegion(name);
            return dr.getDistributionAdvisor().getNumProfiles();
          };
        }

        private Callable<Integer> numDistributionManagers() {
          return () -> {
            InternalDistributedSystem ids = (InternalDistributedSystem)OutOfOffHeapMemoryDUnitTest.system.get();
            return ids.getDistributionManager().getDistributionManagerIds().size();
          };
        }
      });
    }
  }

}
