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
package com.gemstone.gemfire.cache.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.management.MemoryThresholdsDUnitTest.Range;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.gemstone.gemfire.internal.cache.ProxyBucketRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.control.MemoryEvent;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds.MemoryState;
import com.gemstone.gemfire.internal.cache.control.OffHeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.control.OffHeapMemoryMonitor.OffHeapMemoryMonitorObserver;
import com.gemstone.gemfire.internal.cache.control.ResourceAdvisor;
import com.gemstone.gemfire.internal.cache.control.ResourceListener;
import com.gemstone.gemfire.internal.cache.control.TestMemoryThresholdListener;
import com.gemstone.gemfire.internal.cache.partitioned.RegionAdvisor;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * Tests the Off-Heap Memory thresholds of {@link ResourceManager}
 * 
 * @author David Hoots
 * @since 9.0
 */
public class MemoryThresholdsOffHeapDUnitTest extends ClientServerTestCase {
  private static final long serialVersionUID = -684231183212051910L;

  final String expectedEx = LocalizedStrings.MemoryMonitor_MEMBER_ABOVE_CRITICAL_THRESHOLD.getRawText().replaceAll("\\{[0-9]+\\}",
      ".*?");
  final String addExpectedExString = "<ExpectedException action=add>" + this.expectedEx + "</ExpectedException>";
  final String removeExpectedExString = "<ExpectedException action=remove>" + this.expectedEx + "</ExpectedException>";
  final String expectedBelow = LocalizedStrings.MemoryMonitor_MEMBER_BELOW_CRITICAL_THRESHOLD.getRawText().replaceAll(
      "\\{[0-9]+\\}", ".*?");
  final String addExpectedBelow = "<ExpectedException action=add>" + this.expectedBelow + "</ExpectedException>";
  final String removeExpectedBelow = "<ExpectedException action=remove>" + this.expectedBelow + "</ExpectedException>";

  public MemoryThresholdsOffHeapDUnitTest(String name) {
    super(name);
  }
  
  

  @Override
  public void setUp() throws Exception {
    IgnoredException.addIgnoredException(expectedEx);
    IgnoredException.addIgnoredException(expectedBelow);
  }



  @Override
  protected void preTearDownClientServerTestCase() throws Exception {
    Invoke.invokeInEveryVM(this.resetResourceManager);
  }

  private SerializableCallable resetResourceManager = new SerializableCallable() {
    public Object call() throws Exception {
      InternalResourceManager irm = ((GemFireCacheImpl)getCache()).getResourceManager();
      Set<ResourceListener> listeners = irm.getResourceListeners(ResourceType.OFFHEAP_MEMORY);
      Iterator<ResourceListener> it = listeners.iterator();
      while (it.hasNext()) {
        ResourceListener<MemoryEvent> l = it.next();
        if (l instanceof TestMemoryThresholdListener) {
          ((TestMemoryThresholdListener)l).resetThresholdCalls();
        }
      }
      return null;
    }
  };

  /**
   * Make sure appropriate events are delivered when moving between states.
   * 
   * @throws Exception
   */
  public void testEventDelivery() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    final int port2 = ports[1];
    final String regionName = "offHeapEventDelivery";

    startCacheServer(server1, port1, 0f, 0f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);
    startCacheServer(server2, port2, 70f, 90f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);

    registerTestMemoryThresholdListener(server1);
    registerTestMemoryThresholdListener(server2);
    
    // NORMAL -> EVICTION
    setUsageAboveEvictionThreshold(server2, regionName);
    verifyListenerValue(server1, MemoryState.EVICTION, 1, true);
    verifyListenerValue(server2, MemoryState.EVICTION, 1, true);
    
    // EVICTION -> CRITICAL
    setUsageAboveCriticalThreshold(server2, regionName);
    verifyListenerValue(server1, MemoryState.CRITICAL, 1, true);
    verifyListenerValue(server2, MemoryState.CRITICAL, 1, true);
    verifyListenerValue(server1, MemoryState.EVICTION, 2, true);
    verifyListenerValue(server2, MemoryState.EVICTION, 2, true);
    
    // CRITICAL -> CRITICAL
    server2.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws Exception {
        getCache().getLogger().fine(MemoryThresholdsOffHeapDUnitTest.this.addExpectedExString);
        getRootRegion().getSubregion(regionName).destroy("oh3");
        getCache().getLogger().fine(MemoryThresholdsOffHeapDUnitTest.this.removeExpectedExString);
        return null;
      }
    });
    verifyListenerValue(server1, MemoryState.CRITICAL, 1, true);
    verifyListenerValue(server2, MemoryState.CRITICAL, 1, true);
    verifyListenerValue(server1, MemoryState.EVICTION, 2, true);
    verifyListenerValue(server2, MemoryState.EVICTION, 2, true);
    
    // CRITICAL -> EVICTION
    server2.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws Exception {
        getCache().getLogger().fine(MemoryThresholdsOffHeapDUnitTest.this.addExpectedBelow);
        getRootRegion().getSubregion(regionName).destroy("oh2");
        getCache().getLogger().fine(MemoryThresholdsOffHeapDUnitTest.this.removeExpectedBelow);
        return null;
      }
    });
    verifyListenerValue(server1, MemoryState.EVICTION, 3, true);
    verifyListenerValue(server2, MemoryState.EVICTION, 3, true);

    // EVICTION -> EVICTION
    server2.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws Exception {
        getRootRegion().getSubregion(regionName).put("oh6", new byte[20480]);
        return null;
      }
    });
    verifyListenerValue(server1, MemoryState.EVICTION, 3, true);
    verifyListenerValue(server2, MemoryState.EVICTION, 3, true);

    // EVICTION -> NORMAL
    server2.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws Exception {
        getRootRegion().getSubregion(regionName).destroy("oh4");
        return null;
      }
    });

    verifyListenerValue(server1, MemoryState.CRITICAL, 1, true);
    verifyListenerValue(server1, MemoryState.EVICTION, 3, true);
    verifyListenerValue(server1, MemoryState.NORMAL, 1, true);
    
    verifyListenerValue(server2, MemoryState.CRITICAL, 1, true);
    verifyListenerValue(server2, MemoryState.EVICTION, 3, true);
    verifyListenerValue(server2, MemoryState.NORMAL, 1, true);
  }
  
  /**
   * test that disabling threshold does not cause remote event and
   * remote DISABLED events are delivered
   * @throws Exception
   */
  public void testDisabledThresholds() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    
    final String regionName = "offHeapDisabledThresholds";

    //set port to 0 in-order for system to pickup a random port.
    startCacheServer(server1, 0, 0f, 0f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);
    startCacheServer(server2, 0, 0f, 0f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);

    registerTestMemoryThresholdListener(server1);
    registerTestMemoryThresholdListener(server2);
    
    setUsageAboveEvictionThreshold(server1, regionName);
    verifyListenerValue(server1, MemoryState.EVICTION, 0, true);
    verifyListenerValue(server2, MemoryState.EVICTION, 0, true);

    setThresholds(server1, 70f, 0f);
    verifyListenerValue(server1, MemoryState.EVICTION, 1, true);
    verifyListenerValue(server2, MemoryState.EVICTION, 1, true);

    setUsageAboveCriticalThreshold(server1, regionName);
    verifyListenerValue(server1, MemoryState.CRITICAL, 0, true);
    verifyListenerValue(server2, MemoryState.CRITICAL, 0, true);

    setThresholds(server1, 0f, 0f);
    verifyListenerValue(server1, MemoryState.EVICTION_DISABLED, 1, true);
    verifyListenerValue(server2, MemoryState.EVICTION_DISABLED, 1, true);

    setThresholds(server1, 0f, 90f);
    verifyListenerValue(server1, MemoryState.CRITICAL, 1, true);
    verifyListenerValue(server2, MemoryState.CRITICAL, 1, true);

    //verify that stats on server2 are not changed by events on server1
    server2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        InternalResourceManager irm = ((GemFireCacheImpl)getCache()).getResourceManager();
        assertEquals(0, irm.getStats().getOffHeapEvictionStartEvents());
        assertEquals(0, irm.getStats().getOffHeapCriticalEvents());
        assertEquals(0, irm.getStats().getOffHeapCriticalThreshold());
        assertEquals(0, irm.getStats().getOffHeapEvictionThreshold());
        return null;
      }
    });
  }

  private void setUsageAboveCriticalThreshold(final VM vm, final String regionName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getCache().getLoggerI18n().fine(addExpectedExString);
        Region region = getRootRegion().getSubregion(regionName);
        if (!region.containsKey("oh1")) {
          region.put("oh5", new byte[954204]);
        } else {
          region.put("oh5", new byte[122880]);
        }
        getCache().getLoggerI18n().fine(removeExpectedExString);
        return null;
      }
    });
  }

  private void setUsageAboveEvictionThreshold(final VM vm, final String regionName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getCache().getLoggerI18n().fine(addExpectedBelow);
        Region region = getRootRegion().getSubregion(regionName);
        region.put("oh1", new byte[245760]);
        region.put("oh2", new byte[184320]);
        region.put("oh3", new byte[33488]);
        region.put("oh4", new byte[378160]);
        getCache().getLoggerI18n().fine(removeExpectedBelow);
        return null;
      }
    });
  }
  
  private void setUsageBelowEviction(final VM vm, final String regionName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getCache().getLoggerI18n().fine(addExpectedBelow);
        Region region = getRootRegion().getSubregion(regionName);
        region.remove("oh1");
        region.remove("oh2");
        region.remove("oh3");
        region.remove("oh4");
        region.remove("oh5");
        getCache().getLoggerI18n().fine(removeExpectedBelow);
        return null;
      }
    });
  }
  
  private void setThresholds(VM server, final float evictionThreshold,
      final float criticalThreshold) {
    
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        ResourceManager irm = getCache().getResourceManager();
        irm.setCriticalOffHeapPercentage(criticalThreshold);
        irm.setEvictionOffHeapPercentage(evictionThreshold);
        return null;
      }
    });
  }
  
  /**
   * test that puts in a client are rejected when a remote VM crosses
   * critical threshold
   * @throws Exception
   */
  public void testDistributedRegionRemoteClientPutRejection() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client = host.getVM(2);

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int port1 = ports[0];
    final int port2 = ports[1];
    final String regionName = "offHeapDRRemoteClientPutReject";

    startCacheServer(server1, port1, 0f, 0f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);
    startCacheServer(server2, port2, 0f, 90f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);

    startClient(client, server1, port1, regionName);

    registerTestMemoryThresholdListener(server1);
    registerTestMemoryThresholdListener(server2);

    doPuts(client, regionName, false/*catchRejectedException*/,
        false/*catchLowMemoryException*/);
    doPutAlls(client, regionName, false/*catchRejectedException*/,
        false/*catchLowMemoryException*/, Range.DEFAULT);

    //make server2 critical
    setUsageAboveCriticalThreshold(server2, regionName);

    verifyListenerValue(server1, MemoryState.CRITICAL, 1, true);
    verifyListenerValue(server2, MemoryState.CRITICAL, 1, true);

    //make sure that client puts are rejected
    doPuts(client, regionName, true/*catchRejectedException*/,
        false/*catchLowMemoryException*/);
    doPutAlls(client, regionName, true/*catchRejectedException*/,
        false/*catchLowMemoryException*/, new Range(Range.DEFAULT, Range.DEFAULT.width()+1));
    
    setUsageBelowEviction(server2, regionName);
  }

  public void testDistributedRegionRemotePutRejectionLocalDestroy() throws Exception {
    doDistributedRegionRemotePutRejection(true, false);
  }
  
  public void testDistributedRegionRemotePutRejectionCacheClose() throws Exception {
    doDistributedRegionRemotePutRejection(false, true);
  }
  
  public void testDistributedRegionRemotePutRejectionBelowThreshold() throws Exception {
    doDistributedRegionRemotePutRejection(false, false);
  }
  
  public void testGettersAndSetters() {
    getSystem(getOffHeapProperties());
    ResourceManager rm = getCache().getResourceManager();
    assertEquals(0.0f, rm.getCriticalOffHeapPercentage());
    assertEquals(0.0f, rm.getEvictionOffHeapPercentage());
    
    rm.setEvictionOffHeapPercentage(50);
    rm.setCriticalOffHeapPercentage(90);
    
    // verify
    assertEquals(50.0f, rm.getEvictionOffHeapPercentage());
    assertEquals(90.0f, rm.getCriticalOffHeapPercentage());
    
    getCache().createRegionFactory(RegionShortcut.REPLICATE_HEAP_LRU).create(getName());
    
    assertEquals(50.0f, rm.getEvictionOffHeapPercentage());
    assertEquals(90.0f, rm.getCriticalOffHeapPercentage());
  }
  
  /**
   * test that puts in a server are rejected when a remote VM crosses
   * critical threshold
   * @throws Exception
   */
  private void doDistributedRegionRemotePutRejection(boolean localDestroy, boolean cacheClose) throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);

    final String regionName = "offHeapDRRemotePutRejection";

    //set port to 0 in-order for system to pickup a random port.
    startCacheServer(server1, 0, 0f, 0f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);
    startCacheServer(server2, 0, 0f, 90f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);

    registerTestMemoryThresholdListener(server1);
    registerTestMemoryThresholdListener(server2);

    doPuts(server1, regionName, false/*catchRejectedException*/,
        false/*catchLowMemoryException*/);
    doPutAlls(server1, regionName, false/*catchRejectedException*/,
        false/*catchLowMemoryException*/, Range.DEFAULT);

    //make server2 critical
    setUsageAboveCriticalThreshold(server2, regionName);

    verifyListenerValue(server1, MemoryState.CRITICAL, 1, true);
    verifyListenerValue(server2, MemoryState.CRITICAL, 1, true);

    //make sure that local server1 puts are rejected
    doPuts(server1, regionName, false/*catchRejectedException*/,
        true/*catchLowMemoryException*/);
    Range r1 = new Range(Range.DEFAULT, Range.DEFAULT.width()+1);
    doPutAlls(server1, regionName, false/*catchRejectedException*/,
        true/*catchLowMemoryException*/, r1);

    if (localDestroy) {
    //local destroy the region on sick member
    server2.invoke(new SerializableCallable("local destroy") {
      public Object call() throws Exception {
        Region r = getRootRegion().getSubregion(regionName);
        r.localDestroyRegion();
        return null;
      }
    });
    } else if (cacheClose) {
      server2.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          getCache().close();
          return null;
        }
      });
    } else {
      setUsageBelowEviction(server2, regionName);
    }
    
    //wait for remote region destroyed message to be processed
    server1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        WaitCriterion wc = new WaitCriterion() {
          public String description() {
            return "remote localRegionDestroyed message not received";
          }
          public boolean done() {
            DistributedRegion dr = (DistributedRegion)getRootRegion().
                                                getSubregion(regionName);
            return dr.getMemoryThresholdReachedMembers().size() == 0;
          }
        };
        Wait.waitForCriterion(wc, 10000, 10, true);
        return null;
      }
    });

    //make sure puts succeed
    doPuts(server1, regionName, false/*catchRejectedException*/,
        false/*catchLowMemoryException*/);
    Range r2 = new Range(r1, r1.width()+1);
    doPutAlls(server1, regionName, false/*catchRejectedException*/,
        false/*catchLowMemoryException*/, r2);
  }

  /**
   * Test that DistributedRegion cacheLoade and netLoad are passed through to the 
   * calling thread if the local VM is in a critical state.  Once the VM has moved
   * to a safe state then test that they are allowed.
   * @throws Exception
   */
  public void testDRLoadRejection() throws Exception {
    final Host host = Host.getHost(0);
    final VM replicate1 = host.getVM(1);
    final VM replicate2 = host.getVM(2);
    final String rName = getUniqueName();
    
    // Make sure the desired VMs will have a fresh DS.
    AsyncInvocation d1 = replicate1.invokeAsync(DistributedTestCase.class, "disconnectFromDS");
    AsyncInvocation d2 = replicate2.invokeAsync(DistributedTestCase.class, "disconnectFromDS");
    d1.join();
    assertFalse(d1.exceptionOccurred());
    d2.join();
    assertFalse(d2.exceptionOccurred());
    CacheSerializableRunnable establishConnectivity = new CacheSerializableRunnable("establishcConnectivity") {
      @SuppressWarnings("synthetic-access")
      @Override
      public void run2() throws CacheException {
        getSystem(getOffHeapProperties());
      }
    };
    replicate1.invoke(establishConnectivity);
    replicate2.invoke(establishConnectivity);
    
    CacheSerializableRunnable createRegion = new CacheSerializableRunnable("create DistributedRegion") {
      @Override
      public void run2() throws CacheException {
        // Assert some level of connectivity
        InternalDistributedSystem ds = getSystem(getOffHeapProperties());
        assertTrue(ds.getDistributionManager().getNormalDistributionManagerIds().size() >= 1);

        InternalResourceManager irm = (InternalResourceManager)getCache().getResourceManager();
        irm.setCriticalOffHeapPercentage(90f);
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setOffHeap(true);
        Region region = getCache().createRegion(rName, af.create());
      }
    };
    replicate1.invoke(createRegion);
    replicate2.invoke(createRegion);
    
    replicate1.invoke(addExpectedException);
    replicate2.invoke(addExpectedException);
    
    final Integer expected = (Integer)replicate1.invoke(new SerializableCallable("test Local DistributedRegion Load") {
      public Object call() throws Exception {
        final DistributedRegion r = (DistributedRegion) getCache().getRegion(rName);
        AttributesMutator<Integer, String> am = r.getAttributesMutator();
        am.setCacheLoader(new CacheLoader<Integer, String>() {
          final AtomicInteger numLoaderInvocations = new AtomicInteger(0);
          public String load(LoaderHelper<Integer, String> helper) throws CacheLoaderException {
            Integer expectedInvocations = (Integer)helper.getArgument();
            final int actualInvocations = this.numLoaderInvocations.getAndIncrement();
            if (expectedInvocations.intValue() != actualInvocations) {
              throw new CacheLoaderException("Expected " + expectedInvocations 
                  + " invocations, actual is " + actualInvocations);
            }
            return helper.getKey().toString();
          }
          public void close() {}
        });

        int expectedInvocations = 0;
        final OffHeapMemoryMonitor ohmm = ((InternalResourceManager)getCache().getResourceManager()).getOffHeapMonitor();
        assertFalse(ohmm.getState().isCritical());
        {
          Integer k = new Integer(1);
          assertEquals(k.toString(), r.get(k, new Integer(expectedInvocations++)));
        }

        r.put("oh1", new byte[838860]);
        r.put("oh3", new byte[157287]);
        
        WaitCriterion wc = new WaitCriterion() {
          public String description() {
            return "expected region " + r + " to set memoryThreshold";
          }
          public boolean done() {
            return r.memoryThresholdReached.get();
          }
        };
        Wait.waitForCriterion(wc, 30*1000, 10, true);
        {
          Integer k = new Integer(2); 
          assertEquals(k.toString(), r.get(k, new Integer(expectedInvocations++)));
        }
        
        r.destroy("oh3");
        wc = new WaitCriterion() {
          public String description() {
            return "expected region " + r + " to unset memoryThreshold";
          }
          public boolean done() {
            return !r.memoryThresholdReached.get();
          }
        };
        Wait.waitForCriterion(wc, 30*1000, 10, true);
        {
          Integer k = new Integer(3);
          assertEquals(k.toString(), r.get(k, new Integer(expectedInvocations++)));
        }
        return new Integer(expectedInvocations);
      }
    });

    final CacheSerializableRunnable validateData1 = new CacheSerializableRunnable("Validate data 1") {
      @Override
      public void run2() throws CacheException {
        Region r = getCache().getRegion(rName);
        Integer i1 = new Integer(1);
        assertTrue(r.containsKey(i1));
        assertNotNull(r.getEntry(i1));
        Integer i2 = new Integer(2);
        assertFalse(r.containsKey(i2));
        assertNull(r.getEntry(i2));
        Integer i3 = new Integer(3);
        assertTrue(r.containsKey(i3));
        assertNotNull(r.getEntry(i3));
      }
    };
    replicate1.invoke(validateData1);
    replicate2.invoke(validateData1);
    
    replicate2.invoke(new SerializableCallable("test DistributedRegion netLoad") {
      public Object call() throws Exception {
        final DistributedRegion r = (DistributedRegion) getCache().getRegion(rName);
        final OffHeapMemoryMonitor ohmm = ((InternalResourceManager)getCache().getResourceManager()).getOffHeapMonitor();
        assertFalse(ohmm.getState().isCritical());
        
        int expectedInvocations = expected.intValue();
        {
          Integer k = new Integer(4);
          assertEquals(k.toString(), r.get(k, new Integer(expectedInvocations++)));
        }
        
        // Place in a critical state for the next test
        r.put("oh3", new byte[157287]);
        WaitCriterion wc = new WaitCriterion() {
          public String description() {
            return "expected region " + r + " to set memoryThreshold";
          }
          public boolean done() {
            return r.memoryThresholdReached.get();
          }
        };
        Wait.waitForCriterion(wc, 30*1000, 10, true);
        {
          Integer k = new Integer(5);
          assertEquals(k.toString(), r.get(k, new Integer(expectedInvocations++)));
        }

        r.destroy("oh3");
        wc = new WaitCriterion() {
          public String description() {
            return "expected region " + r + " to unset memoryThreshold";
          }
          public boolean done() {
            return !r.memoryThresholdReached.get();
          }
        };
        Wait.waitForCriterion(wc, 30*1000, 10, true);
        {
          Integer k = new Integer(6);
          assertEquals(k.toString(), r.get(k, new Integer(expectedInvocations++)));
        }
        return new Integer(expectedInvocations);
      }
    });
    
    replicate1.invoke(removeExpectedException);
    replicate2.invoke(removeExpectedException);
    
    final CacheSerializableRunnable validateData2 = new CacheSerializableRunnable("Validate data 2") {
      @Override
      public void run2() throws CacheException {
        Region<Integer, String> r = getCache().getRegion(rName);
        Integer i4 = new Integer(4);
        assertTrue(r.containsKey(i4));
        assertNotNull(r.getEntry(i4));
        Integer i5 = new Integer(5);
        assertFalse(r.containsKey(i5));
        assertNull(r.getEntry(i5));
        Integer i6 = new Integer(6);
        assertTrue(r.containsKey(i6));
        assertNotNull(r.getEntry(i6));
      }
    };
    replicate1.invoke(validateData2);
    replicate2.invoke(validateData2);
  }
  

  private SerializableRunnable addExpectedException = new SerializableRunnable
  ("addExpectedEx") {
    public void run() {
      getCache().getLoggerI18n().fine(addExpectedExString);
      getCache().getLoggerI18n().fine(addExpectedBelow);
    };
  };
  
  private SerializableRunnable removeExpectedException = new SerializableRunnable
  ("removeExpectedException") {
    public void run() {
      getCache().getLoggerI18n().fine(removeExpectedExString);
      getCache().getLoggerI18n().fine(removeExpectedBelow);
    };
  };
  
  public void testPR_RemotePutRejectionLocalDestroy() throws Exception {
    prRemotePutRejection(false, true, false);
  }

  public void testPR_RemotePutRejectionCacheClose() throws Exception {
    prRemotePutRejection(true, false, false);
  }

  public void testPR_RemotePutRejection() throws Exception {
    prRemotePutRejection(false, false, false);
  }

  public void testPR_RemotePutRejectionLocalDestroyWithTx() throws Exception {
    prRemotePutRejection(false, true, true);
  }

  public void testPR_RemotePutRejectionCacheCloseWithTx() throws Exception {
    prRemotePutRejection(true, false, true);
  }

  public void testPR_RemotePutRejectionWithTx() throws Exception {
    prRemotePutRejection(false, false, true);
  }

  private void prRemotePutRejection(boolean cacheClose, boolean localDestroy, final boolean useTx) throws Exception {
    final Host host = Host.getHost(0);
    final VM accessor = host.getVM(0);
    final VM servers[] = new VM[3];
    servers[0] = host.getVM(1);
    servers[1] = host.getVM(2);
    servers[2] = host.getVM(3);

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    final String regionName = "offHeapPRRemotePutRejection";
    final int redundancy = 1;

    startCacheServer(servers[0], ports[0], 0f, 90f,
        regionName, true/*createPR*/, false/*notifyBySubscription*/, redundancy);
    startCacheServer(servers[1], ports[1], 0f, 90f,
        regionName, true/*createPR*/, false/*notifyBySubscription*/, redundancy);
    startCacheServer(servers[2], ports[2], 0f, 90f,
        regionName, true/*createPR*/, false/*notifyBySubscription*/, redundancy);
    accessor.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(getOffHeapProperties());
        getCache();
        AttributesFactory factory = new AttributesFactory();        
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy);
        paf.setLocalMaxMemory(0);
        paf.setTotalNumBuckets(11);
        factory.setPartitionAttributes(paf.create());
        factory.setOffHeap(true);
        createRegion(regionName, factory.create());
        return null;
      }
    });
    
    doPuts(accessor, regionName, false, false);
    final Range r1 = Range.DEFAULT;
    doPutAlls(accessor, regionName, false, false, r1);

    servers[0].invoke(addExpectedException);
    servers[1].invoke(addExpectedException);
    servers[2].invoke(addExpectedException);
    setUsageAboveCriticalThreshold(servers[0], regionName);
    
    final Set<InternalDistributedMember> criticalMembers = (Set) servers[0].invoke(new SerializableCallable() {
      public Object call() throws Exception {
        final PartitionedRegion pr = (PartitionedRegion)getRootRegion().getSubregion(regionName);
        final int hashKey = PartitionedRegionHelper.getHashKey(pr, null, "oh5", null, null);
        return pr.getRegionAdvisor().getBucketOwners(hashKey);
      }
    });
    
    accessor.invoke(new SerializableCallable() {
      public Object call() throws Exception {     
        final PartitionedRegion pr = (PartitionedRegion)getRootRegion().getSubregion(regionName);
        WaitCriterion wc = new WaitCriterion() {
          public String description() {
            return "remote bucket not marked sick";
          }
          public boolean done() {
            boolean keyFoundOnSickMember = false;
            boolean caughtException = false;
            for (int i=0; i<20; i++) {
              Integer key = Integer.valueOf(i);
              int hKey = PartitionedRegionHelper.getHashKey(pr, null, key, null, null);
              Set<InternalDistributedMember> owners = pr.getRegionAdvisor().getBucketOwners(hKey);
              final boolean hasCriticalOwners = owners.removeAll(criticalMembers);
              if (hasCriticalOwners) {
                keyFoundOnSickMember = true;
                try {
                  if (useTx) getCache().getCacheTransactionManager().begin();
                  pr.getCache().getLogger().fine("SWAP:putting in tx:"+useTx);
                  pr.put(key, "value");
                  if (useTx) getCache().getCacheTransactionManager().commit();
                } catch (LowMemoryException ex) {
                  caughtException = true;
                  if (useTx) getCache().getCacheTransactionManager().rollback();
                }
              } else {
                //puts on healthy member should continue
                pr.put(key, "value");
              }
            }
            return keyFoundOnSickMember && caughtException;
          }
        };
        Wait.waitForCriterion(wc, 10000, 10, true);
        return null;
      }
    });

    {
      Range r2 = new Range(r1, r1.width()+1);
      doPutAlls(accessor, regionName, false, true, r2);
    }
    
    // Find all VMs that have a critical region
    SerializableCallable getMyId = new SerializableCallable() {
      public Object call() throws Exception {        
        return ((GemFireCacheImpl)getCache()).getMyId();
      }
    };
    final Set<VM> criticalServers = new HashSet<VM>();
    for (final VM server : servers) {
      DistributedMember member = (DistributedMember) server.invoke(getMyId);
      if (criticalMembers.contains(member)) {
        criticalServers.add(server);
      }
    }
    
    if (localDestroy) {
    //local destroy the region on sick members
      for (final VM vm : criticalServers) {
        vm.invoke(new SerializableCallable("local destroy sick member") {
          public Object call() throws Exception {
            Region r = getRootRegion().getSubregion(regionName);
            LogWriterUtils.getLogWriter().info("PRLocalDestroy");
            r.localDestroyRegion();
            return null;
          }
        });
      }
    } else if (cacheClose) {
      // close cache on sick members
      for (final VM vm : criticalServers) {
        vm.invoke(new SerializableCallable("close cache sick member") {
          public Object call() throws Exception {
            getCache().close();
            return null;
          }
        });
      }
    } else {
      setUsageBelowEviction(servers[0], regionName);
      servers[0].invoke(removeExpectedException);
      servers[1].invoke(removeExpectedException);
      servers[2].invoke(removeExpectedException);
    }
    
    //do put all in a loop to allow distribution of message
    accessor.invoke(new SerializableCallable("Put in a loop") {
      public Object call() throws Exception {
        final Region r = getRootRegion().getSubregion(regionName);
        WaitCriterion wc = new WaitCriterion() {
          public String description() {            
            return "pr should have gone un-critical";
          }
          public boolean done() {
            boolean done = true;
            for (int i=0; i<20; i++) {
              try {
                r.put(i,"value");
              } catch (LowMemoryException e) {
                //expected
                done = false;
              }
            }            
            return done;
          }
        };
        Wait.waitForCriterion(wc, 10000, 10, true);
        return null;
      }
    });
    doPutAlls(accessor, regionName, false, false, r1);
  }

  /**
   * Test that a Partitioned Region loader invocation is rejected
   * if the VM with the bucket is in a critical state.
   * @throws Exception
   */
  public void testPRLoadRejection() throws Exception {
    final Host host = Host.getHost(0);
    final VM accessor = host.getVM(1);
    final VM ds1 = host.getVM(2);
    final String rName = getUniqueName();

    // Make sure the desired VMs will have a fresh DS.
    AsyncInvocation d0 = accessor.invokeAsync(DistributedTestCase.class, "disconnectFromDS");
    AsyncInvocation d1 = ds1.invokeAsync(DistributedTestCase.class, "disconnectFromDS");
    d0.join();
    assertFalse(d0.exceptionOccurred());
    d1.join();
    assertFalse(d1.exceptionOccurred());
    CacheSerializableRunnable establishConnectivity = new CacheSerializableRunnable("establishcConnectivity") {
      @Override
      public void run2() throws CacheException { getSystem();  }
    };
    ds1.invoke(establishConnectivity);
    accessor.invoke(establishConnectivity);

    ds1.invoke(createPR(rName, false));
    accessor.invoke(createPR(rName, true));
    
    final AtomicInteger expectedInvocations = new AtomicInteger(0);

    Integer ex = (Integer) accessor.invoke(new SerializableCallable("Invoke loader from accessor, non-critical") {
      public Object call() throws Exception {
        Region<Integer, String> r = getCache().getRegion(rName);
        Integer k = new Integer(1);
        Integer expectedInvocations0 = new Integer(expectedInvocations.getAndIncrement());
        assertEquals(k.toString(), r.get(k, expectedInvocations0)); // should load for new key
        assertTrue(r.containsKey(k));
        Integer expectedInvocations1 = new Integer(expectedInvocations.get());
        assertEquals(k.toString(), r.get(k, expectedInvocations1)); // no load
        assertEquals(k.toString(), r.get(k, expectedInvocations1)); // no load
        return expectedInvocations1;
      }
    });
    expectedInvocations.set(ex.intValue());

    ex = (Integer)ds1.invoke(new SerializableCallable("Invoke loader from datastore, non-critical") {
      public Object call() throws Exception {
        Region<Integer, String> r = getCache().getRegion(rName);
        Integer k = new Integer(2);
        Integer expectedInvocations1 = new Integer(expectedInvocations.getAndIncrement());
        assertEquals(k.toString(), r.get(k, expectedInvocations1)); // should load for new key
        assertTrue(r.containsKey(k));
        Integer expectedInvocations2 = new Integer(expectedInvocations.get());
        assertEquals(k.toString(), r.get(k, expectedInvocations2)); // no load
        assertEquals(k.toString(), r.get(k, expectedInvocations2)); // no load
        String oldVal = r.remove(k);
        assertFalse(r.containsKey(k));
        assertEquals(k.toString(), oldVal);
        return expectedInvocations2;
      }
    });
    expectedInvocations.set(ex.intValue());

    accessor.invoke(addExpectedException);
    ds1.invoke(addExpectedException);

    ex = (Integer)ds1.invoke(new SerializableCallable("Set critical state, assert local load behavior") {
      public Object call() throws Exception {
        final OffHeapMemoryMonitor ohmm = ((InternalResourceManager)getCache().getResourceManager()).getOffHeapMonitor();
        final PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(rName);
        final RegionAdvisor advisor = pr.getRegionAdvisor();
        
        pr.put("oh1", new byte[838860]);
        pr.put("oh3", new byte[157287]);
        
        WaitCriterion wc = new WaitCriterion() {
          public String description() {
            return "verify critical state";
          }
          public boolean done() {
            for (final ProxyBucketRegion bucket : advisor.getProxyBucketArray()) {
              if (bucket.isBucketSick()) {
                return true;
              }
            }
            return false;
          }
        };
        Wait.waitForCriterion(wc, 30*1000, 10, true);
        
        final Integer k = new Integer(2); // reload with same key again and again
        final Integer expectedInvocations3 = new Integer(expectedInvocations.getAndIncrement());
        assertEquals(k.toString(), pr.get(k, expectedInvocations3)); // load
        assertFalse(pr.containsKey(k));
        Integer expectedInvocations4 = new Integer(expectedInvocations.getAndIncrement());
        assertEquals(k.toString(), pr.get(k, expectedInvocations4)); // load
        assertFalse(pr.containsKey(k));
        Integer expectedInvocations5 = new Integer(expectedInvocations.get());
        assertEquals(k.toString(), pr.get(k, expectedInvocations5)); // load
        assertFalse(pr.containsKey(k));
        return expectedInvocations5;
      }
    });
    expectedInvocations.set(ex.intValue());

    ex = (Integer)accessor.invoke(new SerializableCallable("During critical state on datastore, assert accesor load behavior") {
      public Object call() throws Exception {
        final Integer k = new Integer(2);  // reload with same key again and again
        Integer expectedInvocations6 = new Integer(expectedInvocations.incrementAndGet());
        Region<Integer, String> r = getCache().getRegion(rName);
        assertEquals(k.toString(), r.get(k, expectedInvocations6)); // load
        assertFalse(r.containsKey(k));
        Integer expectedInvocations7 = new Integer(expectedInvocations.incrementAndGet());
        assertEquals(k.toString(), r.get(k, expectedInvocations7)); // load
        assertFalse(r.containsKey(k));
        return expectedInvocations7;
      }
    });
    expectedInvocations.set(ex.intValue());
    
    ex = (Integer)ds1.invoke(new SerializableCallable("Set safe state on datastore, assert local load behavior") {
      public Object call() throws Exception {
        final PartitionedRegion r = (PartitionedRegion) getCache().getRegion(rName);
        
        r.destroy("oh3");
        WaitCriterion wc = new WaitCriterion() {
          public String description() {
            return "verify critical state";
          }
          public boolean done() {
            return !r.memoryThresholdReached.get();
          }
        };
        Wait.waitForCriterion(wc, 30*1000, 10, true);
        
        Integer k = new Integer(3); // same key as previously used, this time is should stick
        Integer expectedInvocations8 = new Integer(expectedInvocations.incrementAndGet());
        assertEquals(k.toString(), r.get(k, expectedInvocations8)); // last load for 3
        assertTrue(r.containsKey(k));
        return expectedInvocations8;
      }
    });
    expectedInvocations.set(ex.intValue());

    accessor.invoke(new SerializableCallable("Data store in safe state, assert load behavior, accessor sets critical state, assert load behavior") {
      public Object call() throws Exception {
        final OffHeapMemoryMonitor ohmm = ((InternalResourceManager)getCache().getResourceManager()).getOffHeapMonitor();
        assertFalse(ohmm.getState().isCritical());
        Integer k = new Integer(4);
        Integer expectedInvocations9 = new Integer(expectedInvocations.incrementAndGet());
        final PartitionedRegion r = (PartitionedRegion) getCache().getRegion(rName);
        assertEquals(k.toString(), r.get(k, expectedInvocations9)); // load for 4
        assertTrue(r.containsKey(k));
        assertEquals(k.toString(), r.get(k, expectedInvocations9)); // no load

        // Go critical in accessor
        r.put("oh3", new byte[157287]);
        
        WaitCriterion wc = new WaitCriterion() {
          public String description() {
            return "verify critical state";
          }
          public boolean done() {
            return r.memoryThresholdReached.get();
          }
        };

        k = new Integer(5);
        Integer expectedInvocations10 = new Integer(expectedInvocations.incrementAndGet());
        assertEquals(k.toString(), r.get(k, expectedInvocations10)); // load for key 5
        assertTrue(r.containsKey(k));
        assertEquals(k.toString(), r.get(k, expectedInvocations10)); // no load
        
        // Clean up critical state
        r.destroy("oh3");
        wc = new WaitCriterion() {
          public String description() {
            return "verify critical state";
          }
          public boolean done() {
            return !ohmm.getState().isCritical();
          }
        };
        return expectedInvocations10;
      }
    });

    accessor.invoke(removeExpectedException);
    ds1.invoke(removeExpectedException);
  }
  
  private CacheSerializableRunnable createPR(final String rName, final boolean accessor) {
    return new CacheSerializableRunnable("create PR accessor") {
    @Override
    public void run2() throws CacheException {
      // Assert some level of connectivity
      getSystem(getOffHeapProperties());      
      InternalResourceManager irm = (InternalResourceManager)getCache().getResourceManager();
      irm.setCriticalOffHeapPercentage(90f);
      AttributesFactory<Integer, String> af = new AttributesFactory<Integer, String>();
      if (!accessor) {
        af.setCacheLoader(new CacheLoader<Integer, String>() {
          final AtomicInteger numLoaderInvocations = new AtomicInteger(0);
          public String load(LoaderHelper<Integer, String> helper) throws CacheLoaderException {
            Integer expectedInvocations = (Integer)helper.getArgument();
            final int actualInvocations = this.numLoaderInvocations.getAndIncrement();
            if (expectedInvocations.intValue() != actualInvocations) {
              throw new CacheLoaderException("Expected " + expectedInvocations 
                  + " invocations, actual is " + actualInvocations);
            }
            return helper.getKey().toString();
          }
          public void close() {}
        });

        af.setPartitionAttributes(new PartitionAttributesFactory().create());
      } else {
        af.setPartitionAttributes(new PartitionAttributesFactory().setLocalMaxMemory(0).create());
      }
      af.setOffHeap(true);
      getCache().createRegion(rName, af.create());
    }
  };
  }
  
  /**
   * Test that LocalRegion cache Loads are not stored in the Region
   * if the VM is in a critical state, then test that they are allowed
   * once the VM is no longer critical
   * @throws Exception
   */
  public void testLRLoadRejection() throws Exception {
    final Host host = Host.getHost(0);
    final VM vm = host.getVM(2);
    final String rName = getUniqueName();

    vm.invoke(DistributedTestCase.class, "disconnectFromDS");
    
    vm.invoke(new CacheSerializableRunnable("test LocalRegion load passthrough when critical") {
      @Override
      public void run2() throws CacheException {
        getSystem(getOffHeapProperties());
        InternalResourceManager irm = (InternalResourceManager)getCache().getResourceManager();
        final OffHeapMemoryMonitor ohmm = irm.getOffHeapMonitor();
        irm.setCriticalOffHeapPercentage(90f);
        AttributesFactory<Integer, String> af = new AttributesFactory<Integer, String>();
        af.setScope(Scope.LOCAL);
        af.setOffHeap(true);
        final AtomicInteger numLoaderInvocations = new AtomicInteger(0);
        af.setCacheLoader(new CacheLoader<Integer, String>() {
          public String load(LoaderHelper<Integer, String> helper)
          throws CacheLoaderException {
            numLoaderInvocations.incrementAndGet();
            return helper.getKey().toString();
          }
          public void close() {}
        });
        final LocalRegion r = (LocalRegion) getCache().createRegion(rName, af.create());
        
        assertFalse(ohmm.getState().isCritical());
        int expectedInvocations = 0;
        assertEquals(expectedInvocations++, numLoaderInvocations.get());
        {
          Integer k = new Integer(1);
          assertEquals(k.toString(), r.get(k));
        }
        assertEquals(expectedInvocations++, numLoaderInvocations.get());
        expectedInvocations++; expectedInvocations++;
        r.getAll(createRanges(10, 12));
        assertEquals(expectedInvocations++, numLoaderInvocations.get());
        
        getCache().getLoggerI18n().fine(addExpectedExString);
        r.put("oh1", new byte[838860]);
        r.put("oh3", new byte[157287]);
        getCache().getLoggerI18n().fine(removeExpectedExString);
        WaitCriterion wc = new WaitCriterion() {
          public String description() {
            return "expected region " + r + " to set memoryThresholdReached";
          }
          public boolean done() {
            return r.memoryThresholdReached.get();
          }
        };
        Wait.waitForCriterion(wc, 30*1000, 10, true);
        { 
          Integer k = new Integer(2);
          assertEquals(k.toString(), r.get(k));
        }
        assertEquals(expectedInvocations++, numLoaderInvocations.get());
        expectedInvocations++; expectedInvocations++;
        r.getAll(createRanges(13, 15));
        assertEquals(expectedInvocations++, numLoaderInvocations.get());
        
        getCache().getLoggerI18n().fine(addExpectedBelow);
        r.destroy("oh3");
        getCache().getLoggerI18n().fine(removeExpectedBelow);
        wc = new WaitCriterion() {
          public String description() {
            return "expected region " + r + " to unset memoryThresholdReached";
          }
          public boolean done() {
            return !r.memoryThresholdReached.get();
          }
        };
        Wait.waitForCriterion(wc, 30*1000, 10, true);
        
        {
          Integer k = new Integer(3);
          assertEquals(k.toString(), r.get(k));
        }
        assertEquals(expectedInvocations++, numLoaderInvocations.get());
        expectedInvocations++; expectedInvocations++;
        r.getAll(createRanges(16, 18));
        assertEquals(expectedInvocations, numLoaderInvocations.get());

        // Do extra validation that the entry doesn't exist in the local region
        for (Integer i: createRanges(2, 2, 13, 15)) {
          if (r.containsKey(i)) {
            fail("Expected containsKey return false for key" + i);
          }
          if (r.getEntry(i) != null) {
            fail("Expected getEntry to return null for key" + i);
          }
        }
      }
    });
  }
  
  /** Create a list of integers consisting of the ranges defined by the provided
  * argument e.g..  createRanges(1, 4, 10, 12) means create ranges 1 through 4 and
  * 10 through 12 and should yield the list:
  * 1, 2, 3, 4, 10, 11, 12
  */ 
  public static List<Integer> createRanges(int... startEnds) {
    assert startEnds.length % 2 == 0;
    ArrayList<Integer> ret = new ArrayList<Integer>();
    for (int si=0; si<startEnds.length; si++) { 
      final int start = startEnds[si++];
      final int end = startEnds[si];
      assert end >= start;
      ret.ensureCapacity(ret.size() + ((end-start)+1));
      for (int i=start; i<=end; i++) {
        ret.add(new Integer(i));
      }
    }
    return ret;
  }
  
  public void testCleanAdvisorClose() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    final int port1 = ports[0];
    final int port2 = ports[1];
    final int port3 = ports[2];
    final String regionName = "testEventOrder";

    startCacheServer(server1, port1, 0f, 0f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);
    startCacheServer(server2, port2, 0f, 0f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);

    verifyProfiles(server1, 2);
    verifyProfiles(server2, 2);

    server2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    });

    verifyProfiles(server1, 1);

    startCacheServer(server3, port3, 0f, 0f,
        regionName, false/*createPR*/, false/*notifyBySubscription*/, 0);

    verifyProfiles(server1, 2);
    verifyProfiles(server3, 2);
  }
  
  public void testPRClientPutRejection() throws Exception {
    doClientServerTest("parRegReject", true/*createPR*/);
  }

  public void testDistributedRegionClientPutRejection() throws Exception {
    doClientServerTest("distrReject", false/*createPR*/);
  }
  
  private void doPuts(VM vm, final String regionName,
      final boolean catchServerException, final boolean catchLowMemoryException) {

    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion().getSubregion(regionName);
        try {
          r.put(Integer.valueOf(0), "value-1");
          if (catchServerException || catchLowMemoryException) {
            fail("An expected ResourceException was not thrown");
          }
        } catch (ServerOperationException ex) {
          if (!catchServerException) {
            Assert.fail("Unexpected exception: ", ex);
          }
          if (!(ex.getCause() instanceof LowMemoryException)) {
            Assert.fail("Unexpected exception: ", ex);
          }
        } catch (LowMemoryException low) {
          if (!catchLowMemoryException) {
            Assert.fail("Unexpected exception: ", low);
          }
        }
        return null;
      }
    });
  }

  private void doPutAlls(VM vm, final String regionName,
      final boolean catchServerException, final boolean catchLowMemoryException, 
      final Range rng) {

    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion().getSubregion(regionName);
        Map<Integer, String> temp = new HashMap<Integer, String>();
        for (int i=rng.start; i<rng.end; i++) {
          Integer k = Integer.valueOf(i);
          temp.put(k, "value-"+i);
        }
        try {
          r.putAll(temp);
          if (catchServerException || catchLowMemoryException) {
            fail("An expected ResourceException was not thrown");
          }
          for (Map.Entry<Integer, String> me: temp.entrySet()) {
            assertEquals(me.getValue(), r.get(me.getKey()));
          }
        } catch (ServerOperationException ex) {
          if (!catchServerException) {
            Assert.fail("Unexpected exception: ", ex);
          }
          if (!(ex.getCause() instanceof LowMemoryException)) {
            Assert.fail("Unexpected exception: ", ex);
          }
          for(Integer me: temp.keySet()) {
            assertFalse("Key " + me + " should not exist", r.containsKey(me));
          }
        } catch (LowMemoryException low) {
          LogWriterUtils.getLogWriter().info("Caught LowMemoryException", low);
          if (!catchLowMemoryException) {
            Assert.fail("Unexpected exception: ", low);
          }
          for(Integer me: temp.keySet()) {
            assertFalse("Key " + me + " should not exist", r.containsKey(me));
          }
        }
        return null;
      }
    });
  }
  
  private void doClientServerTest(final String regionName, boolean createPR)
      throws Exception {
    //create region on the server
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final Object bigKey = -1;
    final Object smallKey = -2;

    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    startCacheServer(server, port, 0f, 90f,
        regionName, createPR, false, 0);
    startClient(client, server, port, regionName);
    doPuts(client, regionName, false/*catchServerException*/,
        false/*catchLowMemoryException*/);
    doPutAlls(client, regionName, false/*catchServerException*/,
        false/*catchLowMemoryException*/, Range.DEFAULT);

    
    //make the region sick in the server
    final long bytesUsedAfterSmallKey = (long)server.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        InternalResourceManager irm = ((GemFireCacheImpl)getCache()).getResourceManager();
        final OffHeapMemoryMonitor ohm = irm.getOffHeapMonitor();
        assertTrue(ohm.getState().isNormal());
        getCache().getLoggerI18n().fine(addExpectedExString);
        final LocalRegion r = (LocalRegion) getRootRegion().getSubregion(regionName);
        final long bytesUsedAfterSmallKey;
        {
          OffHeapMemoryMonitorObserverImpl _testHook = new OffHeapMemoryMonitorObserverImpl();
          ohm.testHook = _testHook;
          try {
            r.put(smallKey, "1234567890");
            bytesUsedAfterSmallKey = _testHook.verifyBeginUpdateMemoryUsed(false);
          } finally {
            ohm.testHook = null;
          }
        }
        {
          final OffHeapMemoryMonitorObserverImpl th = new OffHeapMemoryMonitorObserverImpl();
          ohm.testHook = th;
          try {
            r.put(bigKey, new byte[943720]);
            th.verifyBeginUpdateMemoryUsed(bytesUsedAfterSmallKey + 943720 + 8, true);
            WaitCriterion waitForCritical = new WaitCriterion() {
              public boolean done() {
                return th.checkUpdateStateAndSendEventBeforeProcess(bytesUsedAfterSmallKey + 943720 + 8, MemoryState.EVICTION_DISABLED_CRITICAL);
              }
              @Override
              public String description() {
                return null;
              }
            };
            Wait.waitForCriterion(waitForCritical, 30*1000, 9, false);
            th.validateUpdateStateAndSendEventBeforeProcess(bytesUsedAfterSmallKey + 943720 + 8, MemoryState.EVICTION_DISABLED_CRITICAL);
          } finally {
            ohm.testHook = null;
          }
        }
        WaitCriterion wc;
        if (r instanceof PartitionedRegion) {
          final PartitionedRegion pr = (PartitionedRegion) r;
          final int bucketId = PartitionedRegionHelper.getHashKey(pr, null, bigKey, null, null);
          wc = new WaitCriterion() {
            @Override
            public String description() {
              return "Expected to go critical: isCritical=" + ohm.getState().isCritical();
            }

            @Override
            public boolean done() {
              if (!ohm.getState().isCritical()) return false;
              // Only done once the bucket has been marked sick
              try {
                pr.getRegionAdvisor().checkIfBucketSick(bucketId, bigKey);
                return false;
              } catch (LowMemoryException ignore) {
                return true;
              }
            }
          };
        } else {
          wc = new WaitCriterion() {
            @Override
            public String description() {
              return "Expected to go critical: isCritical=" + ohm.getState().isCritical() + " memoryThresholdReached=" + r.memoryThresholdReached.get();
            }

            @Override
            public boolean done() {
              return ohm.getState().isCritical() && r.memoryThresholdReached.get();
            }
          };
        }
        Wait.waitForCriterion(wc, 30000, 9, true);
        getCache().getLoggerI18n().fine(removeExpectedExString);
        return bytesUsedAfterSmallKey;
      }
    });

    //make sure client puts are rejected
    doPuts(client, regionName, true/*catchServerException*/,
        false/*catchLowMemoryException*/);
    doPutAlls(client, regionName, true/*catchServerException*/,
        false/*catchLowMemoryException*/, new Range(Range.DEFAULT, Range.DEFAULT.width()+1));
    
    //make the region healthy in the server
    server.invoke(new SerializableRunnable() {
      public void run() {
        InternalResourceManager irm = ((GemFireCacheImpl)getCache()).getResourceManager();
        final OffHeapMemoryMonitor ohm = irm.getOffHeapMonitor();
        assertTrue(ohm.getState().isCritical());
        getCache().getLogger().fine(MemoryThresholdsOffHeapDUnitTest.this.addExpectedBelow);
        OffHeapMemoryMonitorObserverImpl _testHook = new OffHeapMemoryMonitorObserverImpl();
        ohm.testHook = _testHook;
        try {
          getRootRegion().getSubregion(regionName).destroy(bigKey);
          _testHook.verifyBeginUpdateMemoryUsed(bytesUsedAfterSmallKey, true);
        } finally {
          ohm.testHook = null;
        }
        WaitCriterion wc = new WaitCriterion() {
          @Override
          public String description() {
            return "Expected to go normal";
          }

          @Override
          public boolean done() {
            return ohm.getState().isNormal();
          }
        };
        Wait.waitForCriterion(wc, 30000, 9, true);
        getCache().getLogger().fine(MemoryThresholdsOffHeapDUnitTest.this.removeExpectedBelow);
        return;
      }
    });
  }
  
  private static class OffHeapMemoryMonitorObserverImpl implements OffHeapMemoryMonitorObserver {
    private boolean beginUpdateMemoryUsed;
    private long beginUpdateMemoryUsed_bytesUsed;
    private boolean beginUpdateMemoryUsed_willSendEvent;
    @Override
    public synchronized void beginUpdateMemoryUsed(long bytesUsed, boolean willSendEvent) {
      beginUpdateMemoryUsed = true;
      beginUpdateMemoryUsed_bytesUsed = bytesUsed;
      beginUpdateMemoryUsed_willSendEvent = willSendEvent;
    }
    @Override
    public synchronized void afterNotifyUpdateMemoryUsed(long bytesUsed) {
    }
    @Override
    public synchronized void beginUpdateStateAndSendEvent(long bytesUsed, boolean willSendEvent) {
    }
    private boolean updateStateAndSendEventBeforeProcess;
    private long updateStateAndSendEventBeforeProcess_bytesUsed;
    private MemoryEvent updateStateAndSendEventBeforeProcess_event;
    @Override
    public synchronized void updateStateAndSendEventBeforeProcess(long bytesUsed, MemoryEvent event) {
      updateStateAndSendEventBeforeProcess = true;
      updateStateAndSendEventBeforeProcess_bytesUsed = bytesUsed;
      updateStateAndSendEventBeforeProcess_event = event;
    }
    @Override
    public synchronized void updateStateAndSendEventBeforeAbnormalProcess(long bytesUsed, MemoryEvent event) {
    }
    @Override
    public synchronized void updateStateAndSendEventIgnore(long bytesUsed, MemoryState oldState, MemoryState newState, long mostRecentBytesUsed,
        boolean deliverNextAbnormalEvent) {
    }

    public synchronized void verifyBeginUpdateMemoryUsed(long expected_bytesUsed, boolean expected_willSendEvent) {
      if (!beginUpdateMemoryUsed) {
        fail("beginUpdateMemoryUsed was not called");
      }
      assertEquals(expected_bytesUsed, beginUpdateMemoryUsed_bytesUsed);
      assertEquals(expected_willSendEvent, beginUpdateMemoryUsed_willSendEvent);
    }
    /**
     * Verify that beginUpdateMemoryUsed was called, event will be sent, and return the "bytesUsed" it recorded.
     */
    public synchronized long verifyBeginUpdateMemoryUsed(boolean expected_willSendEvent) {
      if (!beginUpdateMemoryUsed) {
        fail("beginUpdateMemoryUsed was not called");
      }
      assertEquals(expected_willSendEvent, beginUpdateMemoryUsed_willSendEvent);
      return beginUpdateMemoryUsed_bytesUsed;
    }
    public synchronized boolean checkUpdateStateAndSendEventBeforeProcess(long expected_bytesUsed, MemoryState expected_memoryState) {
      if (!updateStateAndSendEventBeforeProcess) {
        return false;
      }
      if (expected_bytesUsed != updateStateAndSendEventBeforeProcess_bytesUsed) {
        return false;
      }
      if (!expected_memoryState.equals(updateStateAndSendEventBeforeProcess_event.getState())) {
        return false;
      }
      return true;
    }
    public synchronized void validateUpdateStateAndSendEventBeforeProcess(long expected_bytesUsed, MemoryState expected_memoryState) {
      if (!updateStateAndSendEventBeforeProcess) {
        fail("updateStateAndSendEventBeforeProcess was not called");
      }
      assertEquals(expected_bytesUsed, updateStateAndSendEventBeforeProcess_bytesUsed);
      assertEquals(expected_memoryState, updateStateAndSendEventBeforeProcess_event.getState());
    }
   }
  private void registerTestMemoryThresholdListener(VM vm) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        TestMemoryThresholdListener listener = new TestMemoryThresholdListener();
        InternalResourceManager irm = ((GemFireCacheImpl)getCache()).getResourceManager();
        irm.addResourceListener(ResourceType.OFFHEAP_MEMORY, listener);
        assertTrue(irm.getResourceListeners(ResourceType.OFFHEAP_MEMORY).contains(listener));
        return null;
      }
    });
  }

  private void startCacheServer(VM server, final int port,
      final float evictionThreshold, final float criticalThreshold, final String regionName,
      final boolean createPR, final boolean notifyBySubscription, final int prRedundancy) throws Exception {

    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(getOffHeapProperties());
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();

        InternalResourceManager irm = cache.getResourceManager();
        irm.setEvictionOffHeapPercentage(evictionThreshold);
        irm.setCriticalOffHeapPercentage(criticalThreshold);

        AttributesFactory factory = new AttributesFactory();
        if (createPR) {
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(prRedundancy);
          paf.setTotalNumBuckets(11);
          factory.setPartitionAttributes(paf.create());
          factory.setOffHeap(true);
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
          factory.setOffHeap(true);
        }
        Region region = createRegion(regionName, factory.create());
        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        } else {
          assertTrue(region instanceof DistributedRegion);
        }
        CacheServer cacheServer = getCache().addCacheServer();
        cacheServer.setPort(port);
        cacheServer.setNotifyBySubscription(notifyBySubscription);
        cacheServer.start();
        return null;
      }
    });
  }
  
  private void startClient(VM client, final VM server, final int serverPort,
      final String regionName) {

    client.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(getClientProps());
        getCache();

        PoolFactory pf = PoolManager.createFactory();
        pf.addServer(NetworkUtils.getServerHostName(server.getHost()), serverPort);
        pf.create("pool1");
        
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.LOCAL);
        af.setPoolName("pool1");        
        createRegion(regionName, af.create());
        return null;
      }
    });
  }

  /**
   * Verifies that the test listener value on the given vm is what is expected
   * Note that for remote events useWaitCriterion must be true.
   * Note also that since off-heap local events are async local events must also
   * set useWaitCriterion to true.
   * 
   * @param vm
   *          the vm where verification should take place
   * @param value
   *          the expected value
   * @param useWaitCriterion
   *          must be true for both local and remote events (see GEODE-138)
   */
  private void verifyListenerValue(VM vm, final MemoryState state, final int value, final boolean useWaitCriterion) {
    vm.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws Exception {
        WaitCriterion wc = null;
        Set<ResourceListener> listeners = getGemfireCache().getResourceManager().getResourceListeners(ResourceType.OFFHEAP_MEMORY);
        TestMemoryThresholdListener tmp_listener = null;
        Iterator<ResourceListener> it = listeners.iterator();
        while (it.hasNext()) {
          ResourceListener<MemoryEvent> l = it.next();
          if (l instanceof TestMemoryThresholdListener) {
            tmp_listener = (TestMemoryThresholdListener) l;
            break;
          }
        }
        final TestMemoryThresholdListener listener = tmp_listener == null ? null : tmp_listener;
        switch (state) {
        case CRITICAL:
          if (useWaitCriterion) {
            wc = new WaitCriterion() {
              @Override
              public String description() {
                return "Remote CRITICAL assert failed " + listener.toString();
              }

              @Override
              public boolean done() {
                return value == listener.getCriticalThresholdCalls();
              }
            };
          } else {
            assertEquals(value, listener.getCriticalThresholdCalls());
          }
          break;
        case CRITICAL_DISABLED:
          if (useWaitCriterion) {
            wc = new WaitCriterion() {
              @Override
              public String description() {
                return "Remote CRITICAL_DISABLED assert failed " + listener.toString();
              }

              @Override
              public boolean done() {
                return value == listener.getCriticalDisabledCalls();
              }
            };
          } else {
            assertEquals(value, listener.getCriticalDisabledCalls());
          }
          break;
        case EVICTION:
          if (useWaitCriterion) {
            wc = new WaitCriterion() {
              @Override
              public String description() {
                return "Remote EVICTION assert failed " + listener.toString();
              }

              @Override
              public boolean done() {
                return value == listener.getEvictionThresholdCalls();
              }
            };
          } else {
            assertEquals(value, listener.getEvictionThresholdCalls());
          }
          break;
        case EVICTION_DISABLED:
          if (useWaitCriterion) {
            wc = new WaitCriterion() {
              @Override
              public String description() {
                return "Remote EVICTION_DISABLED assert failed " + listener.toString();
              }

              @Override
              public boolean done() {
                return value == listener.getEvictionDisabledCalls();
              }
            };
          } else {
            assertEquals(value, listener.getEvictionDisabledCalls());
          }
          break;
        case NORMAL:
          if (useWaitCriterion) {
            wc = new WaitCriterion() {
              @Override
              public String description() {
                return "Remote NORMAL assert failed " + listener.toString();
              }

              @Override
              public boolean done() {
                return value == listener.getNormalCalls();
              }
            };
          } else {
            assertEquals(value, listener.getNormalCalls());
          }
          break;
        default:
          throw new IllegalStateException("Unknown memory state");
        }
        if (useWaitCriterion) {
          Wait.waitForCriterion(wc, 5000, 10, true);
        }
        return null;
      }
    });
  }
  
  private void verifyProfiles(VM vm, final int numberOfProfiles) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        InternalResourceManager irm = ((GemFireCacheImpl)getCache()).getResourceManager();
        final ResourceAdvisor ra = irm.getResourceAdvisor();
        WaitCriterion wc = new WaitCriterion() {
          public String description() {
            return "verify profiles failed. Current profiles: " + ra.adviseGeneric();
          }
          public boolean done() {
            return numberOfProfiles == ra.adviseGeneric().size();
          }
        };
        Wait.waitForCriterion(wc, 10000, 10, true);
        return null;
      }
    });
  }
  
  private Properties getOffHeapProperties() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.LOCATORS_NAME, "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
    p.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");
    return p;
  }
  
  protected Properties getClientProps() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    return p;
  }
}
