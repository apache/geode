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
package com.gemstone.gemfire.internal.cache.wan.misc;

import java.util.Set;

import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.CacheObserverAdapter;
import com.gemstone.gemfire.internal.cache.CacheObserverHolder;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class ShutdownAllPersistentGatewaySenderDUnitTest extends WANTestBase {
  private static final long MAX_WAIT = 70000;

  private static final int NUM_KEYS = 1000;

  public ShutdownAllPersistentGatewaySenderDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    addExpectedException("Cache is being closed by ShutdownAll");
  }

  private static final long serialVersionUID = 1L;

  public void testGatewaySender() throws Exception {
    addExpectedException("Cache is shutting down");

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm2.invoke(WANTestBase.class, "createReceiverAfterCache",
        new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
        new Object[] { testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
        new Object[] { testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 400, false, false, null, true });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
        new Object[] { testName + "_PR", "ln", 1, 100, isOffHeap() });

    // set the CacheObserver to block the ShutdownAll
    SerializableRunnable waitAtShutdownAll = new SerializableRunnable() {
      @Override
      public void run() {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
        CacheObserverHolder.setInstance(new CacheObserverAdapter() {
          @Override
          public void beforeShutdownAll() {
            final Region region = cache.getRegion(testName + "_PR");
            waitForCriterion(new WaitCriterion() {
              @Override
              public boolean done() {
                return region.size() >= 2;
              }

              @Override
              public String description() {
                return "Wait for wan to have processed several events";
              }
            }, 30000, 100, true);
          }
        });
      }
    };
    vm2.invoke(waitAtShutdownAll);
    vm3.invoke(waitAtShutdownAll);
    
    AsyncInvocation vm4_future = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", NUM_KEYS });

    // ShutdownAll will be suspended at observer, so puts will continue
    AsyncInvocation future = shutDownAllMembers(vm2, 2, MAX_WAIT);
    future.join(MAX_WAIT);

    // now restart vm1 with gatewayHub
    getLogWriter().info("restart in VM2");
    vm2.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    AsyncInvocation vm3_future = vm3.invokeAsync(WANTestBase.class,
        "createPersistentPartitionedRegion", new Object[] { testName + "_PR",
            "ln", 1, 100, isOffHeap() });
    vm2.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
        new Object[] { testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm3_future.join(MAX_WAIT);

    vm3.invoke(new SerializableRunnable() {
      public void run() {
        final Region region = cache.getRegion(testName + "_PR");
        cache.getLogger().info(
            "vm1's region size before restart gatewayhub is " + region.size());
      }
    });
    vm2.invoke(WANTestBase.class, "createReceiverAfterCache",
        new Object[] { nyPort });

    // wait for vm0 to finish its work
    vm4_future.join(MAX_WAIT);
    vm4.invoke(new SerializableRunnable() {
      public void run() {
        Region region = cache.getRegion(testName + "_PR");
        assertEquals(NUM_KEYS, region.size());
      }
    });

    // verify the other side (vm1)'s entries received from gateway
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        final Region region = cache.getRegion(testName + "_PR");

        cache.getLogger().info(
            "vm1's region size after restart gatewayhub is " + region.size());
        waitForCriterion(new WaitCriterion() {
          public boolean done() {
            Object lastvalue = region.get(NUM_KEYS - 1);
            if (lastvalue != null && lastvalue.equals(NUM_KEYS - 1)) {
              region.getCache().getLogger().info(
                  "Last key has arrived, its value is " + lastvalue
                      + ", end of wait.");
              return true;
            }
            else
              return (region.size() == NUM_KEYS);
          }

          public String description() {
            return "Waiting for destination region to reach size: " + NUM_KEYS
                + ", current is " + region.size();
          }
        }, MAX_WAIT, 100, true);
        assertEquals(NUM_KEYS, region.size());
      }
    });

  }

  private AsyncInvocation shutDownAllMembers(VM vm, final int expnum, final long timeout) {
      AsyncInvocation future = vm.invokeAsync(new SerializableRunnable("Shutdown all the members") {

      public void run() {
        DistributedSystemConfig config;
        AdminDistributedSystemImpl adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(cache
              .getDistributedSystem(), "");
          adminDS = (AdminDistributedSystemImpl)AdminDistributedSystemFactory
              .getDistributedSystem(config);
          adminDS.connect();
          Set members = adminDS.shutDownAllMembers(timeout);
          int num = members == null ? 0 : members.size();
          assertEquals(expnum, num);
        }
        catch (AdminException e) {
          throw new RuntimeException(e);
        }
        finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
    return future;
  }

}
