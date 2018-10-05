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
package org.apache.geode.internal.cache.wan.misc;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.admin.AdminDistributedSystemFactory;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.CacheObserverAdapter;
import org.apache.geode.internal.cache.CacheObserverHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ShutdownAllPersistentGatewaySenderDUnitTest extends WANTestBase {

  private static final long MAX_WAIT = 70000;

  private static final int NUM_KEYS = 1000;

  public ShutdownAllPersistentGatewaySenderDUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    IgnoredException.addIgnoredException("Cache is being closed by ShutdownAll");
  }

  private static final long serialVersionUID = 1L;

  @Test
  public void testGatewaySender() throws Exception {
    IgnoredException.addIgnoredException("Cache is shutting down");

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(() -> WANTestBase.createReceiver());

    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        "ln", 1, 100, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        "ln", 1, 100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.createCache(lnPort));

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 400, false, false, null, true));

    vm4.invoke(() -> WANTestBase.startSender("ln"));

    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        "ln", 1, 100, isOffHeap()));

    // set the CacheObserver to block the ShutdownAll
    SerializableRunnable waitAtShutdownAll = new SerializableRunnable() {
      @Override
      public void run() {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
        CacheObserverHolder.setInstance(new CacheObserverAdapter() {
          @Override
          public void beforeShutdownAll() {
            final Region region = cache.getRegion(getTestMethodName() + "_PR");
            GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
              @Override
              public boolean done() {
                return region.size() >= 2;
              }

              @Override
              public String description() {
                return "Wait for wan to have processed several events";
              }
            });
          }
        });
      }
    };
    vm2.invoke(waitAtShutdownAll);
    vm3.invoke(waitAtShutdownAll);

    AsyncInvocation vm4_future =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", NUM_KEYS));

    // ShutdownAll will be suspended at observer, so puts will continue
    AsyncInvocation future = shutDownAllMembers(vm2, 2, MAX_WAIT);
    future.join(MAX_WAIT);

    // now restart vm1 with gatewayHub
    LogWriterUtils.getLogWriter().info("restart in VM2");
    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));
    AsyncInvocation vm3_future = vm3.invokeAsync(() -> WANTestBase
        .createPersistentPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap()));
    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        "ln", 1, 100, isOffHeap()));
    vm3_future.join(MAX_WAIT);

    vm3.invoke(new SerializableRunnable() {
      public void run() {
        final Region region = cache.getRegion(getTestMethodName() + "_PR");
        cache.getLogger().info("vm1's region size before restart gatewayHub is " + region.size());
      }
    });
    vm2.invoke(() -> WANTestBase.createReceiver());

    // wait for vm0 to finish its work
    vm4_future.join(MAX_WAIT);
    vm4.invoke(new SerializableRunnable() {
      public void run() {
        Region region = cache.getRegion(getTestMethodName() + "_PR");
        assertEquals(NUM_KEYS, region.size());
      }
    });

    // verify the other side (vm1)'s entries received from gateway
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        final Region region = cache.getRegion(getTestMethodName() + "_PR");

        cache.getLogger().info("vm1's region size after restart gatewayHub is " + region.size());
        GeodeAwaitility.await().untilAsserted(new WaitCriterion() {
          public boolean done() {
            Object lastValue = region.get(NUM_KEYS - 1);
            if (lastValue != null && lastValue.equals(NUM_KEYS - 1)) {
              region.getCache().getLogger()
                  .info("Last key has arrived, its value is " + lastValue + ", end of wait.");
              return true;
            } else
              return (region.size() == NUM_KEYS);
          }

          public String description() {
            return "Waiting for destination region to reach size: " + NUM_KEYS + ", current is "
                + region.size();
          }
        });
        assertEquals(NUM_KEYS, region.size());
      }
    });

  }

  private AsyncInvocation shutDownAllMembers(VM vm, final int expectedNumber, final long timeout) {
    AsyncInvocation future = vm.invokeAsync(new SerializableRunnable("Shutdown all the members") {

      public void run() {
        DistributedSystemConfig config;
        AdminDistributedSystemImpl adminDS = null;
        try {
          config = AdminDistributedSystemFactory
              .defineDistributedSystem(cache.getDistributedSystem(), "");
          adminDS = (AdminDistributedSystemImpl) AdminDistributedSystemFactory
              .getDistributedSystem(config);
          adminDS.connect();
          Set members = adminDS.shutDownAllMembers(timeout);
          int num = members == null ? 0 : members.size();
          assertEquals(expectedNumber, num);
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
    return future;
  }

}
