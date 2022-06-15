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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.UpdateOperation.UpdateMessage;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;

/**
 * Tests interrupting gemfire threads during a put operation to see what happens
 *
 */

public class InterruptsDUnitTest implements Serializable {

  @Rule
  public CacheRule cacheRule = new CacheRule();

  private static volatile Thread puttingThread;
  private static final long MAX_WAIT = 60 * 1000;
  private static final AtomicBoolean doInterrupt = new AtomicBoolean(false);

  private VM vm0;
  private VM vm1;

  public InterruptsDUnitTest() {
    super();
  }

  @Before
  public void setup() {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
  }

  @After
  public final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(() -> {
      puttingThread = null;
      return null;
    });
  }

  /**
   * A simple test case that we are actually persisting with a PR.
   *
   */
  @Test
  public void testDRPutWithInterrupt() throws Throwable {

    createCache(vm0);
    createCache(vm1);
    createRegion(vm0);

    // put some data in vm0
    createData(vm0, 0, 10, "a");

    vm1.invoke(() -> {
      cacheRule.getSystem().disconnect();
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof UpdateMessage
              && ((UpdateMessage) message).regionPath.contains("region")
              && doInterrupt.compareAndSet(true, false)) {
            vm0.invoke(() -> {
              puttingThread.interrupt();
              return null;
            });
            DistributionMessageObserver.setInstance(null);
          }
        }

      });
      return null;
    });

    createCache(vm1);
    createRegion(vm1);

    AsyncInvocation<Void> async0 = vm0.invokeAsync(() -> {
      puttingThread = Thread.currentThread();
      Region<Object, Object> region = cacheRule.getCache().getRegion("region");
      long value = 0;
      long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_WAIT);
      while (!Thread.currentThread().isInterrupted()) {
        region.put(0, value);
        if (System.nanoTime() > end) {
          fail("Did not get interrupted in 60 seconds");
        }
      }
      return null;
    });

    vm1.invoke(() -> {
      doInterrupt.set(true);
      return null;
    });

    async0.getResult();

    Object value0 = checkCacheAndGetValue(vm0);
    Object value1 = checkCacheAndGetValue(vm1);

    assertEquals(value0, value1);

  }

  private Object checkCacheAndGetValue(VM vm) {
    return vm.invoke(() -> {
      assertFalse(cacheRule.getCache().isClosed());
      Region<Object, Object> region = cacheRule.getCache().getRegion("region");
      return region.get(0);
    });
  }

  protected void createCache(VM vm) {
    vm.invoke(() -> {
      cacheRule.createCache(getDistributedSystemProperties());
      return null;
    });
  }

  private void createRegion(VM vm) {
    vm.invoke(() -> {
      cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region");
      return null;
    });
  }

  private void createData(VM vm, final int start, final int end, final String value) {
    vm.invoke(() -> {
      Region<Object, Object> region = cacheRule.getCache().getRegion("region");
      for (int i = start; i < end; i++) {
        region.put(i, value);
      }
      return null;
    });
  }

  public Properties getDistributedSystemProperties() {
    return new Properties();
  }
}
