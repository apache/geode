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

import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.UpdateOperation.UpdateMessage;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;

/**
 * Tests interrupting gemfire threads and seeing what happens
 *
 */
@Category(DistributedTest.class)
public class InterruptClientServerDUnitTest extends JUnit4CacheTestCase {

  private static volatile Thread puttingThread;
  private static final long MAX_WAIT = 60 * 1000;
  private static AtomicBoolean doInterrupt = new AtomicBoolean(false);

  public InterruptClientServerDUnitTest() {
    super();
  }



  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        puttingThread = null;
        return null;
      }
    });
  }

  /**
   * A simple test case that we are actually persisting with a PR.
   * 
   * @throws Throwable
   */
  @Test
  public void testClientPutWithInterrupt() throws Throwable {
    IgnoredException.addIgnoredException("InterruptedException");
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);

    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    createRegionAndServer(vm0, port);

    // put some data in vm0
    createData(vm0, 0, 10, "a");

    final SerializableCallable interruptTask = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        puttingThread.interrupt();
        return null;
      }
    };

    vm1.invoke(new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        disconnectFromDS();
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(DistributionManager dm, DistributionMessage message) {
            if (message instanceof UpdateMessage
                && ((UpdateMessage) message).regionPath.contains("region")
                && doInterrupt.compareAndSet(true, false)) {
              vm2.invoke(interruptTask);
              DistributionMessageObserver.setInstance(null);
            }
          }

        });
        return null;
      }
    });

    createRegion(vm1);
    createClientRegion(vm2, port);

    SerializableCallable doPuts = new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        puttingThread = Thread.currentThread();
        Region<Object, Object> region = getCache().getRegion("region");
        long value = 0;
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_WAIT);
        while (!Thread.currentThread().isInterrupted()) {
          region.put(0, value);
          if (System.nanoTime() > end) {
            fail("Did not get interrupted in 60 seconds");
          }
        }
        return null;
      }
    };

    AsyncInvocation async0 = vm2.invokeAsync(doPuts);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        doInterrupt.set(true);
        return null;
      }
    });

    // vm0.invoke(new SerializableCallable() {
    //
    // @Override
    // public Object call() throws Exception {
    // long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(MAX_WAIT);
    // while(puttingThread == null) {
    // Thread.sleep(50);
    // if(System.nanoTime() > end) {
    // fail("Putting thread not set in 60 seconds");
    // }
    // }
    //
    // puttingThread.interrupt();
    // return null;
    // }
    // });

    async0.getResult();

    Object value0 = checkCacheAndGetValue(vm0);
    Object value1 = checkCacheAndGetValue(vm1);

    assertEquals(value0, value1);

  }

  private void createClientRegion(VM vm, final int port) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        disconnectFromDS();
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer("localhost", port);
        cf.setPoolReadTimeout(60 * 2 * 1000);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
        return null;
      }
    });
  }

  private void createRegionAndServer(VM vm, final int port) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region");
        Cache cache = getCache();
        CacheServer server = cache.addCacheServer();
        server.setPort(port);
        server.start();
        return null;
      }
    });

  }

  private Object checkCacheAndGetValue(VM vm) {
    return vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        assertFalse(basicGetCache().isClosed());
        Region<Object, Object> region = getCache().getRegion("region");
        return region.get(0);
      }
    });
  }



  private void createRegion(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region");
        return null;
      }
    });
  }

  private void createData(VM vm, final int start, final int end, final String value) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<Object, Object> region = getCache().getRegion("region");
        for (int i = start; i < end; i++) {
          region.put(i, value);
        }
        return null;
      }
    });
  }
}
