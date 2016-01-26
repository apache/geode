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
package com.gemstone.gemfire.internal.cache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.UpdateOperation.UpdateMessage;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests interrupting gemfire threads and seeing what happens
 *
 */
public class InterruptClientServerDUnitTest extends CacheTestCase {

  private static volatile Thread puttingThread;
  private static final long MAX_WAIT = 60 * 1000;
  private static AtomicBoolean doInterrupt = new AtomicBoolean(false);

  public InterruptClientServerDUnitTest(String name) {
    super(name);
  }
  
  
  
  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(new SerializableCallable() {
      
      @Override
      public Object call() throws Exception {
        puttingThread = null;
        return null;
      }
    });
    super.tearDown2();
  }
  
  public void _testLoop() throws Throwable {
    for(int i=0; i < 100; i++) {
      System.err.println("i=" +i);
      System.out.println("i=" +i);
      testClientPutWithInterrupt();
      tearDown();
      setUp();
    }
  }


  /**
   * A simple test case that we are actually
   * persisting with a PR.
   * @throws Throwable 
   */
  public void testClientPutWithInterrupt() throws Throwable {
    addExpectedException("InterruptedException");
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    createRegionAndServer(vm0, port);
    
    //put some data in vm0
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
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof UpdateMessage 
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
        while(!Thread.currentThread().isInterrupted()) {
          region.put(0, value);
          if(System.nanoTime() > end) {
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
    
//    vm0.invoke(new SerializableCallable() {
//      
//      @Override
//      public Object call() throws Exception {
//        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(MAX_WAIT);
//        while(puttingThread == null) {
//          Thread.sleep(50);
//          if(System.nanoTime() > end) {
//            fail("Putting thread not set in 60 seconds");
//          }
//        }
//        
//        puttingThread.interrupt();
//        return null;
//      }
//    });
    
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
        assertFalse(cache.isClosed());
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
        for(int i=start; i < end; i++) {
          region.put(i, value);
        }
        return null;
      }
    });
  }
}