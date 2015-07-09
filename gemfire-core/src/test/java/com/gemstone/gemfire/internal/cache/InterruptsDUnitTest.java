/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Ignore;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.cache.UpdateOperation.UpdateMessage;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * Tests interrupting gemfire threads during a put operation to see what happens
 *
 */
public class InterruptsDUnitTest extends CacheTestCase {

  private static volatile Thread puttingThread;
  private static final long MAX_WAIT = 60 * 1000;
  private static AtomicBoolean doInterrupt = new AtomicBoolean(false);

  public InterruptsDUnitTest(String name) {
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
    for(int i=0; i < 10; i++) {
      System.err.println("i=" +i);
      System.out.println("i=" +i);
      testDRPutWithInterrupt();
      tearDown();
      setUp();
    }
  }


  /**
   * A simple test case that we are actually
   * persisting with a PR.
   * @throws Throwable 
   */
  public void testDRPutWithInterrupt() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createRegion(vm0);
    
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
              vm0.invoke(interruptTask);
              DistributionMessageObserver.setInstance(null);
            }
          }
          
        });
        return null;
      }
    });
    
    createRegion(vm1);
    
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
    
    AsyncInvocation async0 = vm0.invokeAsync(doPuts);
    
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