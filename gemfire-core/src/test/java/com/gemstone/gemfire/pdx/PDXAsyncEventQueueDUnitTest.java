/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.pdx;

import java.util.List;
import java.util.Properties;

import org.junit.Ignore;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

public class PDXAsyncEventQueueDUnitTest extends CacheTestCase {

  public PDXAsyncEventQueueDUnitTest(String name) {
    super(name);
  }
  
  /**
   * Test that an async queue doesn't require a persistent PDX
   * type registry.
   */
  
  public void testNonPersistentPDXCreateQueueFirst() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    createSystem(vm0, false);
    createSerialAsyncEventQueue(vm0, false);
    serializeOnVM(vm0, 1);
  }
  
  /**
   * Test that an async queue doesn't require a persistent PDX
   * type registry.
   */
  public void testNonPersistentPDXCreatePDXFirst() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(1);
    
    createSystem(vm0, false);
    serializeOnVM(vm0, 1);
    createSerialAsyncEventQueue(vm0, false);
  }
  
  protected void createRegion(VM vm, final boolean useQueue) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Cache cache = getCache();
         RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
         if(useQueue) {
           rf.addAsyncEventQueueId("queue");
         }
        Region region1  =rf.create("region");
        return null;
      }
    };
    vm.invoke(createSystem);
  }
  
  protected void createSystem(VM vm, final boolean pdxPersistent) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Properties props = new Properties();
//        props.setProperty(DistributionConfig.LOCATORS_NAME, "");
        getSystem(props);
        CacheFactory cf = new CacheFactory();
        cf.setPdxPersistent(pdxPersistent);
        getCache(cf);
        return null;
      }
    }; 
    vm.invoke(createSystem);
  }

  protected void createSerialAsyncEventQueue(VM vm, final boolean persistent) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Cache cache = getCache();
        AsyncEventQueue sender = cache.createAsyncEventQueueFactory().setBatchSize(2)
        .setBatchTimeInterval(1000)
        .setBatchConflationEnabled(false)
        .setPersistent(persistent)
        .create("queue", new AsyncEventListener() {
          
          @Override
          public void close() {
            // TODO Auto-generated method stub
            
          }
          
          @Override
          public boolean processEvents(List<AsyncEvent> events) {
            //do nothing
            return true;
          }
        });
        return null;
      }
    };
    vm.invoke(createSystem);
  }
  
  private void putInRegion(VM vm, final Object key, final int value) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Cache cache = getCache();
        Region region1 = cache.getRegion("region");
        region1.put(key, new SimpleClass(value, (byte) value));
        return null;
      }
    };
    vm.invoke(createSystem);
  }
  
  private void serializeOnVM(VM vm, final int value) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        //Make sure the cache exists
        getCache();
        DataSerializer.writeObject(new SimpleClass(value, (byte) value), new HeapDataOutputStream(Version.CURRENT));
        return null;
      }
    };
    vm.invoke(createSystem);
  }
}
