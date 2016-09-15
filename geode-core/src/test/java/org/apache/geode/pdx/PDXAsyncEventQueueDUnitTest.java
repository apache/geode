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
package org.apache.geode.pdx;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.List;
import java.util.Properties;

import org.junit.Ignore;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;

@Category(DistributedTest.class)
public class PDXAsyncEventQueueDUnitTest extends JUnit4CacheTestCase {

  public PDXAsyncEventQueueDUnitTest() {
    super();
  }
  
  /**
   * Test that an async queue doesn't require a persistent PDX
   * type registry.
   */
  
  @Test
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
  @Test
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
