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
package com.gemstone.gemfire.pdx;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import java.util.List;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TransactionEvent;
import com.gemstone.gemfire.cache.TransactionListener;
import com.gemstone.gemfire.cache.TransactionWriter;
import com.gemstone.gemfire.cache.TransactionWriterException;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.TestTransactionListener;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.TXEvent;
import com.gemstone.gemfire.pdx.internal.PeerTypeRegistration;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

@Category(DistributedTest.class)
public class PdxSerializableDUnitTest extends JUnit4CacheTestCase {

  public PdxSerializableDUnitTest() {
    super();
  }
  
  @Test
  public void testSimplePut() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion("testSimplePdx", af.create());
        return null;
      }
    };

    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    vm3.invoke(createRegion);
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        //Check to make sure the type region is not yet created
        Region r = getRootRegion("testSimplePdx");
        r.put(1, new SimpleClass(57, (byte) 3));
        //Ok, now the type registry should exist
        assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
        return null;
      }
    });
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
      //Ok, now the type registry should exist
        assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
        Region r = getRootRegion("testSimplePdx");
        assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
        return null;
      }
    });
    
    vm3.invoke(new SerializableCallable("check for PDX") {
      
      public Object call() throws Exception {
        assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
        return null;
      }
    });
  }
  
  @Test
  public void testTransactionCallbacksNotInvoked() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    
    SerializableCallable createRegionAndAddPoisonedListener = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion("testSimplePdx", af.create());
        addPoisonedTransactionListeners();
        return null;
      }
    };

    vm1.invoke(createRegionAndAddPoisonedListener);
    vm2.invoke(createRegionAndAddPoisonedListener);
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        //Check to make sure the type region is not yet created
        Region r = getRootRegion("testSimplePdx");
        Cache mycache = getCache();
        mycache.getCacheTransactionManager().begin();
        r.put(1, new SimpleClass(57, (byte) 3));
        mycache.getCacheTransactionManager().commit();
        //Ok, now the type registry should exist
        assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
        return null;
      }
    });
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
      //Ok, now the type registry should exist
        assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
        Region r = getRootRegion("testSimplePdx");
        assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
        return null;
      }
    });
  }
  
  @Test
  public void testPersistenceDefaultDiskStore() throws Throwable {
    
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        //Make sure the type registry is persistent
        CacheFactory cf = new CacheFactory();
        cf.setPdxPersistent(true);
        getCache(cf);
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        createRootRegion("testSimplePdx", af.create());
        return null;
      }
    };

    persistenceTest(createRegion);
  }
  
  @Test
  public void testPersistenceExplicitDiskStore() throws Throwable {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        //Make sure the type registry is persistent
        CacheFactory cf = new CacheFactory();
        cf.setPdxPersistent(true);
        cf.setPdxDiskStore("store1");
        Cache cache = getCache(cf);
        cache.createDiskStoreFactory()
          .setMaxOplogSize(1)
          .setDiskDirs(getDiskDirs())
          .create("store1");
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setDiskStoreName("store1");
        createRootRegion("testSimplePdx", af.create());
        return null;
      }
    };
    persistenceTest(createRegion);
  }


  private void persistenceTest(SerializableCallable createRegion)
      throws Throwable {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        //Check to make sure the type region is not yet created
        Region r = getRootRegion("testSimplePdx");
        r.put(1, new SimpleClass(57, (byte) 3));
        //Ok, now the type registry should exist
        assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
        return null;
      }
    });
    
    final SerializableCallable checkForObject = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
        //Ok, now the type registry should exist
        assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
        return null;
      }
    };
    
    vm2.invoke(checkForObject);
    
    SerializableCallable closeCache = new SerializableCallable() {
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    };
    
    
    //Close the cache in both VMs
    vm1.invoke(closeCache);
    vm2.invoke(closeCache);
    
    
    //Now recreate the region, recoverying from disk
    AsyncInvocation future1 = vm1.invokeAsync(createRegion);
    AsyncInvocation future2 = vm2.invokeAsync(createRegion);
    
    future1.getResult();
    future2.getResult();
    
    //Make sure we can still find and deserialize the result.
    vm1.invoke(checkForObject);
    vm2.invoke(checkForObject);
    
    //Make sure a late comer can still create the type registry.
    vm3.invoke(createRegion);
    
    vm3.invoke(checkForObject);
  }

  /**
   * add a listener and writer that will throw an exception when invoked
   * if events are for internal regions
   */
  public final void addPoisonedTransactionListeners() {
    MyTestTransactionListener listener = new MyTestTransactionListener();
    getCache().getCacheTransactionManager().addListener(listener);
    getCache().getCacheTransactionManager().setWriter(listener);
  }


  static private class MyTestTransactionListener
  implements TransactionWriter, TransactionListener {
    private MyTestTransactionListener() {
      
    }
    
    private void checkEvent(TransactionEvent event) {
      List<CacheEvent<?,?>> events = event.getEvents();
      System.out.println("MyTestTransactionListener.checkEvent: events are " + events);
      for (CacheEvent<?,?> cacheEvent: events) {
        if (((LocalRegion)cacheEvent.getRegion()).isPdxTypesRegion()) {
          throw new UnsupportedOperationException("found internal event: " + cacheEvent
              + " region=" + cacheEvent.getRegion().getName());
        }
      }
    }

    @Override
    public void beforeCommit(TransactionEvent event)
        throws TransactionWriterException {
      checkEvent(event);
    }

    @Override
    public void close() {
    }

    @Override
    public void afterCommit(TransactionEvent event) {
      checkEvent(event);
    }

    @Override
    public void afterFailedCommit(TransactionEvent event) {
      checkEvent(event);
    }

    @Override
    public void afterRollback(TransactionEvent event) {
      checkEvent(event);
    }
  }
}
