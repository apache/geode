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

import java.io.IOException;

import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * A test to ensure that we do not deserialize PDX objects
 * when we shouldn't
 * 
 * This test is trying to cover all of the possible access paths.
 * 
 * @author dsmith
 *
 */
public class PdxDeserializationDUnitTest extends CacheTestCase {

  public PdxDeserializationDUnitTest(String name) {
    super(name);
  }
  
  /**
   * Test that we don't deserialize objects on a remote peer
   * when performing operations. 
   */
  public void testP2P() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    doTest(vm0, vm1);
  }
  
  /**
   * Test to make sure we don't deserialize
   * objects on a server that is a datastore 
   */
  public void testClientToDataStore() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    doTest(vm2, vm1);
  }
  
  /**
   * Test to make sure we don't deserialize
   * objects on a server that is an accessor.
   */
  public void testClientToAccessor() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm3 = host.getVM(3);
    
    doTest(vm3, vm1);
  }
  
  /**
   * Test to make sure we don't deserialize
   * objects on a client that has registered interest.
   */
  public void testAccessorToClient() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(1);
    VM vm3 = host.getVM(2);
    
    doTest(vm1, vm3);
  }
  
  
  /**
   * This test creates the following topology
   * 
   * vm0 a peer accessor for both a PR and a replicate region
   * vm1 a peer data store for a replicate region and a PR
   * vm2 a client of vm0
   * vm3 a client vm1
   * 
   * The test then performs all region operations
   * in operationVM, while deserialization is
   * prevented in disallowDeserializationVM.
   * 
   * If an operation causes a deserialization in the
   * disallow VM, this test will fail.
   * 
   * @param operationVM
   * @param disallowDeserializationVM
   */
  private void doTest(VM operationVM, VM disallowDeserializationVM) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    
    //Create an accessor
    final int port0 = (Integer) vm0.invoke(new SerializableCallable() {
      public Object call() {
        Cache cache = getCache();
        CacheServer server = createCacheServer(cache);
        cache.createRegionFactory(RegionShortcut.REPLICATE_PROXY).create("replicate");
        cache.createRegionFactory(RegionShortcut.PARTITION_PROXY).create("pr");
        cache.createRegionFactory(RegionShortcut.REPLICATE_PROXY).create("overflow_replicate");
        cache.createRegionFactory(RegionShortcut.PARTITION_PROXY).create("overflow_pr");
        
        return server.getPort();
      }
    });

    //Create a datastore
    final int port1 = (Integer) vm1.invoke(new SerializableCallable() {
      public Object call() {
        Cache cache = getCache();
        CacheServer server = createCacheServer(cache);
        
        cache.createRegionFactory(RegionShortcut.REPLICATE)
        .setCacheLoader(new TestCacheLoader())
        .create("replicate");
        cache.createRegionFactory(RegionShortcut.PARTITION)
        .setCacheLoader(new TestCacheLoader())
        .create("pr");
        
        cache.createDiskStoreFactory()
        .setDiskDirs(getDiskDirs())
        .setMaxOplogSize(1)
        .create("store");
        
        
        //these regions will test that faulting in an object
        //from disk doesn't cause an issue.
        cache.createRegionFactory(RegionShortcut.REPLICATE_OVERFLOW)
        .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
        .setDiskStoreName("store")
        .setCacheLoader(new TestCacheLoader())
        .create("overflow_replicate");
        cache.createRegionFactory(RegionShortcut.PARTITION_OVERFLOW)
        .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
        .setDiskStoreName("store")
        .setCacheLoader(new TestCacheLoader())
        .create("overflow_pr");
        
        return server.getPort();
      }
    });
    
    
    //create a client connected to the accessor
    vm2.invoke(new SerializableCallable() {
      
      public Object call() throws Exception {
        createClient(port0);
        return null;
      }
    });

    //create a client connected to the datastore
    vm3.invoke(new SerializableCallable() {
      
      public Object call() throws Exception {
        createClient(port1);
        return null;
      }
    });
    
    
    //Disallow deserialization
    disallowDeserializationVM.invoke(new SerializableRunnable() {
      
      public void run() {
        TestSerializable.throwExceptionOnDeserialization = true;
      }
    });
    
    
    //perform operations in the target VM
    try {
    
    operationVM.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        doOperations(cache.getRegion("replicate"));
        doOperations(cache.getRegion("pr"));
        doOperations(cache.getRegion("overflow_replicate"));
        doOperations(cache.getRegion("overflow_pr"));
      }
    });
    
    } finally {
      //Ok, now allow deserialization.
      disallowDeserializationVM.invoke(new SerializableRunnable() {

        public void run() {
          TestSerializable.throwExceptionOnDeserialization = false;
        }
      });
    }

    //Sanity Check to make sure the values not in some weird form
    //on the actual datastore.
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        checkValues(cache.getRegion("replicate"));
        checkValues(cache.getRegion("pr"));
        checkValues(cache.getRegion("overflow_replicate"));
        checkValues(cache.getRegion("overflow_pr"));
      }
    });
    
    //Make sure the clients receive keys they have registered interest in
    checkRegisterInterestValues(vm2);
    checkRegisterInterestValues(vm3);
  }

  private void checkRegisterInterestValues(VM vm2) {
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        checkClientValue(cache.getRegion("replicate"));
        checkClientValue(cache.getRegion("pr"));
        checkClientValue(cache.getRegion("overflow_replicate"));
        checkClientValue(cache.getRegion("overflow_pr"));
      }
    });
  }
  
  protected void checkClientValue(final Region<Object, Object> region) {
    //Because register interest is asynchronous, we need to wait for the value to arrive.
    Wait.waitForCriterion(new WaitCriterion() {
      
      public boolean done() {
        return region.get("A") != null;
      }
      
      public String description() {
        return "Client region never received value for key A";
      }
    }, 30000, 100, true);
    assertEquals(TestSerializable.class, region.get("A").getClass());
    
    //do a register interest which will download the value
    region.registerInterest("B", InterestResultPolicy.KEYS_VALUES);
    assertEquals(TestSerializable.class, region.get("B").getClass());
  }

  private void doOperations(Region<Object, Object> region) {
    
    //Do a put and a get
    region.put("A", new TestSerializable());
    assertEquals(TestSerializable.class, region.get("A").getClass());
    
    //Do a cache load
    assertEquals(TestSerializable.class, region.get("B").getClass());
    
    //Make sure the cache load is in the right object form
    assertEquals(TestSerializable.class, region.get("B").getClass());
    
    //If we're a client region, try a register interest
    if(region.getAttributes().getPoolName() != null) {
      region.registerInterest(".*", InterestResultPolicy.KEYS_VALUES);
    }
    
    //Do a query
    try {
      SelectResults queryResults = (SelectResults) getCache().getQueryService().newQuery("select * from " + region.getFullPath()).execute();
      for(Object result : queryResults.asList()) {
        assertEquals(TestSerializable.class, result.getClass());
      }
      
    } catch (Exception e) {
      Assert.fail("got exception from query", e);
    }
    

    //TODO Transactions don't work
//    CacheTransactionManager txManager = getCache().getCacheTransactionManager();
//    //Test puts and get in a transaction
//    txManager.begin();
//    region.put("C", new TestSerializable());
//    assertEquals(TestSerializable.class, region.get("C").getClass());
//    txManager.commit();
//    
//    txManager.begin();
//    assertEquals(TestSerializable.class, region.get("C").getClass());
//    txManager.commit();
//    
//    
//    //Test cache load in a transaction
//    txManager.begin();
//    assertEquals(TestSerializable.class, region.get("D").getClass());
//    txManager.commit();
//    
//    txManager.begin();
//    assertEquals(TestSerializable.class, region.get("D").getClass());
//    txManager.commit();
  }
  
  private void checkValues(Region<Object, Object> region) {
    assertEquals(TestSerializable.class, region.get("A").getClass());
    assertEquals(TestSerializable.class, region.get("B").getClass());
    //TODO Transactions don't work
//    assertEquals(TestSerializable.class, region.get("C").getClass());
//    assertEquals(TestSerializable.class, region.get("D").getClass());
  }

  
  private CacheServer createCacheServer(Cache cache) {
    CacheServer server = cache.addCacheServer();
    server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    try {
      server.start();
    } catch (IOException e) {
      Assert.fail("got exception", e);
    }
    return server;
  }
  
  private void createClient(final int port0) {
    ClientCacheFactory cf = new ClientCacheFactory();
    cf.addPoolServer("localhost", port0);
    cf.setPoolSubscriptionEnabled(true);
    ClientCache cache = getClientCache(cf);
    Region replicate = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("replicate");
    Region pr = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("pr");
    cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("overflow_replicate");
    cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("overflow_pr");
    
    //Register interest in a key
    replicate.registerInterest("A", InterestResultPolicy.KEYS_VALUES);
    pr.registerInterest("A", InterestResultPolicy.KEYS_VALUES);
    
  }

  public static class TestCacheLoader implements CacheLoader {
      
    public void close() {
      // TODO Auto-generated method stub

    }

    public Object load(LoaderHelper helper)
    throws CacheLoaderException {
      return new TestSerializable();
    }
  }

  /** Test PDX object. This object will fail to be
   * deserialized in the target VM.
   */
  public static class TestSerializable implements PdxSerializable {
    private static boolean throwExceptionOnDeserialization =false;

    
    public void toData(PdxWriter writer) {
      // TODO Auto-generated method stub
      
    }

    public void fromData(PdxReader reader) {
      if(throwExceptionOnDeserialization) {
        throw new SerializationException("Deserialization should not be happening in this VM");
      }
    }
    
  }
  

}
