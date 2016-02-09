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
package com.gemstone.gemfire.cache;

import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This tests cases where we have both 
 * a connection pool and a bridge loader.
 * @author dsmith
 *
 */
public class ConnectionPoolAndLoaderDUnitTest  extends CacheTestCase {
  
  private static int bridgeServerPort;
  protected boolean useLocator;

  public ConnectionPoolAndLoaderDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  protected final void preTearDownCacheTestCase() {
    //TODO grid. This is a hack. The next dunit test to run after
    //this one is the ConnectionPoolAutoDUnit test. That ends up calling
    //getSystem() with no arguments and expects to get a system without
    //a locator. But getSystem() is broken in that it only compares the 
    //passed in properties (an empty list) with the  current properties.
    disconnectAllFromDS();
  }
  
  /**
   * Tests that we can have both a connection pool and a bridge loader.
   * The expected order of operations for get is.
   * get from server
   * load from loader.
   * 
   * Anything that is loaded on the client is put on the server..
   * 
   */
  public void testPoolAndLoader() {
    final String regionName = this.getName();
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.invoke(new SerializableCallable() {
      public Object call() throws IOException {
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        RegionAttributes attrs = af.create();
        cache.createRegion(regionName, attrs);
        
        startBridgeServer(serverPort, true);
        return null;
      }
    });
    
    client.invoke(new SerializableCallable() {
      public Object call() {
        Cache cache = getCache();
        PoolFactory factory = PoolManager.createFactory();
        factory.addServer(NetworkUtils.getServerHostName(host), serverPort);
        factory.create("pool1");
        
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.DEFAULT);
        af.setScope(Scope.LOCAL);
        af.setPoolName("pool1");
        af.setCacheLoader(new MyCacheLoader("loaded"));
        RegionAttributes attrs = af.create();
        cache.createRegion(regionName, attrs);
        
        return null;
      }
    });
    
    client.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        region.put("a", "put-a");
        region.put("b", "put-b");
        Assert.assertEquals("loaded-c", region.get("c"));
        Assert.assertEquals("loaded-d", region.get("d"));
      }
    });
    
    server.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        Assert.assertEquals("put-a", region.get("a"));
        Assert.assertEquals("put-b", region.get("b"));
        Assert.assertEquals("loaded-c", region.get("c"));
        Assert.assertEquals("loaded-d", region.get("d"));
        region.put("e", "server-e");
      }
    });
    
    client.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        Assert.assertEquals("put-a", region.get("a"));
        Assert.assertEquals("put-b", region.get("b"));
        Assert.assertEquals("loaded-c", region.get("c"));
        Assert.assertEquals("loaded-d", region.get("d"));
        Assert.assertEquals("server-e", region.get("e"));
      }
    });
  }
  
  /**
   * Test the we can have both a connection pool
   * and a cache writer.
   * 
   * The expected order of operations for put is:
   * local writer
   * put on server
   */
  public void testPoolAndWriter() {
    final String regionName = this.getName();
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.invoke(new SerializableCallable() {
      public Object call() throws IOException {
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        RegionAttributes attrs = af.create();
        cache.createRegion(regionName, attrs);
        
        startBridgeServer(serverPort, true);
        return null;
      }
    });
    
    client.invoke(new SerializableCallable() {
      public Object call() {
        Cache cache = getCache();
        PoolFactory factory = PoolManager.createFactory();
        factory.addServer(NetworkUtils.getServerHostName(host), serverPort);
        factory.create("pool1");
        
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.DEFAULT);
        af.setScope(Scope.LOCAL);
        af.setPoolName("pool1");
        af.setCacheWriter(new MyCacheWriter());
        RegionAttributes attrs = af.create();
        cache.createRegion(regionName, attrs);
        
        return null;
      }
    });
    
    client.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        MyCacheWriter writer = (MyCacheWriter) region.getAttributes().getCacheWriter();
        region.put("a", "a");
        region.put("b", "b");
        region.put("c", "c");
        region.destroy("c");
        writer.throwException = true;
        try {
          region.put("a", "new-a");
          fail("Should have gotten a cache writer exception");
        } catch(CacheWriterException e) {
          Assert.assertEquals("beforeUpdate", e.getMessage());
        }
        try {
          region.destroy("b");
          fail("Should have gotten a cache writer exception");
        } catch(CacheWriterException e) {
          Assert.assertEquals("beforeDestroy", e.getMessage());
        }
        try {
          region.put("d", "d");
          fail("Should have gotten a cache writer exception");
        } catch(CacheWriterException e) {
          Assert.assertEquals("beforeCreate", e.getMessage());
        }
        try {
          region.clear();
          fail("Should have gotten a cache writer exception");
        } catch(CacheWriterException e) {
          Assert.assertEquals("beforeRegionClear", e.getMessage());
        }
        try {
          region.destroyRegion();
          fail("Should have gotten a cache writer exception");
        } catch(CacheWriterException e) {
          Assert.assertEquals("beforeRegionDestroy", e.getMessage());
        }
      }
    });
    
    server.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        Assert.assertEquals("a", region.get("a"));
        Assert.assertEquals("b", region.get("b"));
        Assert.assertEquals(null, region.get("c"));
        Assert.assertEquals(null, region.get("d"));
      }
    });
  }
  
  /**
   * Test that we can have a peer, a server,
   * and a bridge loader for the same region.
   * 
   * Expected order
   * 1 localCache 
   * 2 peer 
   * 3 server
   * 4 loader
   */
  public void testPoolLoadAndPeer() {
    final String regionName = this.getName();
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);

    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.invoke(new SerializableCallable() {
      public Object call() throws IOException {
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        RegionAttributes attrs = af.create();

        cache.createRegion(regionName, attrs);
        
        startBridgeServer(serverPort, true);
        return null;
      }
    });
    
    
    
    
    
    SerializableCallable createClient1 = new SerializableCallable() {
      public Object call() {
        //Make sure we get a distributed system that has the locator
        useLocator = true;
        Cache cache = getCache();
        useLocator = false;
        PoolFactory factory = PoolManager.createFactory();
        factory.addServer(NetworkUtils.getServerHostName(host), serverPort);
        factory.create("pool1");
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setPoolName("pool1");
        af.setCacheLoader(new MyCacheLoader("loaded1"));
        RegionAttributes attrs = af.create();
        cache.createRegion(regionName, attrs);
        return null;
      }
    };
    client1.invoke(createClient1);
    
    
    SerializableCallable createClient2 = new SerializableCallable() {
      public Object call() {
        //Make sure we get a distributed system that has the locator
        useLocator = true;
        Cache cache = getCache();
        useLocator = false;
        PoolFactory factory = PoolManager.createFactory();
        factory.addServer(NetworkUtils.getServerHostName(host), serverPort);
        factory.create("pool1");
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setCacheLoader(new MyCacheLoader("loaded2"));
        af.setPoolName("pool1");
        RegionAttributes attrs = af.create();
        cache.createRegion(regionName, attrs);
        return null;
      }
    };
    client2.invoke(createClient2);
    
    

    //We need to test what happens when
    //we do a load in client1 in each of these cases:
    // Case     On Server       On Client 1     On Client 2     Expected
    // a                X                                                         server
    // b                X                       X                                client1
    // c                X                       X               X               client1
    // d                X                                        X               client2
    // e                                        X                                 client1
    // f                                         X                X               client1
    // g                                                          X               client2 (loader does a netSearch)
    // h                                                                           client1 loader
    

    //Setup scenarios
    client1.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        region.put("b", "client1-b");
        region.put("c", "client1-c");
        region.put("e", "client1-e");
        region.put("f", "client1-f");
      }
    });
    
    client2.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        Assert.assertEquals("client1-c", region.get("c"));
        region.put("d", "client2-d");
        Assert.assertEquals("client1-f", region.get("f"));
        region.put("g", "client2-g");
      }
    });
    
    server.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        region.put("a", "server-a");
        region.localDestroy("e");
        region.localDestroy("f");
        region.localDestroy("g");
      }
    });
    
    //Test the scenarios
    client1.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        Assert.assertEquals("server-a", region.get("a"));
        Assert.assertEquals("client1-b", region.get("b"));
        Assert.assertEquals("client1-c", region.get("c"));
        Assert.assertEquals("client2-d", region.get("d"));
        Assert.assertEquals("client1-e", region.get("e"));
        Assert.assertEquals("client1-f", region.get("f"));
        Assert.assertEquals("client2-g", region.get("g"));
        Assert.assertEquals("loaded1-h", region.get("h"));
      }
    });
    
    server.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(regionName);
        Assert.assertEquals("server-a", region.get("a"));
        Assert.assertEquals("client1-b", region.get("b"));
        Assert.assertEquals("client1-c", region.get("c"));
        Assert.assertEquals("client2-d", region.get("d"));
        Assert.assertEquals(null, region.get("e"));
        Assert.assertEquals(null, region.get("f"));
        
        //dsmith - This result seems somewhat suspect. client1 did a net load
        //which found a value in client2, but it never propagated that result
        //to the server. After talking with Darrel we decided to keep it this
        //way for now.
        Assert.assertEquals(null, region.get("g"));
        Assert.assertEquals("loaded1-h", region.get("h"));
      }
    });
  }
  
  protected void startBridgeServer(int port, boolean notifyBySubscription)
  throws IOException {

  Cache cache = getCache();
  CacheServer bridge = cache.addCacheServer();
  bridge.setPort(port);
  bridge.setNotifyBySubscription(notifyBySubscription);
  bridge.start();
  bridgeServerPort = bridge.getPort();
}

  public Properties getDistributedSystemProperties() {
    Properties p = new Properties();
    if(!useLocator) {
      p.setProperty("locators", "");
      p.setProperty("mcast-port", "0");
    }
    return p;
  }
  
  public static class MyCacheWriter extends CacheWriterAdapter {
    protected boolean throwException = false;

    public void beforeCreate(EntryEvent event) throws CacheWriterException {
      if(throwException) {
        throw new CacheWriterException("beforeCreate");
      }
    }

    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
      if(throwException) {
        throw new CacheWriterException("beforeDestroy");
      }
    }

    public void beforeRegionClear(RegionEvent event)
        throws CacheWriterException {
      if(throwException) {
        throw new CacheWriterException("beforeRegionClear");
      }
    }

    public void beforeRegionDestroy(RegionEvent event)
        throws CacheWriterException {
      if(throwException) {
        throw new CacheWriterException("beforeRegionDestroy");
      }
    }

    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
      if(throwException) {
        throw new CacheWriterException("beforeUpdate");
      }
    }

    
  }
  
  public static class MyCacheLoader implements CacheLoader {
    
    private String message;

    public MyCacheLoader(String message) {
      this.message = message;
    }

    public Object load(LoaderHelper helper) throws CacheLoaderException {
      if(helper.getRegion().getAttributes().getScope().equals(Scope.DISTRIBUTED_ACK)) {
        System.err.println("Doing a net search for " + helper.getKey());
        Object result = helper.netSearch(false);
        System.err.println("Net search found " + result);
        if(result != null) {
          return result;
        }
      }
      
      Object key = helper.getKey();
      return message + "-" + key;
    }

    public void close() {
    }
  }
  
  

}
