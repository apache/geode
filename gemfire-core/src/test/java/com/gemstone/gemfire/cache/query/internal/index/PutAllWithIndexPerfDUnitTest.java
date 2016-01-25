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
package com.gemstone.gemfire.cache.query.internal.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.data.PortfolioPdx;
import com.gemstone.gemfire.cache.query.dunit.RemoteQueryDUnitTest;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * @author shobhit
 *
 */
@Category(DistributedTest.class)
@Ignore("Test was disabled by renaming to DisabledTest")
public class PutAllWithIndexPerfDUnitTest extends CacheTestCase {

  /** The port on which the bridge server was started in this VM */
  private static int bridgeServerPort;
  static long timeWithoutStructTypeIndex = 0;
  static long timeWithStructTypeIndex = 0;
  
  public PutAllWithIndexPerfDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
  }

  public void tearDown2() throws Exception {
    try {
      super.tearDown2();
    }
    finally {
      disconnectAllFromDS();
    }
  }

  public void testPutAllWithIndexes() {
    final String name = "testRegion";
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 10000;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.put("locators", "localhost["+getDUnitLocatorPort()+"]");
          Cache cache = new CacheFactory(config).create();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          cache.createRegionFactory(factory.create()).create(name);
          try {
            startBridgeServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
          //Create Index on empty region
          try {
            cache.getQueryService().createIndex("idIndex", "ID", "/"+name);
          } catch (Exception e) {
            fail("index creation failed", e);
          }
        }
      });

    // Create client region
    final int port = vm0.invokeInt(PutAllWithIndexPerfDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    vm1.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          ClientCache cache = new ClientCacheFactory().addPoolServer(host0, port).create();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(name);
        }
      });

    vm1.invoke(new CacheSerializableRunnable("putAll() test") {
      
      @Override
      public void run2() throws CacheException {
        Region exampleRegion = ClientCacheFactory.getAnyInstance().getRegion(name);

        Map warmupMap = new HashMap();
        Map data =  new HashMap();
        for(int i=0; i<10000; i++){
          Object p = new PortfolioPdx(i);
          if (i < 1000) warmupMap.put(i, p);
          data.put(i, p);
        }
        
        for (int i=0; i<10; i++) {
          exampleRegion.putAll(warmupMap);
        }
        
        long start = System.currentTimeMillis();
        for (int i=0; i<10; i++) {
          exampleRegion.putAll(data);
        }
        long end = System.currentTimeMillis();
        timeWithoutStructTypeIndex = ((end-start)/10);
        System.out.println("Total putall time for 10000 objects is: "+ ((end-start)/10) + "ms");
 
      }
    });
    
    vm0.invoke(new CacheSerializableRunnable("Remove Index and create new one") {
      
      @Override
      public void run2() throws CacheException {
        try {
          Cache cache = CacheFactory.getAnyInstance();
          cache.getQueryService().removeIndexes();
          cache.getRegion(name).clear();
          cache.getQueryService().createIndex("idIndex", "p.ID", "/"+name+" p");
        } catch (Exception e) {
          fail("index creation failed", e);
        }
      }
    });

    vm1.invoke(new CacheSerializableRunnable("putAll() test") {
      
      @Override
      public void run2() throws CacheException {
        Region exampleRegion = ClientCacheFactory.getAnyInstance().getRegion(name);
        exampleRegion.clear();
        Map warmupMap = new HashMap();
        Map data =  new HashMap();
        for(int i=0; i<10000; i++){
          Object p = new PortfolioPdx(i);
          if (i < 1000) warmupMap.put(i, p);
          data.put(i, p);
        }
        
        for (int i=0; i<10; i++) {
          exampleRegion.putAll(warmupMap);
        }
        
        long start = System.currentTimeMillis();
        for (int i=0; i<10; i++) {
          exampleRegion.putAll(data);
        }
        long end = System.currentTimeMillis();
        timeWithStructTypeIndex  = ((end-start)/10);
        System.out.println("Total putall time for 10000 objects is: "+ ((end-start)/10) + "ms");
 
      }
    });
    
    if (timeWithoutStructTypeIndex > timeWithStructTypeIndex) {
      fail("putAll took more time without struct type index than simple index");
    }
  }

  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   */
  protected void startBridgeServer(int port, boolean notifyBySubscription)
    throws IOException {

    Cache cache = CacheFactory.getAnyInstance();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  private static int getCacheServerPort() {
    return bridgeServerPort;
  }
}
