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

import org.junit.Ignore;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DSClock;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

public class ClientServerTimeSyncDUnitTest extends CacheTestCase {

  public ClientServerTimeSyncDUnitTest(String name) {
    super(name);
  }

  @Ignore("Bug 52327")
  public void DISABLED_testClientTimeAdvances() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // Server
    VM vm1 = host.getVM(1); // Client

    final String regionName = "testRegion";
    final long TEST_OFFSET = 10000;

    ClientCache cache = null;
    
    try {

      final int serverPort = (Integer)vm0.invoke(new SerializableCallable("Start server with a region") {
        
        @Override
        public Object call() {
          Cache cache = getCache();
          cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
          getLogWriter().info("Done creating region, now creating CacheServer");
          CacheServer server = null;
          try {
            server = cache.addCacheServer();
            server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
            server.start();
          } catch (IOException e) {
            fail("Starting cache server failed.", e);
          }
  
          // now set an artificial time offset for the test
          system.getClock().setCacheTimeOffset(null, TEST_OFFSET, true);
          
          getLogWriter().info("Done creating and starting CacheServer on port " + server.getPort());
          return server.getPort();
        }
      });
      
      final String hostName = getServerHostName(vm0.getHost());
  
      // Start client with proxy region and register interest
        
      disconnectFromDS();
      Properties props = new Properties();
      props.setProperty("locators", "");
      props = getSystem(props).getProperties();
      cache = new ClientCacheFactory(props).setPoolSubscriptionEnabled(true)
          .addPoolServer(hostName, serverPort)
          .setPoolPingInterval(5000)
          .create();
      Region proxyRegion = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);
  
      proxyRegion.registerInterestRegex(".*");
  
      proxyRegion.put("testkey", "testValue1");
          
      final DSClock clock = ((GemFireCacheImpl)cache).getSystem().getClock();
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          long clientTimeOffset = clock.getCacheTimeOffset();
          getLogWriter().info("Client node's new time offset is: " + clientTimeOffset);
          return clientTimeOffset >= TEST_OFFSET;
        }
        public String description() {
          return "Waiting for cacheTimeOffset to be non-zero.  PingOp should have set it to something";
        }
      };
      waitForCriterion(wc, 60000, 1000, true);
    } finally {
      cache.close();
      vm1.invoke(CacheTestCase.class, "disconnectFromDS");
    }
  }
  
  public void testNothing() {
    // place-holder to keep dunit runner from barfing
  }

  @Ignore("not yet implemented")
  public void DISABLED_testClientTimeSlowsDown() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // Server
    VM vm1 = host.getVM(1); // Client

    final String regionName = "testRegion";
    final long TEST_OFFSET = 10000;

    ClientCache cache = null;
    
    try {

      final int serverPort = (Integer)vm0.invoke(new SerializableCallable("Start server with a region") {
        
        @Override
        public Object call() {
          Cache cache = getCache();
          cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
          getLogWriter().info("Done creating region, now creating CacheServer");
          CacheServer server = null;
          try {
            server = cache.addCacheServer();
            server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
            server.start();
          } catch (IOException e) {
            fail("Starting cache server failed.", e);
          }
  
          // now set an artificial time offset for the test
          system.getClock().setCacheTimeOffset(null, -TEST_OFFSET, true);
          
          getLogWriter().info("Done creating and starting CacheServer on port " + server.getPort());
          return server.getPort();
        }
      });
      
      pause((int)TEST_OFFSET);  // let cacheTimeMillis consume the time offset
      
      final String hostName = getServerHostName(vm0.getHost());
  
      // Start client with proxy region and register interest
        
      disconnectFromDS();
      Properties props = new Properties();
      props.setProperty("locators", "");
      props = getSystem(props).getProperties();
      cache = new ClientCacheFactory(props).setPoolSubscriptionEnabled(true)
          .addPoolServer(hostName, serverPort)
          .setPoolPingInterval(5000)
          .create();
      Region proxyRegion = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);
  
      proxyRegion.registerInterestRegex(".*");
  
      proxyRegion.put("testkey", "testValue1");
          
      final DSClock clock = ((GemFireCacheImpl)cache).getSystem().getClock();
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          long clientTimeOffset = clock.getCacheTimeOffset();
          getLogWriter().info("Client node's new time offset is: " + clientTimeOffset);
          if (clientTimeOffset >= 0) {
            return false;
          }
          long cacheTime = clock.cacheTimeMillis();
          return Math.abs(System.currentTimeMillis() - (cacheTime - clientTimeOffset)) < 5;
        }
        public String description() {
          return "Waiting for cacheTimeOffset to be negative and cacheTimeMillis to stabilize";
        }
      };
      waitForCriterion(wc, 60000, 1000, true);
    } finally {
      cache.close();
      vm1.invoke(CacheTestCase.class, "disconnectFromDS");
    }
  }

}
