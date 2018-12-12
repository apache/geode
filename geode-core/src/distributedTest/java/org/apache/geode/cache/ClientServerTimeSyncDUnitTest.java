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
package org.apache.geode.cache;

import static java.lang.Math.abs;
import static java.lang.System.currentTimeMillis;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.Wait.pause;

import java.io.IOException;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ClientServerTimeSyncDUnitTest extends JUnit4CacheTestCase {

  public ClientServerTimeSyncDUnitTest() {
    super();
  }

  @Ignore("Bug 52327")
  @Test
  public void testClientTimeAdvances() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // Server
    VM vm1 = host.getVM(1); // Client

    final String regionName = "testRegion";
    final long TEST_OFFSET = 10000;

    ClientCache cache = null;

    try {

      final int serverPort =
          (Integer) vm0.invoke(new SerializableCallable("Start server with a region") {

            @Override
            public Object call() {
              Cache cache = getCache();
              cache.createRegionFactory(REPLICATE).create(regionName);
              getLogWriter().info("Done creating region, now creating CacheServer");
              CacheServer server = null;
              try {
                server = cache.addCacheServer();
                server.setPort(getRandomAvailableTCPPort());
                server.start();
              } catch (IOException e) {
                fail("Starting cache server failed.", e);
              }

              // now set an artificial time offset for the test
              basicGetSystem().getClock().setCacheTimeOffset(null, TEST_OFFSET, true);

              getLogWriter()
                  .info("Done creating and starting CacheServer on port " + server.getPort());
              return server.getPort();
            }
          });

      final String hostName = getServerHostName(vm0.getHost());

      // Start client with proxy region and register interest

      disconnectFromDS();
      Properties props = new Properties();
      props.setProperty(LOCATORS, "");
      props = getSystem(props).getProperties();
      cache = new ClientCacheFactory(props).setPoolSubscriptionEnabled(true)
          .addPoolServer(hostName, serverPort).setPoolPingInterval(5000).create();
      Region proxyRegion =
          cache.createClientRegionFactory(CACHING_PROXY).create(regionName);

      proxyRegion.registerInterestRegex(".*");

      proxyRegion.put("testkey", "testValue1");

      final DSClock clock = ((GemFireCacheImpl) cache).getSystem().getClock();
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          long clientTimeOffset = clock.getCacheTimeOffset();
          getLogWriter()
              .info("Client node's new time offset is: " + clientTimeOffset);
          return clientTimeOffset >= TEST_OFFSET;
        }

        public String description() {
          return "Waiting for cacheTimeOffset to be non-zero.  PingOp should have set it to something";
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);
    } finally {
      cache.close();
      vm1.invoke(() -> CacheTestCase.disconnectFromDS());
    }
  }


  @Ignore("not yet implemented")
  @Test
  public void testClientTimeSlowsDown() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); // Server
    VM vm1 = host.getVM(1); // Client

    final String regionName = "testRegion";
    final long TEST_OFFSET = 10000;

    ClientCache cache = null;

    try {

      final int serverPort =
          (Integer) vm0.invoke(new SerializableCallable("Start server with a region") {

            @Override
            public Object call() {
              Cache cache = getCache();
              cache.createRegionFactory(REPLICATE).create(regionName);
              getLogWriter().info("Done creating region, now creating CacheServer");
              CacheServer server = null;
              try {
                server = cache.addCacheServer();
                server.setPort(getRandomAvailableTCPPort());
                server.start();
              } catch (IOException e) {
                fail("Starting cache server failed.", e);
              }

              // now set an artificial time offset for the test
              basicGetSystem().getClock().setCacheTimeOffset(null, -TEST_OFFSET, true);

              getLogWriter()
                  .info("Done creating and starting CacheServer on port " + server.getPort());
              return server.getPort();
            }
          });

      pause((int) TEST_OFFSET); // let cacheTimeMillis consume the time offset

      final String hostName = getServerHostName(vm0.getHost());

      // Start client with proxy region and register interest

      disconnectFromDS();
      Properties props = new Properties();
      props.setProperty(LOCATORS, "");
      props = getSystem(props).getProperties();
      cache = new ClientCacheFactory(props).setPoolSubscriptionEnabled(true)
          .addPoolServer(hostName, serverPort).setPoolPingInterval(5000).create();
      Region proxyRegion =
          cache.createClientRegionFactory(CACHING_PROXY).create(regionName);

      proxyRegion.registerInterestRegex(".*");

      proxyRegion.put("testkey", "testValue1");

      final DSClock clock = ((GemFireCacheImpl) cache).getSystem().getClock();
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          long clientTimeOffset = clock.getCacheTimeOffset();
          getLogWriter()
              .info("Client node's new time offset is: " + clientTimeOffset);
          if (clientTimeOffset >= 0) {
            return false;
          }
          long cacheTime = clock.cacheTimeMillis();
          return abs(currentTimeMillis() - (cacheTime - clientTimeOffset)) < 5;
        }

        public String description() {
          return "Waiting for cacheTimeOffset to be negative and cacheTimeMillis to stabilize";
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);
    } finally {
      cache.close();
      vm1.invoke(() -> CacheTestCase.disconnectFromDS());
    }
  }

}
