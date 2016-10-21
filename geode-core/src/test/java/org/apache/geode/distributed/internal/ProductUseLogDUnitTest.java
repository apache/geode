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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class ProductUseLogDUnitTest extends JUnit4CacheTestCase {

  @Override
  public void preSetUp() {
    disconnectAllFromDS();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties p = super.getDistributedSystemProperties();
    p.put(USE_CLUSTER_CONFIGURATION, "false");
    return p;
  }

  @Test
  public void testMembershipMonitoring() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // use a locator so we will monitor server load and record member->server mappings
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Properties p = new Properties();
    p.put(START_LOCATOR, "localhost[" + locatorPort + "],peer=false");
    p.put(USE_CLUSTER_CONFIGURATION, "false");
    InternalDistributedSystem system = getSystem(p);

    InternalLocator locator = (InternalLocator) Locator.getLocator();
    // server location is forced on for product use logging
    assertTrue(locator.isServerLocator());

    File logFile = new File("locator" + locatorPort + "views.log");
    // the locator should have already created this file
    assertTrue(logFile.exists());

    assertTrue(logFile.exists());
    int serverPort = (Integer) vm0.invoke(new SerializableCallable("get system") {
      public Object call() {
        getSystem();
        Cache cache = getCache();
        cache.createRegionFactory(RegionShortcut.REPLICATE).create("myregion");
        CacheServer server = cache.addCacheServer();
        server.setPort(0);
        try {
          server.start();
        } catch (IOException e) {
          Assert.fail("failed to start server", e);
        }
        return server.getPort();
      }
    });

    vm1.invoke(new SerializableRunnable("create a client") {
      public void run() {
        ClientCache clientCache = new ClientCacheFactory().setPoolSubscriptionEnabled(true)
            .addPoolServer("localhost", serverPort).create();
        Region r =
            clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("myregion");
        r.registerInterest(".*");
        r.put("somekey", "somevalue");
      }
    });

    vm0.invoke(new SerializableRunnable("check region") {
      public void run() {
        Region r = getCache().getRegion("myregion");
        Assert.assertNotNull(r.get("somekey"));
      }
    });


    // wait for the server info to be received and logged
    Thread.sleep(2 * CacheServer.DEFAULT_LOAD_POLL_INTERVAL);

    system.disconnect();

    String logContents = readFile(logFile);
    assertTrue("expected " + logFile + " to contain a View", logContents.contains("View"));
    assertTrue("expected " + logFile + " to have a server count of 1",
        logContents.contains("server count: 1"));
    assertTrue("expected " + logFile + " to have a client count of 1",
        logContents.contains("client count: 1"));
    assertTrue("expected " + logFile + " to have a queue count of 1",
        logContents.contains("queue count: 1"));
  }

  private String readFile(File file) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(file));
    StringBuilder sb = new StringBuilder(10000);
    String line;
    while ((line = reader.readLine()) != null) {
      sb.append(line).append(File.separator);
    }
    return sb.toString();
  }

}
