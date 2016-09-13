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
package org.apache.geode.internal.cache;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.LocatorTestBase;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;

@Category(DistributedTest.class)
public class Bug47667DUnitTest extends LocatorTestBase {

  private static final long serialVersionUID = 2859534245283086765L;

  public Bug47667DUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  protected final void postTearDownLocatorTestBase() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void testbug47667() {
    Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);

    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locatorHost = NetworkUtils.getServerHostName(host);
    locator.invoke("Start Locator", () -> startLocator(locatorHost, locatorPort, ""));

    String locString = getLocatorString(host, locatorPort);
    server1.invoke("Start BridgeServer", () -> startBridgeServer(new String[] { "R1" }, locString, new String[] { "R1" }));
    server2.invoke("Start BridgeServer", () -> startBridgeServer(new String[] { "R2" }, locString, new String[] { "R2" }));

    client.invoke("create region and insert data in transaction", () -> {
      ClientCacheFactory ccf = new ClientCacheFactory();
      ccf.addPoolLocator(locatorHost, locatorPort);
      ClientCache cache = ccf.create();
      PoolManager.createFactory().addLocator(locatorHost, locatorPort).setServerGroup("R1").create("R1");
      PoolManager.createFactory().addLocator(locatorHost, locatorPort).setServerGroup("R2").create("R2");
      Region region1 = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).setPoolName("R1").create("R1");
      Region region2 = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).setPoolName("R2").create("R2");
      CacheTransactionManager transactionManager = cache.getCacheTransactionManager();
      transactionManager.begin();
      region1.put(1, "value1");
      transactionManager.commit();
      transactionManager.begin();
      region2.put(2, "value2");
      transactionManager.commit();
      return null;
    });
  }
}
