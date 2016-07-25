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
package com.gemstone.gemfire.internal.cache;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.LocatorTestBase;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;

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
