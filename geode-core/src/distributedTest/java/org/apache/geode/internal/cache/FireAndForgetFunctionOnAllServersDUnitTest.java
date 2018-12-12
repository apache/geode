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

package org.apache.geode.internal.cache;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.LocatorTestBase;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.functions.FireAndForgetFunctionOnAllServers;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;


public class FireAndForgetFunctionOnAllServersDUnitTest extends LocatorTestBase {

  public FireAndForgetFunctionOnAllServersDUnitTest() {
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
  public void testFireAndForgetFunctionOnAllServers() {

    // Test case for Executing a fire-and-forget function on all servers as opposed to only
    // executing on the ones the
    // client is currently connected to.

    Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);

    final String locatorHost = NetworkUtils.getServerHostName();

    // Step 1. Start a locator and one cache server.
    final int locatorPort = locator.invoke("Start Locator", () -> startLocator(locatorHost, ""));

    String locString = getLocatorString(locatorHost, locatorPort);

    // Step 2. Start a server and create a replicated region "R1".
    server1.invoke("Start BridgeServer",
        () -> startBridgeServer(new String[] {"R1"}, locString, new String[] {"R1"}));

    // Step 3. Create a client cache with pool mentioning locator.
    client.invoke("create client cache and pool mentioning locator", () -> {
      ClientCacheFactory ccf = new ClientCacheFactory();
      ccf.addPoolLocator(locatorHost, locatorPort);
      ClientCache cache = ccf.create();
      Pool pool1 = PoolManager.createFactory().addLocator(locatorHost, locatorPort)
          .setServerGroup("R1").create("R1");

      Region region1 = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).setPoolName("R1")
          .create("R1");

      // Step 4. Execute the function to put DistributedMemberID into above created replicated
      // region.
      Function function = new FireAndForgetFunctionOnAllServers();
      FunctionService.registerFunction(function);

      PoolImpl pool = (PoolImpl) pool1;

      await()
          .untilAsserted(() -> Assert.assertEquals(1, pool.getCurrentServers().size()));

      String regionName = "R1";
      Execution dataSet = FunctionService.onServers(pool1);
      dataSet.setArguments(regionName).execute(function);

      // Using Awatility, if the condition is not met during the timeout, a
      // ConditionTimeoutException will be thrown. This makes analyzing the failure much simpler
      // Step 5. Assert for the region keyset size with 1.
      await()
          .untilAsserted(() -> Assert.assertEquals(1, region1.keySetOnServer().size()));

      region1.clear();

      // Step 6. Start another server mentioning locator and create a replicated region "R1".
      server2.invoke("Start BridgeServer",
          () -> startBridgeServer(new String[] {"R1"}, locString, new String[] {"R1"}));

      // Step 7. Execute the same function to put DistributedMemberID into above created replicated
      // region.
      await()
          .untilAsserted(() -> Assert.assertEquals(1, pool.getCurrentServers().size()));
      dataSet = FunctionService.onServers(pool1);
      dataSet.setArguments(regionName).execute(function);

      await()
          .untilAsserted(() -> Assert.assertEquals(2, pool.getCurrentServers().size()));

      // Using Awatility, if the condition is not met during the timeout, a
      // ConditionTimeoutException will be thrown. This makes analyzing the failure much simpler
      // Step 8. Assert for the region keyset size with 2, since above function was executed on 2
      // servers.
      await().untilAsserted(() -> {
        Assert.assertEquals(2, region1.keySetOnServer().size());
      });

      region1.clear();

      // Step 8.Stop one of the servers.
      server1.invoke("Stop BridgeServer", () -> stopBridgeMemberVM(server1));

      // Step 9. Execute the same function to put DistributedMemberID into above created replicated
      // region.
      dataSet = FunctionService.onServers(pool1);
      dataSet.setArguments(regionName).execute(function);

      await()
          .untilAsserted(() -> Assert.assertEquals(1, pool.getCurrentServers().size()));

      // Using Awatility, if the condition is not met during the timeout, a
      // ConditionTimeoutException will be thrown. This makes analyzing the failure much simpler
      // Step 10. Assert for the region keyset size with 1, since only one server was running.
      await().untilAsserted(() -> {
        Assert.assertEquals(1, region1.keySetOnServer().size());
      });

      region1.clear();

      return null;
    });
  }
}
