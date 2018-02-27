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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class ClientFunctionTimeoutRegressionTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME =
      ClientFunctionTimeoutRegressionTest.class.getSimpleName() + "_region";

  private static InternalCache serverCache;

  private static InternalClientCache clientCache;

  private VM server;

  private VM client;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void before() throws Exception {
    server = Host.getHost(0).getVM(0);
    client = Host.getHost(0).getVM(1);

    disconnectAllFromDS();
  }

  @After
  public void after() throws Exception {
    Invoke.invokeInEveryVM(() -> closeCache());
  }

  @Test
  @Parameters({"false,0,server", "false,6000,server", "false,0,region", "false,6000,region",
      "true,0,region", "true,6000,region"})
  public void testExecuteFunctionReadsDefaultTimeout(boolean createPR, int timeout, String mode)
      throws Exception {
    // start server
    int port = server.invoke(() -> createServerCache(createPR));
    // start client
    client.invoke(() -> createClientCache(client.getHost().getHostName(), port, timeout));
    // do puts and get
    server.invoke(() -> doPutsAndGet(10));
    // execute function & verify timeout has been received at server.
    client.invoke(() -> executeFunction(mode, timeout));
  }

  private void closeCache() {
    if (clientCache != null) {
      clientCache.close();
      clientCache = null;
    }
    if (serverCache != null) {
      serverCache.close();
      serverCache = null;
    }
  }

  private void createClientCache(String hostName, Integer port, Integer timeout) {
    if (timeout > 0) {
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT",
          String.valueOf(timeout));
    }

    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");

    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.addPoolServer(hostName, port);
    clientCache = (InternalClientCache) ccf.create();

    ClientRegionFactory<String, String> crf =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    crf.create(REGION_NAME);
  }

  private Integer createServerCache(Boolean createPR) throws IOException {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.ClientFunctionTimeoutRegressionTest*");

    serverCache = (InternalCache) new CacheFactory(props).create();

    RegionFactory<String, String> rf;
    if (createPR) {
      rf = serverCache.createRegionFactory(RegionShortcut.PARTITION);
      rf.setPartitionAttributes(new PartitionAttributesFactory<String, String>()
          .setRedundantCopies(1).setTotalNumBuckets(4).create());
    } else {
      rf = serverCache.createRegionFactory(RegionShortcut.REPLICATE);
    }

    rf.create(REGION_NAME);

    CacheServer server = serverCache.addCacheServer();
    server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    server.start();
    return server.getPort();
  }

  private void executeFunction(String mode, Integer timeout) {
    Function function = new TestFunction(mode + timeout);
    FunctionService.registerFunction(function);
    Execution dataSet;
    if ("region".equalsIgnoreCase(mode)) {
      dataSet = FunctionService.onRegion(clientCache.getRegion(REGION_NAME)).setArguments(timeout);
    } else if ("server".equalsIgnoreCase(mode)) {
      dataSet = FunctionService.onServer(clientCache.getDefaultPool()).setArguments(timeout);
    } else {
      dataSet = FunctionService.onServers(clientCache).setArguments(timeout);
    }
    ResultCollector rs = dataSet.execute(function);
    assertThat((Boolean) ((ArrayList) rs.getResult()).get(0))
        .as("Server did not read client_function_timeout from client.").isTrue();
  }

  private void doPutsAndGet(Integer num) {
    Region r = serverCache.getRegion(REGION_NAME);
    for (int i = 0; i < num; ++i) {
      r.put("KEY_" + i, "VALUE_" + i);
    }
    r.get("KEY_0");
  }

  private static class TestFunction extends FunctionAdapter {

    private final String id;

    public TestFunction(String id) {
      this.id = id;
    }

    @Override
    public void execute(FunctionContext context) {
      boolean timeoutMatches = false;
      int expected = (Integer) context.getArguments();
      AcceptorImpl acceptor =
          ((CacheServerImpl) serverCache.getCacheServers().get(0)).getAcceptor();
      ServerConnection[] scs = acceptor.getAllServerConnectionList();
      for (ServerConnection sc : scs) {
        ServerSideHandshake hs = sc.getHandshake();
        if (hs != null && expected == hs.getClientReadTimeout()) {
          timeoutMatches = true;
        }
      }

      context.getResultSender().lastResult(timeoutMatches);
    }

    @Override
    public String getId() {
      return this.id;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }
}
