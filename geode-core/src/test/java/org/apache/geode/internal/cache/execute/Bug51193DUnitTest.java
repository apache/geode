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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.tier.ClientHandShake;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class Bug51193DUnitTest extends JUnit4DistributedTestCase {


  private static final String REGION_NAME = "Bug51193DUnitTest_region";

  private static GemFireCacheImpl cache;

  private static VM server0;

  private VM client0;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    server0 = host.getVM(0);
    client0 = host.getVM(1);
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();
    server0.invoke(() -> Bug51193DUnitTest.closeCache());
    client0.invoke(() -> Bug51193DUnitTest.closeCache());
  }

  public static void closeCache() {
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT");
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @SuppressWarnings("deprecation")
  public static void createClientCache(String hostName, Integer port, Integer timeout)
      throws Exception {
    try {
      if (timeout > 0) {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT",
            String.valueOf(timeout));
      }
      Properties props = new Properties();
      props.setProperty(LOCATORS, "");
      props.setProperty(MCAST_PORT, "0");
      DistributedSystem ds = new Bug51193DUnitTest().getSystem(props);
      ds.disconnect();
      ClientCacheFactory ccf = new ClientCacheFactory(props);
      ccf.addPoolServer(hostName, port);
      cache = (GemFireCacheImpl) ccf.create();

      ClientRegionFactory<String, String> crf =
          cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

      crf.create(REGION_NAME);
    } finally {
      System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT");
    }
  }

  @SuppressWarnings("deprecation")
  public static Integer createServerCache(Boolean createPR) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");

    Bug51193DUnitTest test = new Bug51193DUnitTest();
    DistributedSystem ds = test.getSystem(props);
    ds.disconnect();
    cache = (GemFireCacheImpl) CacheFactory.create(test.getSystem());

    RegionFactory<String, String> rf = null;
    if (createPR) {
      rf = cache.createRegionFactory(RegionShortcut.PARTITION);
      rf.setPartitionAttributes(new PartitionAttributesFactory<String, String>()
          .setRedundantCopies(1).setTotalNumBuckets(4).create());
    } else {
      rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    }

    rf.create(REGION_NAME);

    CacheServer server = cache.addCacheServer();
    server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    server.start();
    return server.getPort();
  }

  public static void executeFunction(String mode, Integer timeout) {
    Function function = new TestFunction(mode + timeout);
    FunctionService.registerFunction(function);
    Execution dataSet = null;
    if ("region".equalsIgnoreCase(mode)) {
      dataSet = FunctionService.onRegion(cache.getRegion(REGION_NAME)).withArgs(timeout);
    } else if ("server".equalsIgnoreCase(mode)) {
      dataSet = FunctionService.onServer(cache.getDefaultPool()).withArgs(timeout);
    } else {
      dataSet = FunctionService.onServers(cache).withArgs(timeout);
    }
    ResultCollector rs = dataSet.execute(function);
    assertTrue("Server did not read client_function_timeout from client.",
        (Boolean) ((ArrayList) rs.getResult()).get(0));
  }

  @SuppressWarnings("rawtypes")
  public static void doPutsAndGet(Integer num) {
    Region r = cache.getRegion(REGION_NAME);
    for (int i = 0; i < num; ++i) {
      r.put("KEY_" + i, "VALUE_" + i);
    }
    r.get("KEY_0");
  }

  public void doTest(boolean createPR, int timeout, String mode) throws Throwable {
    // start server
    int port = (Integer) server0.invoke(() -> Bug51193DUnitTest.createServerCache(createPR));
    // start client
    client0.invoke(
        () -> Bug51193DUnitTest.createClientCache(client0.getHost().getHostName(), port, timeout));
    // do puts and get
    server0.invoke(() -> Bug51193DUnitTest.doPutsAndGet(10));
    // execute function & verify timeout has been received at server.
    client0.invoke(() -> Bug51193DUnitTest.executeFunction(mode, timeout));
  }

  @Test
  public void testExecuteFunctionReadsDefaultTimeout() throws Throwable {
    doTest(false, 0, "server");
  }

  @Test
  public void testExecuteRegionFunctionReadsDefaultTimeout() throws Throwable {
    doTest(false, 0, "region");
  }

  @Test
  public void testExecuteRegionFunctionSingleHopReadsDefaultTimeout() throws Throwable {
    doTest(true, 0, "region");
  }

  @Test
  public void testExecuteFunctionReadsTimeout() throws Throwable {
    doTest(false, 6000, "server");
  }

  @Test
  public void testExecuteRegionFunctionReadsTimeout() throws Throwable {
    doTest(false, 6000, "region");
  }

  @Test
  public void testExecuteRegionFunctionSingleHopReadsTimeout() throws Throwable {
    doTest(true, 6000, "region");
  }

  static class TestFunction extends FunctionAdapter {

    private String id;

    public TestFunction(String id) {
      this.id = id;
    }

    @Override
    public void execute(FunctionContext context) {
      boolean timeoutMatches = false;
      int expected = (Integer) context.getArguments();
      AcceptorImpl acceptor = ((CacheServerImpl) cache.getCacheServers().get(0)).getAcceptor();
      ServerConnection[] scs = acceptor.getAllServerConnectionList();
      for (int i = 0; i < scs.length; ++i) {
        ClientHandShake hs = scs[i].getHandshake();
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
