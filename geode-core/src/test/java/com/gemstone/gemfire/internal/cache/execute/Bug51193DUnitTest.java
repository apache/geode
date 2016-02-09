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
package com.gemstone.gemfire.internal.cache.execute;

import java.util.ArrayList;
import java.util.Properties;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.ClientHandShake;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

@SuppressWarnings("serial")
public class Bug51193DUnitTest extends DistributedTestCase {

  public Bug51193DUnitTest(String name) {
    super(name);
  }

  private static GemFireCacheImpl cache;
  
  private static final String REGION_NAME = "Bug51193DUnitTest_region";
  
  private static VM server0;
  
  private static VM client0;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    server0 = host.getVM(0);
    client0 = host.getVM(1);
    
  }

  @Override
  protected final void preTearDown() throws Exception {
    closeCache();
    server0.invoke(Bug51193DUnitTest.class, "closeCache");
    client0.invoke(Bug51193DUnitTest.class, "closeCache");
  }

  public static void closeCache() {
    System.clearProperty("gemfire.CLIENT_FUNCTION_TIMEOUT");
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @SuppressWarnings("deprecation")
  public static void createClientCache(String hostName, Integer port,
      Integer timeout) throws Exception {
    try {
      if (timeout > 0) {
        System.setProperty("gemfire.CLIENT_FUNCTION_TIMEOUT",
            String.valueOf(timeout));
      }
      Properties props = new Properties();
      props.setProperty("locators", "");
      props.setProperty("mcast-port", "0");
//      props.setProperty("statistic-archive-file", "client_" + OSProcess.getId()
//          + ".gfs");
//      props.setProperty("statistic-sampling-enabled", "true");
      DistributedSystem ds = new Bug51193DUnitTest("Bug51193DUnitTest")
          .getSystem(props);
      ds.disconnect();
      ClientCacheFactory ccf = new ClientCacheFactory(props);
      ccf.addPoolServer(hostName, port);
      cache = (GemFireCacheImpl) ccf.create();

      ClientRegionFactory<String, String> crf = cache
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

      crf.create(REGION_NAME);
    } finally {
      System.clearProperty("gemfire.CLIENT_FUNCTION_TIMEOUT");
    }
  }

  @SuppressWarnings("deprecation")
  public static Integer createServerCache(Boolean createPR)
      throws Exception {
    Properties props = new Properties();
    props.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");

    Bug51193DUnitTest test = new Bug51193DUnitTest("Bug51193DUnitTest");
    DistributedSystem ds = test.getSystem(props);
    ds.disconnect();
    cache = (GemFireCacheImpl)CacheFactory.create(test.getSystem());

    RegionFactory<String, String> rf = null;
    if (createPR) {
      rf = cache.createRegionFactory(RegionShortcut.PARTITION);
      rf.setPartitionAttributes(
          new PartitionAttributesFactory<String, String>()
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

  public void doTest(boolean createPR, int timeout, String mode)
      throws Throwable {
    // start server
    int port = (Integer) server0.invoke(Bug51193DUnitTest.class,
        "createServerCache", new Object[] { createPR });
    // start client
    client0.invoke(Bug51193DUnitTest.class, "createClientCache", new Object[] {
        client0.getHost().getHostName(), port, timeout });
    // do puts and get
    server0
        .invoke(Bug51193DUnitTest.class, "doPutsAndGet", new Object[] { 10 });
    // execute function & verify timeout has been received at server.
    client0.invoke(Bug51193DUnitTest.class,
        "executeFunction", new Object[] { mode, timeout });
  }

  public void testExecuteFunctionReadsDefaultTimeout() throws Throwable {
    doTest(false, 0, "server");
  }

  public void testExecuteRegionFunctionReadsDefaultTimeout() throws Throwable {
    doTest(false, 0, "region");
  }

  public void testExecuteRegionFunctionSingleHopReadsDefaultTimeout() throws Throwable {
    doTest(true, 0, "region");
  }

  public void testExecuteFunctionReadsTimeout() throws Throwable {
    doTest(false, 6000, "server");
  }

  public void testExecuteRegionFunctionReadsTimeout() throws Throwable {
    doTest(false, 6000, "region");
  }

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
      int expected = (Integer)context.getArguments();
      AcceptorImpl acceptor = ((CacheServerImpl) cache.getCacheServers()
          .get(0)).getAcceptor();
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
