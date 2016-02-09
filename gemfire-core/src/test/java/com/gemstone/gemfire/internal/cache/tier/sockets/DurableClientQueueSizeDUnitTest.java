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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Iterator;
import java.util.Properties;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author ashetkar
 *
 */
@SuppressWarnings("serial")
public class DurableClientQueueSizeDUnitTest extends DistributedTestCase {

  private static VM vm0 = null;
  private static VM vm1 = null;
  private static VM vm2 = null;
  private static VM vm3 = null;

  private static GemFireCacheImpl cache;
  
  private static int port0;

  private static int port1;

  private static final int EXCEPTION = -5;

  public static final String REGION_NAME = "DurableClientQueueSizeDunitTest_region";

  public static final String NEW_REGION = "DurableClientQueueSizeDunitTest_region_2";

  public static final String POOL_NAME = "my-pool";

  public static final String DEFAULT_POOL_NAME = "DEFAULT";

  /**
   * @param name
   */
  public DurableClientQueueSizeDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    vm0 = Host.getHost(0).getVM(0);
    vm1 = Host.getHost(0).getVM(1);
    vm2 = Host.getHost(0).getVM(2);
    vm3 = Host.getHost(0).getVM(3);

    port0 = (Integer) vm0.invoke(DurableClientQueueSizeDUnitTest.class,
        "createCacheServer", new Object[] { });
    port1 = (Integer) vm1.invoke(DurableClientQueueSizeDUnitTest.class,
        "createCacheServer", new Object[] { });
    IgnoredException.addIgnoredException("java.net.SocketException");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }

  @Override
  protected final void preTearDown() throws Exception {
    closeCache();

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache");
    vm3.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache");

    vm0.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache");
    vm1.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache");
  }

  public void testNonDurableClientFails() throws Exception {
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 }, false });

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { EXCEPTION });
  }

  // this test is disabled due to a high rate of failure.  It fails with
  // the queue size being 11 instead of 10 in the first verifyQueueSize check.
  // See internal ticket #52227.
  public void disabledtestSinglePoolClientReconnectsBeforeTimeOut() throws Exception {
    int num = 10;
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 }});
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE });
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "doRI");
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "readyForEvents");

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache", new Object[] {Boolean.TRUE});

    vm0.invoke(DurableClientQueueSizeDUnitTest.class, "doPuts", new Object[] {num});
    
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 }});

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { num + 1 /* +1 for marker */});
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "readyForEvents");
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { EXCEPTION });

  }

  public void testSinglePoolClientReconnectsAfterTimeOut() throws Exception {
    int num = 10;
    long timeoutSeconds = 10;
    vm2.invoke(
        DurableClientQueueSizeDUnitTest.class,
        "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 },
            String.valueOf(timeoutSeconds), true });
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "doRI");
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "readyForEvents");

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache", new Object[] {Boolean.TRUE});

    vm0.invoke(DurableClientQueueSizeDUnitTest.class, "doPuts", new Object[] {num});
    Thread.sleep(timeoutSeconds * 1000); // TODO use a waitCriterion

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 }});

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { PoolImpl.PRIMARY_QUEUE_TIMED_OUT });
  }

  public void testPrimaryServerRebootReturnsCorrectResponse() throws Exception {
    int num = 10;
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 } });
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "doRI");
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "readyForEvents");

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache",
        new Object[] { Boolean.TRUE });

    vm0.invoke(DurableClientQueueSizeDUnitTest.class, "doPuts",
        new Object[] { num });

    // Identify primary and restart it
    boolean isVM0Primary = (Boolean) vm0.invoke(
        DurableClientQueueSizeDUnitTest.class, "isPrimary");
    int port = 0;
    if (isVM0Primary) {
      vm0.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache");
      vm0.invoke(DurableClientQueueSizeDUnitTest.class, "createCacheServer",
          new Object[] { port0 });
      port = port0;
    } else { // vm1 is primary
      vm1.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache");
      vm1.invoke(DurableClientQueueSizeDUnitTest.class, "createCacheServer",
          new Object[] { port1 });
      port = port1;
    }

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port } });

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE });
  }

  public void bug51854_testMultiPoolClientReconnectsBeforeTimeOut() throws Exception {
    int num = 10;
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 }, "300",
            true/* durable */, true /* multiPool */});
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE, PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE });
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "doRI");
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "readyForEvents");

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache", new Object[] {Boolean.TRUE});

    vm0.invoke(DurableClientQueueSizeDUnitTest.class, "doPuts", new Object[] {num});
    
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 }, "300",
            true/* durable */, true /* multiPool */});

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { num + 1 /* +1 for marker */, (num * 2) + 1});
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "readyForEvents");
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { EXCEPTION, EXCEPTION });
  }

  public void bug51854_testMultiPoolClientReconnectsAfterTimeOut() throws Exception {
    int num = 10;
    long timeout = 10;
    vm2.invoke(
        DurableClientQueueSizeDUnitTest.class,
        "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 },
            String.valueOf(timeout), true/* durable */, true /* multiPool */});
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE, PoolImpl.PRIMARY_QUEUE_NOT_AVAILABLE });
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "doRI");
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "readyForEvents");

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "closeCache", new Object[] {Boolean.TRUE});

    vm0.invoke(DurableClientQueueSizeDUnitTest.class, "doPuts", new Object[] {num});
//    vm0.invoke(DurableClientQueueSizeDUnitTest.class,
//        "verifyQueueSizeAtServer", new Object[] { DEFAULT_POOL_NAME, num + 1 });
//    vm0.invoke(DurableClientQueueSizeDUnitTest.class,
//        "verifyQueueSizeAtServer", new Object[] { POOL_NAME, num * 2 + 1 });
    Thread.sleep(timeout * 1000); // TODO use a waitCriterion
    
    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "createClientCache",
        new Object[] { vm2.getHost(), new Integer[] { port0, port1 }, "300",
            true/* durable */, true /* multiPool */});

    vm2.invoke(DurableClientQueueSizeDUnitTest.class, "verifyQueueSize",
        new Object[] { PoolImpl.PRIMARY_QUEUE_TIMED_OUT, PoolImpl.PRIMARY_QUEUE_TIMED_OUT});
  }

  public void _testMultiPoolClientFailsOver() throws Exception {
  }

  public static void closeCache() throws Exception {
    closeCache(false);
  }

  public static void closeCache(Boolean keepAlive) throws Exception {
    setSpecialDurable(false);
    if (cache != null) {
      cache.close(keepAlive);
    }
  }

  public static Boolean isPrimary() throws Exception {
    return CacheClientNotifier.getInstance().getClientProxies().iterator().next().isPrimary();
  }

  public static Integer createCacheServer() throws Exception {
    return createCacheServer(
        AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
  }

  @SuppressWarnings("deprecation")
  public static Integer createCacheServer(Integer serverPort)
      throws Exception {
    Properties props = new Properties();
    props.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
//    props.setProperty("log-level", "fine");
//    props.setProperty("log-file", "server_" + OSProcess.getId() + ".log");
//    props.setProperty("statistic-archive-file", "server_" + OSProcess.getId()
//        + ".gfs");
//    props.setProperty("statistic-sampling-enabled", "true");

    DurableClientQueueSizeDUnitTest test = new DurableClientQueueSizeDUnitTest(
        "DurableClientQueueSizeDUnitTest");
    DistributedSystem ds = test.getSystem(props);
    ds.disconnect();
    cache = (GemFireCacheImpl)CacheFactory.create(test.getSystem());
//    cache = (GemFireCacheImpl) new CacheFactory(props).create();

    RegionFactory<String, String> rf = cache
        .createRegionFactory(RegionShortcut.REPLICATE);

    rf.create(REGION_NAME);
    rf.create(NEW_REGION);

    CacheServer server = cache.addCacheServer();
    server.setPort(serverPort);
    server.start();
    return server.getPort();
  }

  public static void createClientCache(Host host, Integer[] ports)
      throws Exception {
    createClientCache(host, ports, "300", Boolean.TRUE);
  }

  public static void createClientCache(Host host, Integer[] ports, Boolean durable)
      throws Exception {
    createClientCache(host, ports, "300", durable);
  }

  public static void createClientCache(Host host, Integer[] ports,
      String timeoutMilis, Boolean durable) throws Exception {
    createClientCache(host, ports, timeoutMilis, durable, false);
  }

  public static void setSpecialDurable(Boolean bool) {
    System.setProperty("gemfire.SPECIAL_DURABLE", bool.toString());
  }

  @SuppressWarnings("deprecation")
  public static void createClientCache(Host host, Integer[] ports,
      String timeoutSeconds, Boolean durable, Boolean multiPool) throws Exception {
    if (multiPool) {
      System.setProperty("gemfire.SPECIAL_DURABLE", "true");
    }
    Properties props = new Properties();
    if (durable) {
      props.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME,
          "my-durable-client");
      props.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME,
          timeoutSeconds);
    }
    //    props.setProperty("log-file", "client_" + OSProcess.getId() + ".log");
//    props.setProperty("log-level", "fine");
//    props.setProperty("statistic-archive-file", "client_" + OSProcess.getId()
//        + ".gfs");
//    props.setProperty("statistic-sampling-enabled", "true");

    DistributedSystem ds = new DurableClientQueueSizeDUnitTest(
        "DurableClientQueueSizeDUnitTest").getSystem(props);
    ds.disconnect();
    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.setPoolSubscriptionEnabled(true);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionRedundancy(1);
    ccf.setPoolMaxConnections(1);
    for (int port : ports) {
      ccf.addPoolServer(host.getHostName(), port);
    }
    cache = (GemFireCacheImpl) ccf.create();

    ClientRegionFactory<String, String> crf = cache
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    crf.setPoolName(cache.getDefaultPool().getName());
    crf.create(REGION_NAME);

    if (multiPool) {
      String poolName = POOL_NAME;
      PoolFactory pf = PoolManager.createFactory();
      for (int port : ports) {
        pf.addServer(host.getHostName(), port);
      }
      pf.setSubscriptionEnabled(true);
      pf.create(poolName);
      crf.setPoolName(poolName);
      crf.create(NEW_REGION);
    }
  }
  
  @SuppressWarnings("unchecked")
  public static void doRI() {
    cache.getRegion(REGION_NAME).registerInterest("ALL_KEYS", true);
    if (cache.getRegion(NEW_REGION) != null) {
      cache.getRegion(NEW_REGION).registerInterest("ALL_KEYS", true);
    }
  }
  
  public static void readyForEvents() {
    cache.readyForEvents();
  }
  
  @SuppressWarnings("unchecked")
  public static void doPuts(Integer numOfPuts) throws Exception {
    Region<String, String> region = cache.getRegion(REGION_NAME);

    for (int j = 0; j < numOfPuts; j++) {
      region.put("KEY_" + j, "VALUE_" + j);
    }

    region = cache.getRegion(NEW_REGION);

    for (int j = 0; j < (numOfPuts*2); j++) {
      region.put("KEY_" + j, "VALUE_" + j);
    }
  }

  public static void verifyQueueSizeAtServer(String poolName, Integer num) throws Exception {
    Iterator<CacheClientProxy> it = CacheClientNotifier.getInstance().getClientProxies().iterator();
    while (it.hasNext()) {
      CacheClientProxy ccp = it.next();
      if (ccp.getDurableId().contains(poolName)) {
        assertEquals(num.intValue(), ccp.getQueueSize());
      }
    }
  }

  public static void verifyQueueSize(Integer num) throws Exception {
    verifyQueueSize(num, Integer.MIN_VALUE);
  }

  public static void verifyQueueSize(Integer num1, Integer num2) throws Exception {
    try {
      assertEquals(num1.intValue(),
          PoolManager.find(DEFAULT_POOL_NAME).getPendingEventCount());
    } catch (IllegalStateException ise) {
      assertEquals(EXCEPTION, num1.intValue());
    }
    if (num2 != Integer.MIN_VALUE) {
      try {
        assertEquals(num2.intValue(),
            PoolManager.find(POOL_NAME).getPendingEventCount());
      } catch (IllegalStateException ise) {
        assertEquals(EXCEPTION, num2.intValue());
      }
    }
  }
}
