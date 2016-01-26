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
package com.gemstone.gemfire.internal.cache.ha;

import java.net.SocketException;
import java.util.Properties;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

public class HASlowReceiverDUnitTest extends DistributedTestCase {
  protected static Cache cache = null;

  private static VM serverVM1 = null;

  protected static VM serverVM2 = null;

  protected static VM clientVM = null;

  private static int PORT0;

  private static int PORT1;

  private static int PORT2;

  private static final String regionName = "HASlowReceiverDUnitTest";

  protected static LogWriter logger = null;

  static PoolImpl pool = null;
  
  private static boolean isUnresponsiveClientRemoved = false;

  public HASlowReceiverDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    final Host host = Host.getHost(0);
    serverVM1 = host.getVM(1);
    serverVM2 = host.getVM(2);
    clientVM = host.getVM(3);

    PORT0 = createServerCache().intValue();
    PORT1 = ((Integer)serverVM1.invoke(HASlowReceiverDUnitTest.class,
        "createServerCache")).intValue();
    PORT2 = ((Integer)serverVM2.invoke(HASlowReceiverDUnitTest.class,
        "createServerCache")).intValue();

  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    clientVM.invoke(HASlowReceiverDUnitTest.class, "closeCache");

    // then close the servers
    closeCache();
    serverVM1.invoke(HASlowReceiverDUnitTest.class, "closeCache");
    serverVM2.invoke(HASlowReceiverDUnitTest.class, "closeCache");
    disconnectAllFromDS();
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static Integer createServerCache() throws Exception {
    return createServerCache(null);
  }

  public static Integer createServerCache(String ePolicy) throws Exception {
    return createServerCache(ePolicy, new Integer(1));
  }

  public static Integer createServerCache(String ePolicy, Integer cap)
      throws Exception {

    Properties prop = new Properties();
    prop.setProperty(DistributionConfig.REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME,
        "true");
    new HASlowReceiverDUnitTest("temp").createCache(prop);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    logger = cache.getLogger();

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.setMaximumMessageCount(200);
    if (ePolicy != null) {
      server1.getClientSubscriptionConfig().setEvictionPolicy(ePolicy);
      server1.getClientSubscriptionConfig().setCapacity(cap.intValue());
    }
    server1.start();
    return new Integer(server1.getPort());
  }

  public static void createClientCache(String host, Integer port1,
      Integer port2, Integer port3, Integer rLevel, Boolean addListener)
      throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new HASlowReceiverDUnitTest("temp").createCache(props);

    AttributesFactory factory = new AttributesFactory();
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer("localhost",
        port1).addServer("localhost", port2).addServer("localhost", port3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(
            rLevel.intValue()).setThreadLocalConnections(true)
        .setMinConnections(6).setReadTimeout(20000).setPingInterval(1000)
        .setRetryAttempts(5).create("HASlowRecieverDUnitTestPool");

    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    if (addListener.booleanValue()) {
      factory.addCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterUpdate(EntryEvent event) {

          if (event.getNewValue().equals("v20")) {
            try {
              Thread.sleep(120000);
            }
            catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }
      });
    }
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    pool = p;
  }

  public static void createClientCache(String host, Integer port1,
      Integer port2, Integer port3, Integer rLevel) throws Exception {
    createClientCache(host, port1, port2, port3, rLevel, Boolean.TRUE);
  }

  public static void registerInterest() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("ALL_KEYS");
    }
    catch (Exception ex) {
      fail("failed in registerInterestListAll", ex);
    }
  }

  public static void putEntries() {
    try {

      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      for (long i = 0; i < 300; i++) {
        r.put("k" + (i % 10), "v" + i);
        r.put("k" + (i % 10), new byte[1000]);
      }
    }
    catch (Exception ex) {
      fail("failed in putEntries()", ex);
    }
  }

  public static void createEntries(Long num) {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      for (long i = 0; i < num.longValue(); i++) {
        r.create("k" + i, "v" + i);
      }
    }
    catch (Exception ex) {
      fail("failed in createEntries(Long)", ex);
    }
  }

  public static void checkRedundancyLevel(final Integer redundantServers) {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return pool.getRedundantNames().size() == redundantServers.intValue();
      }

      public String description() {
        return "Expected redundant count (" + pool.getRedundantNames().size()
            + ") to become " + redundantServers.intValue();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 200 * 1000, 1000, true);
  }

  // Test slow client
  public void testSlowClient() throws Exception {
    setBridgeObeserverForAfterQueueDestroyMessage();
    clientVM.invoke(HASlowReceiverDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(Host.getHost(0)), new Integer(PORT0),
            new Integer(PORT1), new Integer(PORT2), new Integer(2) });
    clientVM.invoke(HASlowReceiverDUnitTest.class, "registerInterest");
    // add expected socket exception string
    final ExpectedException ex1 = addExpectedException(SocketException.class
        .getName());
    final ExpectedException ex2 = addExpectedException(InterruptedException.class
        .getName());
    putEntries();
    Thread.sleep(20000);// wait for put to block and allow server to remove
                        // client queue
    clientVM.invoke(HASlowReceiverDUnitTest.class, "checkRedundancyLevel",
        new Object[] { new Integer(2) });
    // check for slow client queue is removed or not.
    assertTrue("isUnresponsiveClientRemoved is false, but should be true "
        + "after 20 seconds", isUnresponsiveClientRemoved);
    ex1.remove();
    ex2.remove();
  }

  public static void setBridgeObeserverForAfterQueueDestroyMessage()
      throws Exception {
    PoolImpl.AFTER_QUEUE_DESTROY_MESSAGE_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      @Override
      public void afterQueueDestroyMessage() {       
        clientVM.invoke(HASlowReceiverDUnitTest.class, "checkRedundancyLevel",
            new Object[] { new Integer(0) });
        isUnresponsiveClientRemoved = true;   
        PoolImpl.AFTER_QUEUE_DESTROY_MESSAGE_FLAG = false;
      }
    });
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

}
