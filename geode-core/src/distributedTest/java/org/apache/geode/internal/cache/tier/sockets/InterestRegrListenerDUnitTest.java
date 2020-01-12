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
package org.apache.geode.internal.cache.tier.sockets;

import static java.lang.Thread.sleep;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.internal.cache.tier.sockets.InterestRegrListenerDUnitTest.getListenerMapTask;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.ClientSession;
import org.apache.geode.cache.InterestRegistrationEvent;
import org.apache.geode.cache.InterestRegistrationListener;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Written to test fix for Bug #47132
 */
@Category({ClientSubscriptionTest.class})
public class InterestRegrListenerDUnitTest extends JUnit4DistributedTestCase {

  private Cache cache;
  private DistributedSystem ds;
  private CacheServer cacheServer;
  private ClientCache clientCache;
  private int cacheServerPort;
  private String hostName;
  private final Map<String, Integer> listnerMap = new HashMap<String, Integer>();

  private static final String UNREGISTER_INTEREST = "UnregisterInterest";
  private static final String REGISTER_INTEREST = "RegisterInterest";
  private static final int DURABLE_CLIENT_TIMEOUT_TEST = 20;

  private static InterestRegrListenerDUnitTest instance = new InterestRegrListenerDUnitTest();

  @Override
  public final void preSetUp() throws Exception {
    disconnectAllFromDS();
  }

  public Cache createCache(Properties props) throws Exception {
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  private void createServer() throws IOException {
    cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer = cache.addCacheServer();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();
    hostName = InetAddress.getLocalHost().getHostAddress();
    listnerMap.clear();
  }

  public int getCacheServerPort() {
    return cacheServerPort;
  }

  public String getCacheServerHost() {
    return hostName;
  }

  public void stopCacheServer() {
    this.cacheServer.stop();
  }

  public void setUpServerVM() throws Exception {
    Properties gemFireProps = new Properties();
    createCache(gemFireProps);
    RegionFactory factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region r = factory.create("serverRegion");
    r.put("serverkey", "servervalue");
  }

  public void doClientRegionPut() {
    Region<String, String> region = clientCache.getRegion("serverRegion");
    assertEquals("servervalue", region.get("serverkey"));
    region.put("clientkey", "clientvalue");
    assertEquals("clientvalue", region.get("clientkey"));
  }

  public void doServerRegionPut() {
    Region<String, String> region = cache.getRegion("serverRegion");
    assertEquals("servervalue", region.get("serverkey"));
    assertEquals("clientvalue", region.get("clientkey"));
  }

  public void doClientRegionRegisterInterest(boolean isDurable) {
    Region<String, String> region = clientCache.getRegion("serverRegion");
    region.registerInterestRegex(".*", // everything
        InterestResultPolicy.DEFAULT, true);
  }

  private void doExpressInterestOnServer(boolean isDurable) {
    LogWriterUtils.getLogWriter()
        .info("Total ClientSessions " + cacheServer.getAllClientSessions().size());
    for (ClientSession c : this.cacheServer.getAllClientSessions()) {
      c.registerInterestRegex("/serverRegion", ".*", isDurable);
    }
  }

  private void doRegisterListener() {
    InterestRegistrationListener listener = new InterestRegistrationListener() {
      @Override
      public void close() {}

      @Override
      public void afterUnregisterInterest(InterestRegistrationEvent event) {
        Integer count = InterestRegrListenerDUnitTest.this.listnerMap.get(UNREGISTER_INTEREST);
        int intCount = 0;
        if (count != null)
          intCount = count.intValue();
        intCount++;
        InterestRegrListenerDUnitTest.this.listnerMap.put(UNREGISTER_INTEREST, intCount);
        LogWriterUtils.getLogWriter()
            .info("InterestRegistrationListener afterUnregisterInterest  for "
                + event.getRegionName() + " keys " + event.getKeysOfInterest() + "Count " + intCount
                + " Client : " + event.getClientSession().toString());
      }

      @Override
      public void afterRegisterInterest(InterestRegistrationEvent event) {
        Integer count = InterestRegrListenerDUnitTest.this.listnerMap.get(REGISTER_INTEREST);
        int intCount = 0;
        if (count != null)
          intCount = count.intValue();
        intCount++;
        InterestRegrListenerDUnitTest.this.listnerMap.put(REGISTER_INTEREST, intCount);
        LogWriterUtils.getLogWriter()
            .info("InterestRegistrationListener afterRegisterInterest  for " + event.getRegionName()
                + " keys " + event.getKeysOfInterest() + "Count " + intCount + " Client : "
                + event.getClientSession().toString());
      }
    };
    LogWriterUtils.getLogWriter().info("Registered InterestRegistationLister");
    this.cacheServer.registerInterestRegistrationListener(listener);
  }

  public void setUpClientVM(String host, int port, boolean isDurable, String vmID) {
    Properties gemFireProps = new Properties();
    if (isDurable) {
      gemFireProps.put(DURABLE_CLIENT_ID, vmID);
      gemFireProps.put(DURABLE_CLIENT_TIMEOUT, "" + DURABLE_CLIENT_TIMEOUT_TEST);
    }
    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(gemFireProps);
    clientCacheFactory.addPoolServer(host, port);
    clientCacheFactory.setPoolSubscriptionEnabled(true);
    clientCacheFactory.setPoolMinConnections(5);
    clientCache = clientCacheFactory.create();
    ClientRegionFactory<String, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Region<String, String> region = regionFactory.create("serverRegion");

    LogWriterUtils.getLogWriter()
        .info("Client Cache is created in this vm connected to cacheServer " + host + ":" + port
            + " durable? " + isDurable + " with VMID=" + vmID + " region " + region.getFullPath()
            + " regionSize " + region.size());
    assertNotNull(clientCache);
    assertNotNull(region);
  }

  public static void setUpServerVMTask() throws Exception {
    instance.setUpServerVM();
  }

  public static void createServerTask() throws Exception {
    instance.createServer();
  }

  public static void setUpClientVMTask(String host, int port, boolean isDurable, String vmID)
      throws Exception {
    instance.setUpClientVM(host, port, isDurable, vmID);
  }

  public static Map<String, Integer> getListenerMapTask() throws Exception {
    return instance.listnerMap;
  }

  public static void doClientRegionPutTask() {
    instance.doClientRegionPut();
  }

  public static void doServerRegionPutTask() {
    instance.doServerRegionPut();
  }

  public static void doExpressInterestOnServerTask(boolean isDurable) {
    instance.doExpressInterestOnServer(isDurable);
  }

  public static void doRegisterListenerTask() {
    instance.doRegisterListener();
  }

  public static Object[] getCacheServerEndPointTask() {
    Object[] array = new Object[2];
    array[0] = instance.getCacheServerHost();
    array[1] = instance.getCacheServerPort();
    return array;
  }

  public static void closeCacheTask() {
    instance.cache.close();
  }

  public static void closeClientCacheTask(boolean keepAlive) {
    instance.clientCache.close(keepAlive);
  }

  public static void doClientRegionRegisterInterestTask(boolean isDurable) {
    instance.doClientRegionRegisterInterest(isDurable);
  }

  @Test
  public void testDurableClientExit_ClientExpressedInterest() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(0);
    VM clientVM_1 = host.getVM(1);
    VM clientVM_2 = host.getVM(2);
    VM clientVM_3 = host.getVM(3);

    serverVM.invoke(() -> InterestRegrListenerDUnitTest.setUpServerVMTask());
    serverVM.invoke(() -> InterestRegrListenerDUnitTest.createServerTask());

    Object array[] = (Object[]) serverVM
        .invoke(() -> InterestRegrListenerDUnitTest.getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[4];
    params[0] = hostName;
    params[1] = port;
    params[2] = true;
    params[3] = "VM_1";
    LogWriterUtils.getLogWriter()
        .info("Starting client1 with server endpoint <" + hostName + ">:" + port);
    clientVM_1.invoke(InterestRegrListenerDUnitTest.class, "setUpClientVMTask", params);

    params[3] = "VM_2";
    LogWriterUtils.getLogWriter()
        .info("Starting client2 with server endpoint <" + hostName + ">:" + port);
    clientVM_2.invoke(InterestRegrListenerDUnitTest.class, "setUpClientVMTask", params);

    params[3] = "VM_3";
    LogWriterUtils.getLogWriter()
        .info("Starting client2 with server endpoint <" + hostName + ">:" + port);
    clientVM_3.invoke(InterestRegrListenerDUnitTest.class, "setUpClientVMTask", params);


    params = new Object[1];
    params[0] = true;
    serverVM.invoke(() -> InterestRegrListenerDUnitTest.doRegisterListenerTask());

    clientVM_1.invoke(InterestRegrListenerDUnitTest.class, "doClientRegionRegisterInterestTask",
        params);
    clientVM_2.invoke(InterestRegrListenerDUnitTest.class, "doClientRegionRegisterInterestTask",
        params);
    clientVM_3.invoke(InterestRegrListenerDUnitTest.class, "doClientRegionRegisterInterestTask",
        params);

    clientVM_1.invoke(() -> InterestRegrListenerDUnitTest.doClientRegionPutTask());
    clientVM_2.invoke(() -> InterestRegrListenerDUnitTest.doClientRegionPutTask());
    clientVM_3.invoke(() -> InterestRegrListenerDUnitTest.doClientRegionPutTask());
    serverVM.invoke(() -> InterestRegrListenerDUnitTest.doServerRegionPutTask());

    clientVM_1.invoke(() -> InterestRegrListenerDUnitTest.closeClientCacheTask(true));
    clientVM_2.invoke(() -> InterestRegrListenerDUnitTest.closeClientCacheTask(true));
    clientVM_3.invoke(() -> InterestRegrListenerDUnitTest.closeClientCacheTask(true));
    Thread.sleep(2);
    Map<String, Integer> listnerMap = (Map<String, Integer>) serverVM
        .invoke(() -> InterestRegrListenerDUnitTest.getListenerMapTask());
    LogWriterUtils.getLogWriter().info("Listener Map " + listnerMap);
    int registerCount = getMapValueForKey(listnerMap, REGISTER_INTEREST);
    int unregisterCount = getMapValueForKey(listnerMap, UNREGISTER_INTEREST);
    assertEquals(3, registerCount);
    assertEquals(0, unregisterCount);
    LogWriterUtils.getLogWriter().info(
        "Sleeping till durable client queue are expired and unregister event is called on to listener");
    Thread.sleep((DURABLE_CLIENT_TIMEOUT_TEST + 5) * 1000);
    listnerMap = (Map<String, Integer>) serverVM
        .invoke(() -> InterestRegrListenerDUnitTest.getListenerMapTask());
    LogWriterUtils.getLogWriter().info("Listener Map after sleeping " + listnerMap);
    registerCount = getMapValueForKey(listnerMap, REGISTER_INTEREST);
    unregisterCount = getMapValueForKey(listnerMap, UNREGISTER_INTEREST);
    assertEquals(3, registerCount);
    assertEquals(3, unregisterCount);
    serverVM.invoke(() -> InterestRegrListenerDUnitTest.closeCacheTask());
  }

  @Test
  public void testDurableClientExit_ServerExpressedInterest() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(0);
    VM clientVM_1 = host.getVM(1);
    VM clientVM_2 = host.getVM(2);
    VM clientVM_3 = host.getVM(3);

    serverVM.invoke(() -> InterestRegrListenerDUnitTest.setUpServerVMTask());
    serverVM.invoke(() -> InterestRegrListenerDUnitTest.createServerTask());

    Object array[] = (Object[]) serverVM
        .invoke(() -> InterestRegrListenerDUnitTest.getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[4];
    params[0] = hostName;
    params[1] = port;
    params[2] = true;
    params[3] = "VM_1";
    LogWriterUtils.getLogWriter()
        .info("Starting client1 with server endpoint <" + hostName + ">:" + port);
    clientVM_1.invoke(InterestRegrListenerDUnitTest.class, "setUpClientVMTask", params);

    params[3] = "VM_2";
    LogWriterUtils.getLogWriter()
        .info("Starting client2 with server endpoint <" + hostName + ">:" + port);
    clientVM_2.invoke(InterestRegrListenerDUnitTest.class, "setUpClientVMTask", params);

    params[3] = "VM_3";
    LogWriterUtils.getLogWriter()
        .info("Starting client2 with server endpoint <" + hostName + ">:" + port);
    clientVM_3.invoke(InterestRegrListenerDUnitTest.class, "setUpClientVMTask", params);


    params = new Object[1];
    params[0] = true;
    serverVM.invoke(() -> InterestRegrListenerDUnitTest.doRegisterListenerTask());
    serverVM.invoke(InterestRegrListenerDUnitTest.class, "doExpressInterestOnServerTask", params);

    clientVM_1.invoke(() -> InterestRegrListenerDUnitTest.doClientRegionPutTask());
    clientVM_2.invoke(() -> InterestRegrListenerDUnitTest.doClientRegionPutTask());
    clientVM_3.invoke(() -> InterestRegrListenerDUnitTest.doClientRegionPutTask());
    serverVM.invoke(() -> InterestRegrListenerDUnitTest.doServerRegionPutTask());

    clientVM_1.invoke(() -> InterestRegrListenerDUnitTest.closeClientCacheTask(true));
    clientVM_2.invoke(() -> InterestRegrListenerDUnitTest.closeClientCacheTask(true));
    clientVM_3.invoke(() -> InterestRegrListenerDUnitTest.closeClientCacheTask(true));
    Thread.sleep(2);
    Map<String, Integer> listnerMap = (Map<String, Integer>) serverVM
        .invoke(() -> InterestRegrListenerDUnitTest.getListenerMapTask());
    LogWriterUtils.getLogWriter().info("Listener Map " + listnerMap);
    int registerCount = getMapValueForKey(listnerMap, REGISTER_INTEREST);
    int unregisterCount = getMapValueForKey(listnerMap, UNREGISTER_INTEREST);
    assertEquals(3, registerCount);
    assertEquals(0, unregisterCount);
    LogWriterUtils.getLogWriter().info(
        "Sleeping till durable client queue are expired and unregister event is called on to listener");
    Thread.sleep((DURABLE_CLIENT_TIMEOUT_TEST + 5) * 1000);
    listnerMap = (Map<String, Integer>) serverVM
        .invoke(() -> InterestRegrListenerDUnitTest.getListenerMapTask());
    LogWriterUtils.getLogWriter().info("Listener Map after sleeping " + listnerMap);
    registerCount = getMapValueForKey(listnerMap, REGISTER_INTEREST);
    unregisterCount = getMapValueForKey(listnerMap, UNREGISTER_INTEREST);
    assertEquals(3, registerCount);
    assertEquals(3, unregisterCount);
    serverVM.invoke(() -> InterestRegrListenerDUnitTest.closeCacheTask());
  }

  @Test
  public void testDurableClientExit_ServerExpressedInterest_NonDurableInterest() throws Exception {
    final Host host = getHost(0);
    final VM serverVM = host.getVM(0);
    final VM clientVM_1 = host.getVM(1);
    final VM clientVM_2 = host.getVM(2);
    final VM clientVM_3 = host.getVM(3);

    serverVM.invoke(() -> setUpServerVMTask());
    serverVM.invoke(() -> createServerTask());

    Object array[] = (Object[]) serverVM
        .invoke(() -> getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[4];
    params[0] = hostName;
    params[1] = port;
    params[2] = true;
    params[3] = "VM_1";
    getLogWriter()
        .info("Starting client1 with server endpoint <" + hostName + ">:" + port);
    clientVM_1.invoke(InterestRegrListenerDUnitTest.class, "setUpClientVMTask", params);

    params[3] = "VM_2";
    getLogWriter()
        .info("Starting client2 with server endpoint <" + hostName + ">:" + port);
    clientVM_2.invoke(InterestRegrListenerDUnitTest.class, "setUpClientVMTask", params);

    params[3] = "VM_3";
    getLogWriter()
        .info("Starting client2 with server endpoint <" + hostName + ">:" + port);
    clientVM_3.invoke(InterestRegrListenerDUnitTest.class, "setUpClientVMTask", params);


    params = new Object[1];
    params[0] = false; // non-durable interest

    serverVM.invoke(() -> doRegisterListenerTask());
    serverVM.invoke(InterestRegrListenerDUnitTest.class, "doExpressInterestOnServerTask", params);

    clientVM_1.invoke(() -> doClientRegionPutTask());
    clientVM_2.invoke(() -> doClientRegionPutTask());
    clientVM_3.invoke(() -> doClientRegionPutTask());
    serverVM.invoke(() -> doServerRegionPutTask());

    clientVM_1.invoke(() -> closeClientCacheTask(false));
    clientVM_2.invoke(() -> closeClientCacheTask(false));
    clientVM_3.invoke(() -> closeClientCacheTask(false));

    sleep(2);

    WaitCriterion wc = new WaitCriterion() {
      int registerCount = 0;
      int unregisterCount = 0;

      @Override
      public boolean done() {
        Map<String, Integer> listnerMap = (Map<String, Integer>) serverVM
            .invoke(() -> getListenerMapTask());
        getLogWriter().info("Listener Map " + listnerMap);
        registerCount = getMapValueForKey(listnerMap, REGISTER_INTEREST);
        unregisterCount = getMapValueForKey(listnerMap, UNREGISTER_INTEREST);
        if (registerCount == 3 && unregisterCount == 3) {
          return true;
        }
        getLogWriter()
            .info("Waiting for counts to each reach 3.  Current registerCount=" + registerCount
                + "; unregisterCount=" + unregisterCount);
        return false;
      }

      @Override
      public String description() {
        return "Waiting for counts to each reach 3.  Current registerCount=" + registerCount
            + "; unregisterCount=" + unregisterCount;
      }
    };

    GeodeAwaitility.await().untilAsserted(wc);

    getLogWriter().info(
        "Sleeping till durable client queue are expired and unregister event is called on to listener");
    sleep((DURABLE_CLIENT_TIMEOUT_TEST + 5) * 1000);
    serverVM.invoke(() -> closeCacheTask());
  }

  private int getMapValueForKey(Map<String, Integer> map, String key) {
    if (map.containsKey(key))
      return map.get(key).intValue();
    else
      return 0;
  }

}
