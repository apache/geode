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
package org.apache.geode.cache.client.internal;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListenerAdapter;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests cases that are particular for the auto connection source - dynamically discovering servers,
 * locators, handling locator disappearance, etc.
 */
@Category({DistributedTest.class, ClientServerTest.class})
public class AutoConnectionSourceDUnitTest extends LocatorTestBase {

  protected static final Object BRIDGE_LISTENER = "BRIDGE_LISTENER";
  private static final long MAX_WAIT = 60000;

  @Override
  public final void postSetUp() throws Exception {
    IgnoredException.addIgnoredException("NoAvailableLocatorsException");
  }

  public AutoConnectionSourceDUnitTest() {
    super();
  }

  @Test
  public void testDiscoverBridgeServers() throws Exception {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);

    String hostName = getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));

    String locators = getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";

    vm1.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    vm2.invoke("StartBridgeClient",
        () -> startBridgeClient(null, getServerHostName(vm0.getHost()), locatorPort));

    putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");

    Assert.assertEquals("value", getInVM(vm1, "key"));
  }

  @Test
  public void testNoLocators() {

    VM vm0 = VM.getVM(0);

    try {
      vm0.invoke("StartBridgeClient",
          () -> startBridgeClient(null, getServerHostName(vm0.getHost()),
              AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET)));
      checkLocators(vm0, new InetSocketAddress[] {}, new InetSocketAddress[] {});
      putInVM(vm0, "key", "value");
      fail("Client cache should not have been able to start");
    } catch (Exception e) {
      // expected an exception
    }
  }

  @Test
  public void testNoBridgeServer() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    String hostName = getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));
    try {
      vm1.invoke("StartBridgeClient", () -> startBridgeClient(null, hostName, locatorPort));
      putInVM(vm0, "key", "value");
      fail("Client cache should not have been able to start");
    } catch (Exception e) {
      // expected an exception
    }
  }

  @Test
  public void testDynamicallyFindBridgeServer() throws Exception {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);

    String hostName = getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));

    String locators = getLocatorString(hostName, locatorPort);

    vm1.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    vm2.invoke("StartBridgeClient",
        () -> startBridgeClient(null, getServerHostName(vm0.getHost()), locatorPort));

    putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");

    vm3.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    stopBridgeMemberVM(vm1);

    putAndWaitForSuccess(vm2, REGION_NAME, "key2", "value2");

    Assert.assertEquals("value2", getInVM(vm3, "key2"));
  }

  @Test
  public void testClientDynamicallyFindsNewLocator() throws Exception {
    final String hostName = getServerHostName();
    VM locator0VM = VM.getVM(0);
    VM locator1VM = VM.getVM(1);
    VM clientVM = VM.getVM(2);
    VM serverVM = VM.getVM(3);

    final int locator0Port = locator0VM.invoke("Start Locator1 ", () -> startLocator(hostName, ""));

    clientVM.invoke("StartBridgeClient", () -> startBridgeClient(null, hostName, locator0Port));

    final int locator1Port = locator1VM.invoke("Start Locator2 ",
        () -> startLocator(hostName, getLocatorString(hostName, locator0Port)));

    serverVM.invoke("Start BridgeServer",
        () -> startBridgeServer(null, getLocatorString(hostName, locator0Port, locator1Port)));

    putAndWaitForSuccess(clientVM, REGION_NAME, "key", "value");
    Assert.assertEquals("value", getInVM(serverVM, "key"));

    InetSocketAddress locatorToWaitFor = new InetSocketAddress(hostName, locator1Port);
    waitForLocatorDiscovery(clientVM, locatorToWaitFor);
  }

  @Test
  public void testClientDynamicallyDropsStoppedLocator() throws Exception {
    final String hostName = getServerHostName();
    VM locator0VM = VM.getVM(0);
    VM locator1VM = VM.getVM(1);
    VM clientVM = VM.getVM(2);
    VM serverVM = VM.getVM(3);

    final int locator0Port = locator0VM.invoke("Start Locator1 ", () -> startLocator(hostName, ""));
    final int locator1Port = locator1VM.invoke("Start Locator2 ",
        () -> startLocator(hostName, getLocatorString(hostName, locator0Port)));

    clientVM.invoke("StartBridgeClient", () -> startBridgeClient(null, hostName, locator0Port));

    waitForLocatorDiscovery(clientVM, new InetSocketAddress(hostName, locator1Port));

    InetSocketAddress[] initialLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostName, locator0Port)};

    checkLocators(clientVM, initialLocators, new InetSocketAddress(hostName, locator0Port),
        new InetSocketAddress(hostName, locator1Port));

    // stop one of the locators and ensure that the client can find and use a server
    locator0VM.invoke("Stop Locator", this::stopLocator);

    serverVM.invoke("Start BridgeServer",
        () -> startBridgeServer(null, getLocatorString(hostName, locator1Port)));

    putAndWaitForSuccess(clientVM, REGION_NAME, "key", "value");
    Assert.assertEquals("value", getInVM(serverVM, "key"));

    checkLocators(clientVM, initialLocators, new InetSocketAddress(hostName, locator1Port));

  }

  @Test
  public void testClientCanUseAnEmbeddedLocator() throws Exception {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    String locators = getLocatorString(getServerHostName(), locatorPort);

    vm0.invoke("Start BridgeServer", () -> startBridgeServerWithEmbeddedLocator(null, locators,
        new String[] {REGION_NAME}, CacheServer.DEFAULT_LOAD_PROBE));

    vm1.invoke("StartBridgeClient",
        () -> startBridgeClient(null, getServerHostName(), locatorPort));

    putAndWaitForSuccess(vm1, REGION_NAME, "key", "value");

    Assert.assertEquals("value", getInVM(vm1, "key"));
  }

  private void waitForLocatorDiscovery(VM vm, final InetSocketAddress locatorToWaitFor) {
    vm.invoke(() -> {
      MyLocatorCallback callback = (MyLocatorCallback) remoteObjects.get(CALLBACK_KEY);

      boolean discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
      Assert.assertTrue(
          "Waited " + MAX_WAIT + " for " + locatorToWaitFor
              + " to be discovered on client. List is now: " + callback.getDiscovered(),
          discovered);
      return null;
    });
  }

  @Test
  public void testClientFindsServerGroups() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);

    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(getServerHostName(), ""));

    String locators = getLocatorString(getServerHostName(), locatorPort);

    vm1.invoke("Start BridgeServer", () -> startBridgeServer(new String[] {"group1", "group2"},
        locators, new String[] {"A", "B"}));
    vm2.invoke("Start BridgeServer", () -> startBridgeServer(new String[] {"group2", "group3"},
        locators, new String[] {"B", "C"}));

    vm3.invoke("StartBridgeClient", () -> startBridgeClient("group1", getServerHostName(),
        locatorPort, new String[] {"A", "B", "C"}));
    putAndWaitForSuccess(vm3, "A", "key", "value");
    Assert.assertEquals("value", getInVM(vm1, "A", "key"));
    try {
      putInVM(vm3, "C", "key2", "value2");
      fail("Should not have been able to find Region C on the server");
    } catch (RMIException expected) {
      Throwable realCause = expected;
      while (realCause.getCause() != null) {
        realCause = realCause.getCause();
      }
      assertEquals("Found wrong exception: " + realCause, RegionDestroyedException.class,
          realCause.getClass());
    }

    stopBridgeMemberVM(vm3);

    vm3.invoke("StartBridgeClient", () -> startBridgeClient("group3", getServerHostName(),
        locatorPort, new String[] {"A", "B", "C"}));
    try {
      putInVM(vm3, "A", "key3", "value");
      fail("Should not have been able to find Region A on the server");
    } catch (RMIException expected) {
      Throwable realCause = expected;
      while (realCause.getCause() != null) {
        realCause = realCause.getCause();
      }
      assertEquals("Found wrong exception: " + realCause, RegionDestroyedException.class,
          realCause.getClass());
    }
    putInVM(vm3, "C", "key4", "value");
    Assert.assertEquals("value", getInVM(vm2, "C", "key4"));

    stopBridgeMemberVM(vm3);

    vm3.invoke("StartBridgeClient", () -> startBridgeClient("group2", getServerHostName(),
        locatorPort, new String[] {"A", "B", "C"}));
    putInVM(vm3, "B", "key5", "value");
    Assert.assertEquals("value", getInVM(vm1, "B", "key5"));
    Assert.assertEquals("value", getInVM(vm2, "B", "key5"));

    stopBridgeMemberVM(vm1);
    putInVM(vm3, "B", "key6", "value");
    Assert.assertEquals("value", getInVM(vm2, "B", "key6"));
    vm1.invoke("Start BridgeServer", () -> startBridgeServer(new String[] {"group1", "group2"},
        locators, new String[] {"A", "B"}));
    stopBridgeMemberVM(vm2);

    putInVM(vm3, "B", "key7", "value");
    Assert.assertEquals("value", getInVM(vm1, "B", "key7"));
  }

  @Test
  public void testTwoServersInSameVM() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);

    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(getServerHostName(), ""));

    final String locators = getLocatorString(getServerHostName(), locatorPort);

    final int serverPort1 =
        vm1.invoke("Start Server", () -> startBridgeServer(new String[] {"group1"}, locators));
    final int serverPort2 =
        vm1.invoke("Start Server", () -> addCacheServer(new String[] {"group2"}));

    vm2.invoke("Start Client", () -> startBridgeClient("group2", getServerHostName(), locatorPort));

    checkEndpoints(vm2, serverPort2);

    stopBridgeMemberVM(vm2);

    vm2.invoke("Start Client", () -> startBridgeClient("group1", getServerHostName(), locatorPort));

    checkEndpoints(vm2, serverPort1);
  }

  @Test
  public void testClientMembershipListener() {
    VM locatorVM = VM.getVM(0);
    VM bridge1VM = VM.getVM(1);
    VM bridge2VM = VM.getVM(2);
    VM clientVM = VM.getVM(3);
    int locatorPort =
        locatorVM.invoke("Start Locator", () -> startLocator(getServerHostName(), ""));

    String locators = getLocatorString(getServerHostName(), locatorPort);

    // start a bridge server with a listener
    addBridgeListener(bridge1VM);
    int serverPort1 =
        bridge1VM.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    // start a bridge client with a listener
    addBridgeListener(clientVM);
    clientVM.invoke("StartBridgeClient", () -> {
      startBridgeClient(null, getServerHostName(), locatorPort);
    });
    // wait for client to connect
    checkEndpoints(clientVM, serverPort1);

    // make sure the client and bridge server both noticed each other
    waitForJoin(bridge1VM);
    MyListener serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(0, serverListener.getDepartures());
    Assert.assertEquals(1, serverListener.getJoins());
    resetBridgeListener(bridge1VM);

    waitForJoin(clientVM);
    MyListener clientListener = getBridgeListener(clientVM);
    Assert.assertEquals(0, clientListener.getCrashes());
    Assert.assertEquals(0, clientListener.getDepartures());
    Assert.assertEquals(1, clientListener.getJoins());
    resetBridgeListener(clientVM);

    checkEndpoints(clientVM, serverPort1);

    // start another bridge server and make sure it is detected by the client
    int serverPort2 =
        bridge2VM.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    checkEndpoints(clientVM, serverPort1, serverPort2);
    serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(0, serverListener.getDepartures());
    Assert.assertEquals(0, serverListener.getJoins());
    resetBridgeListener(bridge1VM);
    waitForJoin(clientVM);
    clientListener = getBridgeListener(clientVM);
    Assert.assertEquals(0, clientListener.getCrashes());
    Assert.assertEquals(0, clientListener.getDepartures());
    Assert.assertEquals(1, clientListener.getJoins());
    resetBridgeListener(clientVM);

    // stop the second bridge server and make sure it is detected by the client
    stopBridgeMemberVM(bridge2VM);

    checkEndpoints(clientVM, serverPort1);
    serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(0, serverListener.getDepartures());
    Assert.assertEquals(0, serverListener.getJoins());
    resetBridgeListener(bridge1VM);
    waitForCrash(clientVM);
    clientListener = getBridgeListener(clientVM);
    Assert.assertEquals(0, clientListener.getJoins());
    Assert.assertEquals(1, clientListener.getDepartures() + clientListener.getCrashes());
    resetBridgeListener(clientVM);

    // stop the client and make sure the bridge server notices
    stopBridgeMemberVM(clientVM);
    waitForDeparture(bridge1VM);
    serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(1, serverListener.getDepartures());
    Assert.assertEquals(0, serverListener.getJoins());
  }

  private Object getInVM(VM vm, final Serializable key) {
    return getInVM(vm, REGION_NAME, key);
  }

  private Object getInVM(VM vm, final String regionName, final Serializable key) {
    return vm.invoke("Get in VM", () -> {
      Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
      Region region = cache.getRegion(regionName);
      return region.get(key);
    });
  }

  private void putAndWaitForSuccess(VM vm, final String regionName, final Serializable key,
      final Serializable value) {
    Awaitility.await().atMost(MAX_WAIT, MILLISECONDS).until(() -> {
      putInVM(vm, regionName, key, value);
    });
  }

  protected void putInVM(VM vm, final Serializable key, final Serializable value) {
    putInVM(vm, REGION_NAME, key, value);
  }

  protected void putInVM(VM vm, final String regionName, final Serializable key,
      final Serializable value) {
    vm.invoke("Put in VM",
        () -> ((Cache) remoteObjects.get(CACHE_KEY)).getRegion(regionName).put(key, value));
  }

  /**
   * Assert that there is one endpoint with the given host in port on the client vm.
   *
   * @param vm - the vm the client is running in
   * @param expectedPorts - The server ports we expect the client to be connected to.
   */
  protected void checkEndpoints(VM vm, final int... expectedPorts) {
    vm.invoke("Check endpoint", () -> {
      PoolImpl pool = (PoolImpl) PoolManager.find(POOL_NAME);
      HashSet expectedEndpointPorts = new HashSet();
      for (int i = 0; i < expectedPorts.length; i++) {
        expectedEndpointPorts.add(new Integer(expectedPorts[i]));
      }
      Awaitility.await().atMost(5, SECONDS).until(() -> {
        List<ServerLocation> endpoints;
        HashSet actualEndpointPorts;
        endpoints = pool.getCurrentServers();
        actualEndpointPorts = new HashSet();
        for (Iterator itr = endpoints.iterator(); itr.hasNext();) {
          ServerLocation sl = (ServerLocation) itr.next();
          actualEndpointPorts.add(new Integer(sl.getPort()));
        }
        assertEquals(expectedEndpointPorts, actualEndpointPorts);
      });
    });
  }

  protected void checkLocators(VM vm, final InetSocketAddress[] expectedInitial,
      final InetSocketAddress... expected) {
    vm.invoke("Check locators", () -> {
      Pool pool = PoolManager.find(POOL_NAME);

      List<InetSocketAddress> initialLocators = pool.getLocators();
      Assert.assertEquals(expectedInitial.length, initialLocators.size());
      Arrays.sort(expectedInitial, Comparator.comparing(InetSocketAddress::getPort));
      for (int i = 0; i < initialLocators.size(); i++) {
        InetSocketAddress locatorAddress = initialLocators.get(i);
        InetSocketAddress expectedAddress = expectedInitial[i];
        Assert.assertEquals(expectedAddress, locatorAddress);
      }

      List<InetSocketAddress> locators = pool.getOnlineLocators();
      Assert.assertEquals("found " + locators, expected.length, locators.size());
      Arrays.sort(expected, Comparator.comparing(InetSocketAddress::getPort));
      for (int i = 0; i < locators.size(); i++) {
        InetSocketAddress locatorAddress = locators.get(i);
        InetSocketAddress expectedAddress = expected[i];
        Assert.assertEquals(expectedAddress, locatorAddress);
      }
    });
  }

  protected void addBridgeListener(VM vm) {
    vm.invoke("Add membership listener", () -> {
      MyListener listener = new MyListener();
      ClientMembership.registerClientMembershipListener(listener);
      remoteObjects.put(BRIDGE_LISTENER, listener);
    });
  }

  protected void resetBridgeListener(VM vm) {
    vm.invoke("Reset membership listener", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      listener.reset();
    });
  }

  private MyListener getBridgeListener(VM vm) {
    return (MyListener) vm.invoke("Get membership listener", () -> {
      return remoteObjects.get(BRIDGE_LISTENER);
    });
  }

  private void waitForJoin(VM vm) {
    vm.invoke("wait for join", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      try {
        Awaitility.await().atMost(10, SECONDS).until(() -> listener.getJoins() > 0);
      } catch (ConditionTimeoutException e) {
        // do nothing here - caller will perform validations
      }
    });
  }

  private void waitForCrash(VM vm) {
    vm.invoke("wait for crash", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      try {
        Awaitility.await().atMost(10, SECONDS).until(() -> listener.getCrashes() > 0);
      } catch (ConditionTimeoutException e) {
        // do nothing here - caller will perform validations
      }
    });
  }

  private void waitForDeparture(VM vm) {
    vm.invoke("wait for departure", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      try {
        Awaitility.await().atMost(10, SECONDS).until(() -> listener.getDepartures() > 0);
      } catch (ConditionTimeoutException e) {
        // do nothing here - caller will perform validations
      }
    });
  }

  public static class MyListener extends ClientMembershipListenerAdapter implements Serializable {
    protected volatile int crashes = 0;
    protected volatile int joins = 0;
    protected volatile int departures = 0;

    @Override
    public synchronized void memberCrashed(ClientMembershipEvent event) {
      crashes++;
      System.out.println("memberCrashed invoked");
      notifyAll();
    }

    public synchronized void reset() {
      crashes = 0;
      joins = 0;
      departures = 0;
    }

    @Override
    public synchronized void memberJoined(ClientMembershipEvent event) {
      joins++;
      System.out.println("memberJoined invoked");
      notifyAll();
    }

    @Override
    public synchronized void memberLeft(ClientMembershipEvent event) {
      departures++;
      System.out.println("memberLeft invoked");
      notifyAll();
    }

    public synchronized int getCrashes() {
      return crashes;
    }

    public synchronized int getJoins() {
      return joins;
    }

    public synchronized int getDepartures() {
      return departures;
    }
  }
}
