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

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocationAndMemberId;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListenerAdapter;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Tests cases that are particular for the auto connection source - dynamically discovering servers,
 * locators, handling locator disappearance, etc.
 */
@Category({ClientServerTest.class})
public class AutoConnectionSourceDUnitTest extends LocatorTestBase {
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final Object BRIDGE_LISTENER = "BRIDGE_LISTENER";
  private static final long MAX_WAIT = 60000;

  @Override
  public final void postSetUp() {
    addIgnoredException("NoAvailableLocatorsException");
  }

  @After
  public void teardown() {
    VM.getVM(0).bounceForcibly();
    VM.getVM(1).bounceForcibly();
    VM.getVM(2).bounceForcibly();
    VM.getVM(3).bounceForcibly();
  }

  public AutoConnectionSourceDUnitTest() {
    super();
  }

  @Test
  public void testDiscoverBridgeServers() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);

    String hostName = getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));

    String locators = getServerHostName() + "[" + locatorPort + "]";

    vm1.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    vm2.invoke("StartBridgeClient",
        () -> startBridgeClient(null, getServerHostName(), locatorPort));

    putAndWaitForSuccess(vm2, REGION_NAME, KEY, VALUE);

    Assert.assertEquals(VALUE, getInVM(vm1, KEY));
  }

  @Test
  public void testNoLocators() {

    VM vm0 = VM.getVM(0);

    try {
      vm0.invoke("StartBridgeClient",
          () -> startBridgeClient(null, getServerHostName(), getRandomAvailableTCPPort()));
      checkLocators(vm0, new InetSocketAddress[] {}, new InetSocketAddress[] {});
      putInVM(vm0);
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
      putInVM(vm0);
      fail("Client cache should not have been able to start");
    } catch (Exception e) {
      // expected an exception
    }
  }

  @Test
  public void testDynamicallyFindBridgeServer() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);

    String hostName = getServerHostName();
    int locatorPort = vm0.invoke("Start Locator", () -> startLocator(hostName, ""));

    String locators = getLocatorString(hostName, locatorPort);

    vm1.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    vm2.invoke("StartBridgeClient",
        () -> startBridgeClient(null, getServerHostName(), locatorPort));

    putAndWaitForSuccess(vm2, REGION_NAME, KEY, VALUE);

    vm3.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    stopBridgeMemberVM(vm1);

    putAndWaitForSuccess(vm2, REGION_NAME, "key2", "value2");

    Assert.assertEquals("value2", getInVM(vm3, "key2"));
  }

  @Test
  public void testClientDynamicallyFindsNewLocator() {
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

    putAndWaitForSuccess(clientVM, REGION_NAME, KEY, VALUE);
    Assert.assertEquals(VALUE, getInVM(serverVM, KEY));

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
    assertThat(locator0Port).isGreaterThan(0);
    assertThat(locator1Port).isGreaterThan(0);

    startBridgeClient(null, hostName, locator0Port);
    InetSocketAddress locatorToWaitFor = new InetSocketAddress(hostName, locator1Port);
    MyLocatorCallback callback = (MyLocatorCallback) remoteObjects.get(CALLBACK_KEY);

    boolean discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
    Assert.assertTrue(
        "Waited " + MAX_WAIT + " for " + locatorToWaitFor
            + " to be discovered on client. List is now: " + callback.getDiscovered(),
        discovered);

    InetSocketAddress[] initialLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostName, locator0Port)};

    InetSocketAddress[] expectedLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostName, locator0Port),
            new InetSocketAddress(hostName, locator1Port)};

    final Pool pool = PoolManager.find(POOL_NAME);

    verifyLocatorsMatched(initialLocators, pool.getLocators());

    verifyLocatorsMatched(expectedLocators, pool.getOnlineLocators());

    // stop one of the locators and ensure that the client can find and use a server
    locator0VM.invoke("Stop Locator", this::stopLocator);

    await().until(() -> pool.getOnlineLocators().size() == 1);

    int serverPort = serverVM.invoke("Start BridgeServer",
        () -> startBridgeServer(null, getLocatorString(hostName, locator1Port)));
    assertThat(serverPort).isGreaterThan(0);

    verifyLocatorsMatched(initialLocators, pool.getLocators());

    InetSocketAddress[] postShutdownLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostName, locator1Port)};
    verifyLocatorsMatched(postShutdownLocators, pool.getOnlineLocators());

    await().untilAsserted(
        () -> assertThatCode(
            () -> ((Cache) remoteObjects.get(CACHE_KEY)).getRegion(REGION_NAME).put(KEY, VALUE))
                .doesNotThrowAnyException());
    Assert.assertEquals(VALUE, getInVM(serverVM, KEY));

  }

  @Test
  public void testClientCanUseAnEmbeddedLocator() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    int locatorPort = getRandomAvailableTCPPort();

    String locators = getLocatorString(getServerHostName(), locatorPort);

    vm0.invoke("Start BridgeServer", () -> startBridgeServerWithEmbeddedLocator(null, locators,
        new String[] {REGION_NAME}, CacheServer.DEFAULT_LOAD_PROBE));

    vm1.invoke("StartBridgeClient",
        () -> startBridgeClient(null, getServerHostName(), locatorPort));

    putAndWaitForSuccess(vm1, REGION_NAME, KEY, VALUE);

    Assert.assertEquals(VALUE, getInVM(vm1, KEY));
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
    putAndWaitForSuccess(vm3, "A", KEY, VALUE);
    Assert.assertEquals(VALUE, getInVM(vm1, "A", KEY));
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
      putInVM(vm3, "A", "key3", VALUE);
      fail("Should not have been able to find Region A on the server");
    } catch (RMIException expected) {
      Throwable realCause = expected;
      while (realCause.getCause() != null) {
        realCause = realCause.getCause();
      }
      assertEquals("Found wrong exception: " + realCause, RegionDestroyedException.class,
          realCause.getClass());
    }
    putInVM(vm3, "C", "key4", VALUE);
    Assert.assertEquals(VALUE, getInVM(vm2, "C", "key4"));

    stopBridgeMemberVM(vm3);

    vm3.invoke("StartBridgeClient", () -> startBridgeClient("group2", getServerHostName(),
        locatorPort, new String[] {"A", "B", "C"}));
    putInVM(vm3, "B", "key5", VALUE);
    Assert.assertEquals(VALUE, getInVM(vm1, "B", "key5"));
    Assert.assertEquals(VALUE, getInVM(vm2, "B", "key5"));

    stopBridgeMemberVM(vm1);
    putInVM(vm3, "B", "key6", VALUE);
    Assert.assertEquals(VALUE, getInVM(vm2, "B", "key6"));
    vm1.invoke("Start BridgeServer", () -> startBridgeServer(new String[] {"group1", "group2"},
        locators, new String[] {"A", "B"}));
    stopBridgeMemberVM(vm2);

    putInVM(vm3, "B", "key7", VALUE);
    Assert.assertEquals(VALUE, getInVM(vm1, "B", "key7"));
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

    // start a cache server with a listener
    addBridgeListener(bridge1VM);
    int serverPort1 =
        bridge1VM.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    // start a bridge client with a listener
    addBridgeListener(clientVM);
    clientVM.invoke("StartBridgeClient",
        () -> startBridgeClient(null, getServerHostName(), locatorPort));
    // wait for client to connect
    checkEndpoints(clientVM, serverPort1);

    // make sure the client and cache server both noticed each other
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

    // start another cache server and make sure it is detected by the client
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

    // stop the second cache server and make sure it is detected by the client
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

    // stop the client and make sure the cache server notices
    stopBridgeMemberVM(clientVM);
    waitForDeparture(bridge1VM);
    serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(1, serverListener.getDepartures());
    Assert.assertEquals(0, serverListener.getJoins());
  }


  @Test
  public void testClientGetsLocatorListWithExternalAddress() throws Exception {
    final String hostName = getServerHostName();
    final String hostExternalAddress1 = "127.0.0.1";
    final String hostExternalAddress2 = "127.0.0.2";

    VM locator0VM = VM.getVM(0);
    VM locator1VM = VM.getVM(1);

    final int locator0Port =
        locator0VM.invoke("Start Locator1 ",
            () -> startLocator(hostName, "", hostExternalAddress1));
    final int locator1Port = locator1VM.invoke("Start Locator2 ",
        () -> startLocator(hostName, getLocatorString(hostName, locator0Port),
            hostExternalAddress2));
    assertThat(locator0Port).isGreaterThan(0);
    assertThat(locator1Port).isGreaterThan(0);

    startBridgeClient(null, hostName, locator0Port, false);
    InetSocketAddress locatorToWaitFor = new InetSocketAddress(hostExternalAddress2, locator1Port);
    MyLocatorCallback callback = (MyLocatorCallback) remoteObjects.get(CALLBACK_KEY);

    boolean discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
    Assert.assertTrue(
        "Waited " + MAX_WAIT + " for " + locatorToWaitFor
            + " to be discovered on client. List is now: " + callback.getDiscovered(),
        discovered);

    InetSocketAddress[] initialLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostName, locator0Port)};

    InetSocketAddress[] expectedLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostExternalAddress1, locator0Port),
            new InetSocketAddress(hostExternalAddress2, locator1Port)};

    final Pool pool = PoolManager.find(POOL_NAME);

    verifyLocatorsMatched(initialLocators, pool.getLocators());
    verifyLocatorsMatched(expectedLocators, pool.getOnlineLocators(), false);
  }

  @Test
  public void testClientGetsLocatorListWithInternalAddress() throws Exception {
    final String hostName = getServerHostName();
    final String hostExternalAddress1 = "127.0.0.1";
    final String hostExternalAddress2 = "127.0.0.2";

    VM locator0VM = VM.getVM(0);
    VM locator1VM = VM.getVM(1);

    final int locator0Port =
        locator0VM.invoke("Start Locator1 ",
            () -> startLocator(hostName, "", hostExternalAddress1));
    final int locator1Port = locator1VM.invoke("Start Locator2 ",
        () -> startLocator(hostName, getLocatorString(hostName, locator0Port),
            hostExternalAddress2));
    assertThat(locator0Port).isGreaterThan(0);
    assertThat(locator1Port).isGreaterThan(0);

    startBridgeClient(null, hostName, locator0Port, true);
    InetSocketAddress locatorToWaitFor = new InetSocketAddress(hostName, locator1Port);
    MyLocatorCallback callback = (MyLocatorCallback) remoteObjects.get(CALLBACK_KEY);

    boolean discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
    Assert.assertTrue(
        "Waited " + MAX_WAIT + " for " + locatorToWaitFor
            + " to be discovered on client. List is now: " + callback.getDiscovered(),
        discovered);

    InetSocketAddress[] initialLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostName, locator0Port)};

    InetSocketAddress[] expectedLocators =
        new InetSocketAddress[] {new InetSocketAddress(hostName, locator0Port),
            new InetSocketAddress(hostName, locator1Port)};

    final Pool pool = PoolManager.find(POOL_NAME);

    verifyLocatorsMatched(initialLocators, pool.getLocators());
    verifyLocatorsMatched(expectedLocators, pool.getOnlineLocators());
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
    await().untilAsserted(
        () -> assertThatCode(() -> putInVM(vm, regionName, key, value)).doesNotThrowAnyException());
  }

  private void putInVM(VM vm) {
    putInVM(vm, REGION_NAME, KEY, VALUE);
  }

  private void putInVM(VM vm, final String regionName, final Serializable key,
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
  private void checkEndpoints(VM vm, final int... expectedPorts) {
    vm.invoke("Check endpoint", () -> {
      PoolImpl pool = (PoolImpl) PoolManager.find(POOL_NAME);
      HashSet<Integer> expectedEndpointPorts = new HashSet<>();
      for (int expectedPort : expectedPorts) {
        expectedEndpointPorts.add(expectedPort);
      }
      await().untilAsserted(() -> {
        List<ServerLocationAndMemberId> endpoints;
        HashSet<Integer> actualEndpointPorts;
        endpoints = pool.getCurrentServers();
        actualEndpointPorts = new HashSet<>();
        for (ServerLocationAndMemberId slAndMemberId : endpoints) {
          actualEndpointPorts.add(slAndMemberId.getServerLocation().getPort());
        }
        assertEquals(expectedEndpointPorts, actualEndpointPorts);
      });
    });
  }

  private Boolean verifyLocatorsMatched(InetSocketAddress[] expected,
      List<InetSocketAddress> initialLocators) {
    return verifyLocatorsMatched(expected, initialLocators, true);
  }

  /**
   * Assert that Sets of locators are the same.
   *
   * In case locators have different hostnames, no need to sort Set according to ports.
   * So parameter sortSet can be set to false.
   */
  private Boolean verifyLocatorsMatched(InetSocketAddress[] expected,
      List<InetSocketAddress> initialLocators, boolean sortSet) {

    if (expected.length != initialLocators.size()) {
      return false;
    }

    if (sortSet) {
      Arrays.sort(expected, Comparator.comparing(InetSocketAddress::getPort));
    }

    assertThat(initialLocators).containsExactly(expected);

    for (int i = 0; i < initialLocators.size(); i++) {
      InetSocketAddress locatorAddress = initialLocators.get(i);
      InetSocketAddress expectedAddress = expected[i];

      if (!expectedAddress.equals(locatorAddress)) {
        assertThat(locatorAddress).isEqualTo(expectedAddress);
        return false;
      }
    }
    return true;
  }

  private void checkLocators(VM vm, final InetSocketAddress[] expectedInitial,
      final InetSocketAddress... expected) {
    vm.invoke("Check locators", () -> {
      Pool pool = PoolManager.find(POOL_NAME);

      verifyLocatorsMatched(expectedInitial, pool.getLocators());

      verifyLocatorsMatched(expected, pool.getOnlineLocators());
    });
  }

  private void addBridgeListener(VM vm) {
    vm.invoke("Add membership listener", () -> {
      MyListener listener = new MyListener();
      ClientMembership.registerClientMembershipListener(listener);
      remoteObjects.put(BRIDGE_LISTENER, listener);
    });
  }

  private void resetBridgeListener(VM vm) {
    vm.invoke("Reset membership listener", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      listener.reset();
    });
  }

  private MyListener getBridgeListener(VM vm) {
    return (MyListener) vm
        .invoke("Get membership listener", () -> remoteObjects.get(BRIDGE_LISTENER));
  }

  private void waitForJoin(VM vm) {
    vm.invoke("wait for join", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      try {
        await().until(() -> listener.getJoins() > 0);
      } catch (ConditionTimeoutException e) {
        // do nothing here - caller will perform validations
      }
    });
  }

  private void waitForCrash(VM vm) {
    vm.invoke("wait for crash", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      try {
        await().until(() -> listener.getCrashes() > 0);
      } catch (ConditionTimeoutException e) {
        // do nothing here - caller will perform validations
      }
    });
  }

  private void waitForDeparture(VM vm) {
    vm.invoke("wait for departure", () -> {
      MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
      try {
        await().until(() -> listener.getDepartures() > 0);
      } catch (ConditionTimeoutException e) {
        // do nothing here - caller will perform validations
      }
    });
  }

  static class MyListener extends ClientMembershipListenerAdapter implements Serializable {
    volatile int crashes = 0;
    volatile int joins = 0;
    volatile int departures = 0;

    @Override
    public synchronized void memberCrashed(ClientMembershipEvent event) {
      crashes++;
      System.out.println("memberCrashed invoked");
      notifyAll();
    }

    synchronized void reset() {
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

    synchronized int getCrashes() {
      return crashes;
    }

    synchronized int getJoins() {
      return joins;
    }

    synchronized int getDepartures() {
      return departures;
    }
  }
}
