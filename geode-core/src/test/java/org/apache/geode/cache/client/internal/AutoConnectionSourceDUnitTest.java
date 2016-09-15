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
package org.apache.geode.cache.client.internal;

import static org.junit.Assert.*;

import java.io.*;
import java.net.*;
import java.util.*;

import org.junit.Assert;
import org.junit.*;
import org.junit.experimental.categories.*;

import org.apache.geode.cache.*;
import org.apache.geode.cache.client.*;
import org.apache.geode.cache.server.*;
import org.apache.geode.distributed.internal.*;
import org.apache.geode.internal.*;
import org.apache.geode.internal.cache.*;
import org.apache.geode.management.membership.*;
import org.apache.geode.test.dunit.*;
import org.apache.geode.test.junit.categories.*;

/**
 * Tests cases that are particular for the auto connection source
 * - dynamically discovering servers, locators, handling
 * locator disappearance, etc.
 */
@Category(DistributedTest.class)
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
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    String hostName = NetworkUtils.getServerHostName(vm0.getHost());
    vm0.invoke("Start Locator", () -> startLocator(hostName, locatorPort, ""));

    String locators = NetworkUtils.getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";

    vm1.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    vm2.invoke("StartBridgeClient", () -> startBridgeClient(null, NetworkUtils.getServerHostName(vm0.getHost()), locatorPort));

    putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");

    Assert.assertEquals("value", getInVM(vm1, "key"));
  }

  @Test
  public void testNoLocators() {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    try {
      vm0.invoke("StartBridgeClient", () -> startBridgeClient(null, NetworkUtils.getServerHostName(vm0.getHost())
          , AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET)));
      putInVM(vm0, "key", "value");
      fail("Client cache should not have been able to start");
    } catch (Exception e) {
      //expected an exception
    }
  }

  @Test
  public void testNoBridgeServer() {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    String hostName = NetworkUtils.getServerHostName(vm0.getHost());
    vm0.invoke("Start Locator", () -> startLocator(hostName, locatorPort, ""));
    try {
      vm1.invoke("StartBridgeClient", () -> startBridgeClient(null, NetworkUtils.getServerHostName(vm0.getHost()), locatorPort));
      putInVM(vm0, "key", "value");
      fail("Client cache should not have been able to start");
    } catch (Exception e) {
      //expected an exception
    }
  }

  @Test
  public void testDynamicallyFindBridgeServer() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    String hostName = NetworkUtils.getServerHostName(vm0.getHost());
    vm0.invoke("Start Locator", () -> startLocator(hostName, locatorPort, ""));

    String locators = NetworkUtils.getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";

    vm1.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    vm2.invoke("StartBridgeClient", () -> startBridgeClient(null, NetworkUtils.getServerHostName(vm0.getHost()), locatorPort));

    putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");

    vm3.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    stopBridgeMemberVM(vm1);

    putAndWaitForSuccess(vm2, REGION_NAME, "key2", "value2");

    Assert.assertEquals("value2", getInVM(vm3, "key2"));
  }

  @Test
  public void testDynamicallyFindLocators() throws Exception {
    try {
      final Host host = Host.getHost(0);
      final String hostName = NetworkUtils.getServerHostName(host);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      VM vm2 = host.getVM(2);
      VM vm3 = host.getVM(3);

      int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);

      final int locatorPort0 = ports[0];
      final int locatorPort1 = ports[1];
      final int locatorPort3 = ports[2];
      String locators = getLocatorString(host, new int[] { locatorPort0, locatorPort1, locatorPort3 });
      vm0.invoke("Start Locator", () -> startLocator(NetworkUtils.getServerHostName(vm0.getHost()), locatorPort0, locators));
      vm1.invoke("Start Locator", () -> startLocator(NetworkUtils.getServerHostName(vm1.getHost()), locatorPort1, locators));

      vm2.invoke("StartBridgeClient", () -> startBridgeClient(null, NetworkUtils.getServerHostName(vm0.getHost()), locatorPort0));

      InetSocketAddress locatorToWaitFor = new InetSocketAddress(hostName, locatorPort1);
      waitForLocatorDiscovery(vm2, locatorToWaitFor);

      vm0.invoke("Stop Locator", () -> stopLocator());
      vm0.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

      putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");
      Assert.assertEquals("value", getInVM(vm0, "key"));

      vm3.invoke("Start Locator", () -> startLocator(NetworkUtils.getServerHostName(vm3.getHost()), locatorPort3, locators));
      stopBridgeMemberVM(vm0);
      locatorToWaitFor = new InetSocketAddress(hostName, locatorPort3);
      waitForLocatorDiscovery(vm2, locatorToWaitFor);
      vm1.invoke("Stop Locator", () -> stopLocator());
      vm1.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));
      putAndWaitForSuccess(vm2, REGION_NAME, "key2", "value2");
      Assert.assertEquals("value2", getInVM(vm1, "key2"));
    } catch (Exception ec) {
      if (ec.getCause() != null && (ec.getCause().getCause() instanceof BindException))
        return;//BindException let it pass
      throw ec;
    }
  }

  @Test
  public void testEmbeddedLocator() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    String locators = NetworkUtils.getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";

    vm0.invoke("Start BridgeServer", () -> startBridgeServerWithEmbeddedLocator(null, locators, new String[] { REGION_NAME }
        , CacheServer.DEFAULT_LOAD_PROBE));

    vm2.invoke("StartBridgeClient", () -> startBridgeClient(null, NetworkUtils.getServerHostName(vm0.getHost()), locatorPort));

    putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");

    Assert.assertEquals("value", getInVM(vm2, "key"));
  }

  private void waitForLocatorDiscovery(VM vm,
      final InetSocketAddress locatorToWaitFor) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws InterruptedException {
        MyLocatorCallback callback = (MyLocatorCallback) remoteObjects.get(CALLBACK_KEY);

        boolean discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
        Assert.assertTrue("Waited " + MAX_WAIT + " for " + locatorToWaitFor
            + " to be discovered on client. List is now: "
            + callback.getDiscovered(), discovered);
        return null;
      }
    });
  }

  @Test
  public void testServerGroups() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    vm0.invoke("Start Locator", () -> startLocator(NetworkUtils.getServerHostName(vm0.getHost()), locatorPort, ""));

    String locators = NetworkUtils.getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";

    vm1.invoke("Start BridgeServer", () -> startBridgeServer(new String[] { "group1", "group2" }, locators, new String[] { "A", "B" }));
    vm2.invoke("Start BridgeServer", () -> startBridgeServer(new String[] { "group2", "group3" }, locators, new String[] { "B", "C" }));

    vm3.invoke("StartBridgeClient", () -> startBridgeClient("group1", NetworkUtils.getServerHostName(vm0.getHost())
        , locatorPort, new String[] { "A", "B", "C" }));
    putAndWaitForSuccess(vm3, "A", "key", "value");
    Assert.assertEquals("value", getInVM(vm1, "A", "key"));
    try {
      putInVM(vm3, "C", "key2", "value2");
      fail("Should not have been able to find Region C on the server");
    } catch (Exception expected) {
    }

    stopBridgeMemberVM(vm3);

    vm3.invoke("StartBridgeClient", () -> startBridgeClient("group3", NetworkUtils.getServerHostName(vm0.getHost()),
        locatorPort, new String[] { "A", "B", "C" }));
    try {
      putInVM(vm3, "A", "key3", "value");
      fail("Should not have been able to find Region A on the server");
    } catch (Exception expected) {
    }
    putInVM(vm3, "C", "key4", "value");
    Assert.assertEquals("value", getInVM(vm2, "C", "key4"));

    stopBridgeMemberVM(vm3);

    vm3.invoke("StartBridgeClient", () -> startBridgeClient("group2", NetworkUtils.getServerHostName(vm0.getHost()),
        locatorPort, new String[] { "A", "B", "C" }));
    putInVM(vm3, "B", "key5", "value");
    Assert.assertEquals("value", getInVM(vm1, "B", "key5"));
    Assert.assertEquals("value", getInVM(vm2, "B", "key5"));

    stopBridgeMemberVM(vm1);
    putInVM(vm3, "B", "key6", "value");
    Assert.assertEquals("value", getInVM(vm2, "B", "key6"));
    vm1.invoke("Start BridgeServer", () -> startBridgeServer(new String[] { "group1", "group2" }, locators, new String[] { "A", "B" }));
    stopBridgeMemberVM(vm2);

    putInVM(vm3, "B", "key7", "value");
    Assert.assertEquals("value", getInVM(vm1, "B", "key7"));
  }

  @Test
  public void testTwoServersInSameVM() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    //    VM vm3 = host.getVM(3);

    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    vm0.invoke("Start Locator", () -> startLocator(NetworkUtils.getServerHostName(vm0.getHost()), locatorPort, ""));

    final String locators = NetworkUtils.getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";

    final int serverPort1 = vm1.invoke("Start BridgeServer", () -> startBridgeServer(new String[] { "group1" }, locators));
    final int serverPort2 = vm1.invoke("Start CacheServer", () -> addCacheServer(new String[] { "group2" }));

    vm2.invoke("StartBridgeClient", () -> startBridgeClient("group2", NetworkUtils.getServerHostName(vm0.getHost()), locatorPort));

    checkEndpoints(vm2, new int[] { serverPort2 });

    stopBridgeMemberVM(vm2);

    vm2.invoke("StartBridgeClient", () -> startBridgeClient("group1", NetworkUtils.getServerHostName(vm0.getHost()), locatorPort));

    checkEndpoints(vm2, new int[] { serverPort1 });
  }

  @Test
  public void testClientMembershipListener() throws Exception {
    final Host host = Host.getHost(0);
    VM locatorVM = host.getVM(0);
    VM bridge1VM = host.getVM(1);
    VM bridge2VM = host.getVM(2);
    VM clientVM = host.getVM(3);
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    locatorVM.invoke("Start Locator", () -> startLocator(NetworkUtils.getServerHostName(locatorVM.getHost()), locatorPort, ""));

    String locators = NetworkUtils.getServerHostName(locatorVM.getHost()) + "[" + locatorPort + "]";

    //start a bridge server with a listener
    addBridgeListener(bridge1VM);
    int serverPort1 = bridge1VM.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    //start a bridge client with a listener
    addBridgeListener(clientVM);
    clientVM.invoke("StartBridgeClient", () -> {
      String locatorHostName = NetworkUtils.getServerHostName(locatorVM.getHost());
      startBridgeClient(null, locatorHostName, locatorPort);
    });
    // wait for client to connect
    checkEndpoints(clientVM, new int[] { serverPort1 });

    //make sure the client and bridge server both noticed each other
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

    checkEndpoints(clientVM, new int[] { serverPort1 });

    //start another bridge server and make sure it is detected by the client
    int serverPort2 = bridge2VM.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    checkEndpoints(clientVM, new int[] { serverPort1, serverPort2 });
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

    //stop the second bridge server and make sure it is detected by the client
    stopBridgeMemberVM(bridge2VM);

    checkEndpoints(clientVM, new int[] { serverPort1 });
    serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(0, serverListener.getDepartures());
    Assert.assertEquals(0, serverListener.getJoins());
    resetBridgeListener(bridge1VM);
    waitForCrash(clientVM);
    clientListener = getBridgeListener(clientVM);
    Assert.assertEquals(0, clientListener.getJoins());
    Assert.assertEquals(1, clientListener.getDepartures()+clientListener.getCrashes());
    resetBridgeListener(clientVM);

    //stop the client and make sure the bridge server notices
    stopBridgeMemberVM(clientVM);
    waitForDeparture(bridge1VM);
    serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(1, serverListener.getDepartures());
    Assert.assertEquals(0, serverListener.getJoins());
  }

  protected Object getInVM(VM vm, final Serializable key) {
    return getInVM(vm, REGION_NAME, key);
  }

  protected Object getInVM(VM vm, final String regionName, final Serializable key) {
    return vm.invoke(new SerializableCallable("Get in VM") {
      public Object call() throws Exception {
        Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
        Region region = cache.getRegion(regionName);
        return region.get(key);
      }
    });
  }

  protected void putAndWaitForSuccess(VM vm, final String regionName, final Serializable key, final Serializable value) throws InterruptedException {
    long endTime = System.currentTimeMillis() + MAX_WAIT;
    long remaining = MAX_WAIT;
    int i = 0;
    while (true) {
      try {
        System.err.println("Attempt: " + (i++));
        putInVM(vm, regionName, key, value);
        break;
      } catch (NoAvailableLocatorsException | org.apache.geode.test.dunit.RMIException e) {
        if (!(e instanceof NoAvailableLocatorsException)
            && !(e.getCause() instanceof NoAvailableServersException)) {
          throw e;
        }
        if (remaining <= 0) {
          throw e;
        }
        Wait.pause(100);
        remaining = endTime - System.currentTimeMillis();
      }
    }
  }

  protected void putInVM(VM vm, final Serializable key, final Serializable value) {
    putInVM(vm, REGION_NAME, key, value);
  }

  protected void putInVM(VM vm, final String regionName, final Serializable key, final Serializable value) {
    vm.invoke(new SerializableCallable("Put in VM") {
      public Object call() throws Exception {
        Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
        Region region = cache.getRegion(regionName);
        return region.put(key, value);
      }
    });
  }

  /**
   * Assert that there is one endpoint with the given host in port
   * on the client vm.
   *
   * @param vm            - the vm the client is running in
   * @param expectedPorts - The server ports we expect the client to be connected to.
   */
  protected void checkEndpoints(VM vm, final int[] expectedPorts) {
    vm.invoke(new SerializableRunnable("Check endpoint") {
      public void run() {
        PoolImpl pool = (PoolImpl) PoolManager.find(POOL_NAME);
        int retryCount = 50;
        List/*<ServerLocation>*/ endpoints;
        HashSet actualEndpointPorts;
        HashSet expectedEndpointPorts = new HashSet();
        for (int i = 0; i < expectedPorts.length; i++) {
          expectedEndpointPorts.add(new Integer(expectedPorts[i]));
        }
        do {
          endpoints = pool.getCurrentServers();
          actualEndpointPorts = new HashSet();
          for (Iterator itr = endpoints.iterator(); itr.hasNext(); ) {
            ServerLocation sl = (ServerLocation) itr.next();
            actualEndpointPorts.add(new Integer(sl.getPort()));
          }
          if (expectedEndpointPorts.size() == actualEndpointPorts.size()) {
            break;
          }
          Wait.pause(100);
        } while (retryCount-- > 0);
        Assert.assertEquals(expectedEndpointPorts, actualEndpointPorts);
      }
    });
  }

  protected void addBridgeListener(VM vm) {
    vm.invoke(new SerializableRunnable("Add membership listener") {
      public void run() {
        MyListener listener = new MyListener();
        ClientMembership.registerClientMembershipListener(listener);
        remoteObjects.put(BRIDGE_LISTENER, listener);
      }
    });
  }

  protected void resetBridgeListener(VM vm) {
    vm.invoke(new SerializableRunnable("Reset membership listener") {
      public void run() {
        MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
        listener.reset();
      }
    });
  }

  private MyListener getBridgeListener(VM vm) {
    return (MyListener) vm.invoke(new SerializableCallable("Get membership listener") {
      public Object call() {
        return remoteObjects.get(BRIDGE_LISTENER);
      }
    });
  }

  private void waitForJoin(VM vm) {
    vm.invoke(new SerializableRunnable("wait for join") {
      public void run() {
        MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
        synchronized (listener) {
          long end = System.currentTimeMillis() + 10000;
          while (listener.joins == 0) {
            try {
              long remaining = end - System.currentTimeMillis();
              if (remaining <= 0) {
                break;
              }
              listener.wait(remaining);
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        }
      }
    });
  }

  private void waitForCrash(VM vm) {
    vm.invoke(new SerializableRunnable("wait for crash") {
      public void run() {
        MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
        synchronized (listener) {
          long end = System.currentTimeMillis() + 10000;
          while (listener.crashes == 0) {
            try {
              long remaining = end - System.currentTimeMillis();
              if (remaining <= 0) {
                return;
              }
              listener.wait(remaining);
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }
      }
    });
  }

  private void waitForDeparture(VM vm) {
    vm.invoke(new SerializableRunnable("wait for departure") {
      public void run() {
        MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
        synchronized (listener) {
          long end = System.currentTimeMillis() + 10000;
          while (listener.departures == 0) {
            try {
              long remaining = end - System.currentTimeMillis();
              if (remaining < 0) {
                break;
              }
              listener.wait(remaining);
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }
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
