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
package com.gemstone.gemfire.cache.client.internal;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.NoAvailableLocatorsException;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.membership.ClientMembership;
import com.gemstone.gemfire.management.membership.ClientMembershipEvent;
import com.gemstone.gemfire.management.membership.ClientMembershipListenerAdapter;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests cases that are particular for the auto connection source
 * - dynamically discovering servers, locators, handling 
 * locator disappearance, etc.
 * @author dsmith
 *
 */
public class AutoConnectionSourceDUnitTest extends LocatorTestBase {
  
  protected static final Object BRIDGE_LISTENER = "BRIDGE_LISTENER";
  private static final long MAX_WAIT = 60000;
  
  public void setUp() throws Exception {
    super.setUp();
    addExpectedException("NoAvailableLocatorsException");
  }

  public AutoConnectionSourceDUnitTest(String name) {
    super(name);
  }
  
  public void testDiscoverBridgeServers() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    
    String locators = getServerHostName(vm0.getHost())+ "[" + locatorPort + "]";
    
    startBridgeServerInVM(vm1, null, locators);

    startBridgeClientInVM(vm2, null, getServerHostName(vm0.getHost()), locatorPort);

    putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");
    
    Assert.assertEquals("value", getInVM(vm1, "key"));
  }

  public void testNoLocators() {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    try {
      startBridgeClientInVM(vm0, null, getServerHostName(vm0.getHost()), AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
      putInVM(vm0, "key", "value");
      fail("Client cache should not have been able to start");
    } catch(Exception e) {
      //expected an exception
    }
  }
  
  public void testNoBridgeServer() {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    try { 
      startBridgeClientInVM(vm1, null, getServerHostName(vm0.getHost()), locatorPort);
      putInVM(vm0, "key", "value");
      fail("Client cache should not have been able to start");
    } catch(Exception e) {
      //expected an exception
    }
  }
  
  public void testDynamicallyFindBridgeServer() throws Exception  {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    
    String locators = getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";
    
    startBridgeServerInVM(vm1, null, locators);
    
    startBridgeClientInVM(vm2, null, getServerHostName(vm0.getHost()), locatorPort);
    
    putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");
    
    startBridgeServerInVM(vm3, null, locators);
    
    stopBridgeMemberVM(vm1);
    
    putAndWaitForSuccess(vm2, REGION_NAME, "key2", "value2");
    
    Assert.assertEquals("value2", getInVM(vm3, "key2"));
  }
  
  public void testDynamicallyFindLocators() throws Exception {
    final Host host = Host.getHost(0);
    final String hostName = getServerHostName(host);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    
    final int locatorPort0 = ports[0];
    final int locatorPort1 = ports[1];
    final int locatorPort3 = ports[2];
    String locators = getLocatorString(host, new int[] { locatorPort0, locatorPort1, locatorPort3});
    startLocatorInVM(vm0, locatorPort0, locators);
    
    startLocatorInVM(vm1, locatorPort1, locators);
    startBridgeClientInVM(vm2, null, getServerHostName(vm0.getHost()), locatorPort0);
    
    InetSocketAddress locatorToWaitFor= new InetSocketAddress(hostName, locatorPort1);
    waitForLocatorDiscovery(vm2, locatorToWaitFor);
    
    stopLocatorInVM(vm0);
    startBridgeServerInVM(vm0, null, locators);
    
    putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");
    Assert.assertEquals("value", getInVM(vm0, "key"));
    
    startLocatorInVM(vm3, locatorPort3, locators);
    stopBridgeMemberVM(vm0);
    locatorToWaitFor= new InetSocketAddress(hostName, locatorPort3);
    waitForLocatorDiscovery(vm2, locatorToWaitFor);
    stopLocatorInVM(vm1);
    startBridgeServerInVM(vm1, null, locators);
    putAndWaitForSuccess(vm2, REGION_NAME, "key2", "value2");
    Assert.assertEquals("value2", getInVM(vm1, "key2"));
    
  }
  
  public void testEmbeddedLocator() throws Exception  {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
    String locators = getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";
    
    startBridgeServerWithEmbeddedLocator(vm0, null, locators, new String[] {REGION_NAME}, CacheServer.DEFAULT_LOAD_PROBE);
    
    startBridgeClientInVM(vm2, null, getServerHostName(vm0.getHost()), locatorPort);
    
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
  
  public void testServerGroups() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    
    String locators = getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";
    
    startBridgeServerInVM(vm1, new String[] {"group1", "group2"} , locators, new String[] {"A", "B"});
    startBridgeServerInVM(vm2, new String[] {"group2", "group3"}, locators, new String[] {"B", "C"});

    
    startBridgeClientInVM(vm3, "group1", getServerHostName(vm0.getHost()), locatorPort, new String [] {"A", "B", "C"});
    putAndWaitForSuccess(vm3, "A", "key", "value");
    Assert.assertEquals("value", getInVM(vm1, "A", "key"));
    try {
      putInVM(vm3, "C", "key2", "value2");
      fail("Should not have been able to find Region C on the server");
    } catch(Exception expected) {}
    
    stopBridgeMemberVM(vm3);
    
    startBridgeClientInVM(vm3, "group3", getServerHostName(vm0.getHost()), locatorPort, new String [] {"A", "B", "C"});
    try {
      putInVM(vm3, "A", "key3", "value");
      fail("Should not have been able to find Region A on the server");
    } catch(Exception expected) {}
    putInVM(vm3, "C", "key4", "value");
    Assert.assertEquals("value", getInVM(vm2, "C", "key4"));
    
    stopBridgeMemberVM(vm3);
    
    startBridgeClientInVM(vm3, "group2", getServerHostName(vm0.getHost()), locatorPort, new String [] {"A", "B", "C"});
    putInVM(vm3, "B", "key5", "value");
    Assert.assertEquals("value", getInVM(vm1, "B", "key5"));
    Assert.assertEquals("value", getInVM(vm2, "B", "key5"));
    
    stopBridgeMemberVM(vm1);
    putInVM(vm3, "B", "key6", "value");
    Assert.assertEquals("value", getInVM(vm2, "B", "key6"));
    startBridgeServerInVM(vm1, new String[] {"group1", "group2"} , locators, new String[] {"A", "B"});
    stopBridgeMemberVM(vm2);
    
    putInVM(vm3, "B", "key7", "value");
    Assert.assertEquals("value", getInVM(vm1, "B", "key7"));
  }
  
  public void testTwoServersInSameVM() {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
//    VM vm3 = host.getVM(3);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
    startLocatorInVM(vm0, locatorPort, "");
    
    final String locators = getServerHostName(vm0.getHost()) + "[" + locatorPort + "]";
    
    final int serverPort1 =startBridgeServerInVM(vm1, new String[] {"group1"}, locators);
    final int serverPort2 =addCacheServerInVM(vm1, new String[] {"group2"});
    
    startBridgeClientInVM(vm2, "group2", getServerHostName(vm0.getHost()), locatorPort);
    
    checkEndpoints(vm2, new int[] {serverPort2});
    
    stopBridgeMemberVM(vm2);

    startBridgeClientInVM(vm2, "group1", getServerHostName(vm0.getHost()), locatorPort);
    
    checkEndpoints(vm2, new int[] {serverPort1});
  }
  
  public void testClientMembershipListener() throws Exception {
    final Host host = Host.getHost(0);
    VM locatorVM = host.getVM(0);
    VM bridge1VM = host.getVM(1);
    VM bridge2VM = host.getVM(2);
    VM clientVM = host.getVM(3);
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(locatorVM, locatorPort, "");
    String locators = getServerHostName(locatorVM.getHost()) + "[" + locatorPort + "]";

    //start a bridge server with a listener
    addBridgeListener(bridge1VM);
    int serverPort1 = startBridgeServerInVM(bridge1VM, null, locators);

    //start a bridge client with a listener
    addBridgeListener(clientVM);
    startBridgeClientInVM(clientVM, null, getServerHostName(locatorVM.getHost()), locatorPort);
    // wait for client to connect
    checkEndpoints(clientVM, new int[] {serverPort1});
    
    //make sure the client and bridge server both noticed each other
    waitForJoin(bridge1VM);
    MyListener serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(0, serverListener.getDepartures());
    Assert.assertEquals(1, serverListener.getJoins());
    resetBridgeListener(bridge1VM);
    
    waitForJoin(clientVM);
    MyListener clientListener= getBridgeListener(clientVM);
    Assert.assertEquals(0, clientListener.getCrashes());
    Assert.assertEquals(0, clientListener.getDepartures());
    Assert.assertEquals(1, clientListener.getJoins());
    resetBridgeListener(clientVM);
    
    checkEndpoints(clientVM, new int[] {serverPort1});

    //start another bridge server and make sure it is detected by the client
    int serverPort2 = startBridgeServerInVM(bridge2VM, null, locators);
    
    checkEndpoints(clientVM, new int[] {serverPort1, serverPort2});
    serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(0, serverListener.getDepartures());
    Assert.assertEquals(0, serverListener.getJoins());
    resetBridgeListener(bridge1VM);
    waitForJoin(clientVM);
    clientListener= getBridgeListener(clientVM);
    Assert.assertEquals(0, clientListener.getCrashes());
    Assert.assertEquals(0, clientListener.getDepartures());
    Assert.assertEquals(1, clientListener.getJoins());
    resetBridgeListener(clientVM);
    
    //stop the second bridge server and make sure it is detected by the client
    stopBridgeMemberVM(bridge2VM);
    
    checkEndpoints(clientVM, new int[] {serverPort1});
    serverListener = getBridgeListener(bridge1VM);
    Assert.assertEquals(0, serverListener.getCrashes());
    Assert.assertEquals(0, serverListener.getDepartures());
    Assert.assertEquals(0, serverListener.getJoins());
    resetBridgeListener(bridge1VM);
    waitForCrash(clientVM);
    clientListener= getBridgeListener(clientVM);
    Assert.assertEquals(1, clientListener.getCrashes());
    Assert.assertEquals(0, clientListener.getDepartures());
    Assert.assertEquals(0, clientListener.getJoins());
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
  
  protected void putAndWaitForSuccess(VM vm,  final String regionName, final Serializable key, final Serializable value) throws InterruptedException
  {
    long endTime = System.currentTimeMillis() + MAX_WAIT;
    long remaining = MAX_WAIT;
    int i = 0;
    while(true) {
      try {
        System.err.println("Attempt: " + (i++));
        putInVM(vm, regionName, key, value);
        break;
      } catch (NoAvailableLocatorsException | com.gemstone.gemfire.test.dunit.RMIException e) {
        if( !(e instanceof NoAvailableLocatorsException)
            && !(e.getCause() instanceof NoAvailableServersException)) {
          throw e;
        }
        if(remaining <= 0) {
          throw e;
        }
        pause(100);
        remaining = endTime - System.currentTimeMillis();
      }
    }
  }

  protected void putInVM(VM vm, final Serializable key, final Serializable value) {
    putInVM(vm, REGION_NAME, key, value);
  }
  
  
  
  protected void putInVM(VM vm,  final String regionName, final Serializable key, final Serializable value) {
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
   * @param vm - the vm the client is running in
   * @param expectedPort - The server port we expect the client to be connected to.
   */
  protected void checkEndpoints(VM vm, final int[] expectedPorts) {
    vm.invoke(new SerializableRunnable("Check endpoint") {
      public void run() {
        PoolImpl pool = (PoolImpl) PoolManager.find(POOL_NAME);
        int retryCount = 50;
        List/*<ServerLocation>*/ endpoints;
        HashSet actualEndpointPorts;
        HashSet expectedEndpointPorts = new HashSet();
        for(int i = 0; i < expectedPorts.length; i++) {
          expectedEndpointPorts.add(new Integer(expectedPorts[i]));
        }
        do {
          endpoints = pool.getCurrentServers();
          actualEndpointPorts = new HashSet();
          for(Iterator itr = endpoints.iterator(); itr.hasNext();) {
            ServerLocation sl = (ServerLocation)itr.next();
            actualEndpointPorts.add(new Integer(sl.getPort()));
          }
          if (expectedEndpointPorts.size() == actualEndpointPorts.size()) {
            break;
          } else {
            pause(100);
          }
        } while(retryCount-- > 0);
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
    vm.invoke(new SerializableRunnable("Add membership listener") {
      public void run() {
        MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
        listener.reset();
      }
    });
  }
  
  private MyListener getBridgeListener(VM vm) {
    return (MyListener) vm.invoke(new SerializableCallable("Add membership listener") {
      public Object call() {
        return remoteObjects.get(BRIDGE_LISTENER);
      }
    });
  }
  
  private void waitForJoin(VM vm) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
        synchronized(listener) {
          long end = System.currentTimeMillis() + 10000;
          while (listener.joins == 0) {
            try {
              long remaining = end - System.currentTimeMillis();
              if(remaining < 0) {
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
  
  private void waitForCrash(VM vm) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
        synchronized(listener) {
          long end = System.currentTimeMillis() + 10000;
          while (listener.crashes== 0) {
            try {
              long remaining = end - System.currentTimeMillis();
              if(remaining < 0) {
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
  
  private void waitForDeparture(VM vm) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        MyListener listener = (MyListener) remoteObjects.get(BRIDGE_LISTENER);
        synchronized(listener) {
          long end = System.currentTimeMillis() + 10000;
          while (listener.departures == 0) {
            try {
              long remaining = end - System.currentTimeMillis();
              if(remaining < 0) {
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
    protected int crashes = 0;
    protected int joins = 0;
    protected int departures= 0;

    @Override
    public synchronized void memberCrashed(ClientMembershipEvent event) {
      crashes++;
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
      notifyAll();
    }

    @Override
    public synchronized void memberLeft(ClientMembershipEvent event) {
      departures++;
      notifyAll();
    }

    public synchronized int getCrashes() {
      return crashes;
    }

    public synchronized int getJoins() {
      return joins;
    }

    public synchronized int  getDepartures() {
      return departures;
    }
  }
}
