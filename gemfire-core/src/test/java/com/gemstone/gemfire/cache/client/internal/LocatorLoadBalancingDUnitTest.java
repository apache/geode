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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionResponse;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionResponse;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.cache.server.ServerLoadProbeAdapter;
import com.gemstone.gemfire.cache.server.ServerMetrics;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.ServerLocator;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author dsmith
 *
 */
public class LocatorLoadBalancingDUnitTest extends LocatorTestBase {
  
  /**
   * The number of connections that we can be off by in the balancing tests
   * We need this little fudge factor, because the locator can receive an update
   * from the bridge server after it has made incremented its counter for a client
   * connection, but the client hasn't connected yet. This wipes out the estimation
   * on the locator.  This means that we may be slighly off in our balance.
   * 
   * TODO grid fix this hole in the locator.
   */
  private static final int ALLOWABLE_ERROR_IN_COUNT = 1;
  protected static final long MAX_WAIT = 60000;

  public LocatorLoadBalancingDUnitTest(String name) {
    super(name);
  }

  /**
   * Test the locator discovers a bridge server and is initialized with 
   * the correct load for that bridge server.
   */
  public void testDiscovery() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
//    vm0.invoke(new SerializableRunnable() {
//      public void run() {
//        System.setProperty("gemfire.DistributionAdvisor.VERBOSE", "true");
//      }
//    });
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    
    String locators = getLocatorString(host, locatorPort);
    
    int serverPort = startBridgeServerInVM(vm1, new String[] {"a", "b"},  locators);
    
    ServerLoad expectedLoad = new ServerLoad(0f, 1 / 800.0f, 0f, 1f);
    ServerLocation expectedLocation = new ServerLocation(getServerHostName(vm0
        .getHost()), serverPort);
    Map expected = new HashMap();
    expected.put(expectedLocation, expectedLoad);
    
    checkLocatorLoad(vm0, expected);
    
    int serverPort2 = startBridgeServerInVM(vm2, new String[] {"a", "b"},  locators);
    
    ServerLocation expectedLocation2 = new ServerLocation(getServerHostName(vm0
        .getHost()), serverPort2);
    
    expected.put(expectedLocation2, expectedLoad);
    checkLocatorLoad(vm0, expected);
  }
  
  /**
   * Test that the locator will properly estimate the load for servers when
   * it receives connection requests. 
   */
  public void testEstimation() throws UnknownHostException, IOException, ClassNotFoundException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    String locators = getLocatorString(host, locatorPort);
    
    int serverPort = startBridgeServerInVM(vm1, new String[] {"a", "b"},  locators);
    
    ServerLoad expectedLoad = new ServerLoad(2/800f, 1 / 800.0f, 0f, 1f);
    ServerLocation expectedLocation = new ServerLocation(getServerHostName(host), serverPort);
    Map expected = new HashMap();
    expected.put(expectedLocation, expectedLoad);
    
    ClientConnectionResponse response;
    response = (ClientConnectionResponse) TcpClient.requestToServer(InetAddress
        .getByName(getServerHostName(host)), locatorPort,
        new ClientConnectionRequest(Collections.EMPTY_SET, null), 10000);
    Assert.assertEquals(expectedLocation, response.getServer());
    
    response = (ClientConnectionResponse) TcpClient.requestToServer(InetAddress
        .getByName(getServerHostName(host)), locatorPort,
        new ClientConnectionRequest(Collections.EMPTY_SET, null), 10000, true);
    Assert.assertEquals(expectedLocation, response.getServer());
    
    //we expect that the connection load load will be 2 * the loadPerConnection
    checkLocatorLoad(vm0, expected);
    
    QueueConnectionResponse response2;
    response2 = (QueueConnectionResponse) TcpClient.requestToServer(InetAddress
        .getByName(getServerHostName(host)), locatorPort,
        new QueueConnectionRequest(null, 2,
            Collections.EMPTY_SET, null, false), 10000, true);
    Assert.assertEquals(Collections.singletonList(expectedLocation), response2.getServers());
    
    response2 = (QueueConnectionResponse) TcpClient
        .requestToServer(InetAddress.getByName(getServerHostName(host)),
            locatorPort, new QueueConnectionRequest(null, 5, Collections.EMPTY_SET, null,
                false), 10000, true);
    
    Assert.assertEquals(Collections.singletonList(expectedLocation), response2.getServers());

    //we expect that the queue load will increase by 2
    expectedLoad.setSubscriptionConnectionLoad(2f);
    checkLocatorLoad(vm0, expected);
  }
  
  /**
   * Test to make sure the bridge servers communicate
   * their updated load to the controller when the load
   * on the bridge server changes.
   */
  public void testLoadMessaging() {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    String locators = getLocatorString(host, locatorPort);
    
    final int serverPort = startBridgeServerInVM(vm1, new String[] {"a", "b"},  locators);
    
    //We expect 0 load
    Map expected = new HashMap();
    ServerLocation expectedLocation = new ServerLocation(getServerHostName(host), serverPort);
    ServerLoad expectedLoad = new ServerLoad(0f, 1 / 800.0f, 0f, 1f);
    expected.put(expectedLocation, expectedLoad);
    checkLocatorLoad(vm0, expected);
    
    PoolFactoryImpl pf = new PoolFactoryImpl(null);
    pf.addServer(getServerHostName(host), serverPort);
    pf.setMinConnections(8);
    pf.setMaxConnections(8);
    pf.setSubscriptionEnabled(true);
    startBridgeClientInVM(vm2, pf.getPoolAttributes(), new String[] {REGION_NAME});
    
    //We expect 8 client to server connections. The queue requires
    //an additional client to server connection, but that shouldn't show up here.
    expectedLoad = new ServerLoad(8/800f, 1 / 800.0f, 1f, 1f);
    expected.put(expectedLocation, expectedLoad);
    
    
    checkLocatorLoad(vm0, expected);
    
    stopBridgeMemberVM(vm2);
    
    //Now we expect 0 load
    expectedLoad = new ServerLoad(0f, 1 / 800.0f, 0f, 1f);
    expected.put(expectedLocation, expectedLoad);
    checkLocatorLoad(vm0, expected);
  }
  
  /**
   * Test to make sure that the locator
   * balancing load between two servers.
   */
  public void testBalancing() {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    String locators = getLocatorString(host, locatorPort);
    
    startBridgeServerInVM(vm1, new String[] {"a", "b"},  locators);
    startBridgeServerInVM(vm2, new String[] {"a", "b"},  locators);
    
    PoolFactoryImpl pf = new PoolFactoryImpl(null);
    pf.addLocator(getServerHostName(host), locatorPort);
    pf.setMinConnections(80);
    pf.setMaxConnections(80);
    pf.setSubscriptionEnabled(false);
    pf.setIdleTimeout(-1);
    startBridgeClientInVM(vm3, pf.getPoolAttributes(), new String[] {REGION_NAME});
    
    waitForPrefilledConnections(vm3, 80);
    
    checkConnectionCount(vm1, 40);
    checkConnectionCount(vm2, 40);
  }

  private void checkConnectionCount(VM vm, final int count) {
    SerializableRunnableIF checkConnectionCount = new SerializableRunnable("checkConnectionCount") {
      public void run() {
        Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
        final CacheServerImpl server = (CacheServerImpl)
            cache.getCacheServers().get(0);
        WaitCriterion wc = new WaitCriterion() {
          String excuse;
          public boolean done() {
            int sz = server.getAcceptor().getStats()
                .getCurrentClientConnections();
            if (Math.abs(sz - count) <= ALLOWABLE_ERROR_IN_COUNT) {
              return true;
            }
            excuse = "Found " + sz + " connections, expected " + count;
            return false;
          }
          public String description() {
            return excuse;
          }
        };
        DistributedTestCase.waitForCriterion(wc, 5 * 60 * 1000, 1000, true);
      }
    };
    
    vm.invoke(checkConnectionCount);
  }
  
  private void waitForPrefilledConnections(VM vm, final int count) {
    waitForPrefilledConnections(vm, count, POOL_NAME);
  }

  private void waitForPrefilledConnections(VM vm, final int count, final String poolName) {
    SerializableRunnable runnable = new SerializableRunnable("waitForPrefilledConnections") {
      public void run() {
        final PoolImpl pool = (PoolImpl) PoolManager.getAll().get(poolName);
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return pool.getConnectionCount() >= count; 
          }
          public String description() {
            return "connection count never reached " + count;
          }
        };
        DistributedTestCase.waitForCriterion(ev, MAX_WAIT, 200, true);
      }
    };
    if(vm == null) {
      runnable.run();
    } else {
      vm.invoke(runnable);
    }
  }
  
  /** Test that the locator balances load between
   * three servers with intersecting server groups.
   * Server:    1       2       3
   * Groups:    a       a,b     b
   */
  public void testIntersectingServerGroups() {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    String locators = getLocatorString(host, locatorPort);
    
    int serverPort1 = startBridgeServerInVM(vm1, new String[] {"a"},  locators);
    startBridgeServerInVM(vm2, new String[] {"a", "b"},  locators);
    startBridgeServerInVM(vm3, new String[] {"b"},  locators);
    
    PoolFactoryImpl pf = new PoolFactoryImpl(null);
    pf.addLocator(getServerHostName(host), locatorPort);
    pf.setMinConnections(12);
    pf.setSubscriptionEnabled(false);
    pf.setServerGroup("a");
    pf.setIdleTimeout(-1);
    startBridgeClientInVM(null, pf.getPoolAttributes(), new String[] {REGION_NAME});
    waitForPrefilledConnections(null, 12);
    
    checkConnectionCount(vm1, 6);
    checkConnectionCount(vm2, 6);
    checkConnectionCount(vm3, 0);
    
    getLogWriter().info("pool1 prefilled");
    
    PoolFactoryImpl pf2 = (PoolFactoryImpl) PoolManager.createFactory();
    pf2.init(pf.getPoolAttributes());
    pf2.setServerGroup("b");
    PoolImpl pool2= (PoolImpl) pf2.create("testPool2");
    waitForPrefilledConnections(null, 12, "testPool2");

    // The load will not be perfect, because we created all of the connections
    //for group A first.
    checkConnectionCount(vm1, 6);
    checkConnectionCount(vm2, 9);
    checkConnectionCount(vm3, 9);
    
    getLogWriter().info("pool2 prefilled");
    
    ServerLocation location1 = new ServerLocation(getServerHostName(host), serverPort1);
    PoolImpl pool1 = (PoolImpl) PoolManager.getAll().get(POOL_NAME);
    Assert.assertEquals("a", pool1.getServerGroup());
    
    //Use up all of the pooled connections on pool1, and acquire 3 more
    for(int i = 0; i < 15; i++) {
      pool1.acquireConnection();
    }
    
    getLogWriter().info("aquired 15 connections in pool1");
    
    //now the load should be equal
    checkConnectionCount(vm1, 9);
    checkConnectionCount(vm2, 9);
    checkConnectionCount(vm3, 9);
    
    //use up all of the pooled connections on pool2
    for(int i = 0; i < 12; i++) {
      pool2.acquireConnection();
    }
    
    getLogWriter().info("aquired 12 connections in pool2");
    
    //interleave creating connections in both pools
    for(int i = 0; i < 6; i++) {
      pool1.acquireConnection();
      pool2.acquireConnection();
    }
    
    getLogWriter().info("interleaved 6 connections from pool1 with 6 connections from pool2");
    
    //The load should still be balanced
    checkConnectionCount(vm1, 13);
    checkConnectionCount(vm2, 13);
    checkConnectionCount(vm3, 13);
    
  }
  
  public void testCustomLoadProbe() {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
//    VM vm3 = host.getVM(3);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    String locators = getLocatorString(host, locatorPort);
    
    ServerLoad load1= new ServerLoad(.3f, .01f, .44f, 4564f);
    ServerLoad load2= new ServerLoad(23.2f, 1.1f, 22.3f, .3f);
    int serverPort1 = startBridgeServerInVM(vm1, null, locators, new String[] {REGION_NAME}, new MyLoadProbe(load1 ));
    int serverPort2 = startBridgeServerInVM(vm2, null, locators, new String[] {REGION_NAME}, new MyLoadProbe(load2 ));
    
    HashMap expected = new HashMap();
    ServerLocation l1 = new ServerLocation(getServerHostName(host), serverPort1);
    ServerLocation l2 = new ServerLocation(getServerHostName(host), serverPort2);
    expected.put(l1, load1);
    expected.put(l2, load2);
    checkLocatorLoad(vm0, expected);
    
    load1.setConnectionLoad(25f);
    changeLoad(vm1, load1);
    load2.setSubscriptionConnectionLoad(3.5f);
    changeLoad(vm2, load2);
    checkLocatorLoad(vm0, expected);
    
    load1 = new ServerLoad(1f, .1f, 0f, 1f);
    load2 = new ServerLoad(2f, 5f, 0f, 2f);
    expected.put(l1, load1);
    expected.put(l2, load2);
    changeLoad(vm1, load1);
    changeLoad(vm2, load2);
    checkLocatorLoad(vm0, expected);
    
    PoolFactoryImpl pf = new PoolFactoryImpl(null);
    pf.addLocator(getServerHostName(host), locatorPort);
    pf.setMinConnections(20);
    pf.setSubscriptionEnabled(true);
    pf.setIdleTimeout(-1);
    startBridgeClientInVM(null, pf.getPoolAttributes(), new String[] {REGION_NAME});
    waitForPrefilledConnections(null, 20);
    
    //The first 10 connection should to go vm1, then 1 to vm2, then another 9 to vm1
    //because have unequal values for loadPerConnection
    checkConnectionCount(vm1, 19);
    checkConnectionCount(vm2, 1);
  }
  
  public void checkLocatorLoad(VM vm, final Map expected) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        List locators = Locator.getLocators();
        Assert.assertEquals(1, locators.size());
        InternalLocator locator = (InternalLocator) locators.get(0);
        final ServerLocator sl = locator.getServerLocatorAdvisee();
        InternalLogWriter log = new LocalLogWriter(InternalLogWriter.FINEST_LEVEL, System.out);
        sl.getDistributionAdvisor().dumpProfiles("PROFILES= ");
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return expected.equals(sl.getLoadMap());
          }
          public String description() {
            return "load map never became equal to " + expected;
          }
        };
        DistributedTestCase.waitForCriterion(ev, MAX_WAIT, 200, true);
      }
    });
  }
  
  private void changeLoad(VM vm, final ServerLoad newLoad) {
    vm.invoke(new SerializableRunnable() {

      public void run() {
        Cache cache = (Cache) remoteObjects.get(CACHE_KEY);
        CacheServer server = (CacheServer) cache.getCacheServers().get(0);
        MyLoadProbe probe = (MyLoadProbe) server.getLoadProbe();
        probe.setLoad(newLoad);
      }
      
    });
  }
  
  private static class MyLoadProbe extends ServerLoadProbeAdapter implements Serializable {
    private ServerLoad load;
    
    public MyLoadProbe(ServerLoad load) {
      this.load = load;
    }
    
    public ServerLoad getLoad(ServerMetrics metrics) {
      float connectionLoad = load.getConnectionLoad()
          + metrics.getConnectionCount() * load.getLoadPerConnection();
      float queueLoad = load.getSubscriptionConnectionLoad() + metrics.getSubscriptionConnectionCount()
          * load.getLoadPerSubscriptionConnection();
      return new ServerLoad(connectionLoad, load.getLoadPerConnection(),
          queueLoad, load.getLoadPerSubscriptionConnection());
    }
    
    public void setLoad(ServerLoad load) {
      this.load = load;
    }
  }
}
