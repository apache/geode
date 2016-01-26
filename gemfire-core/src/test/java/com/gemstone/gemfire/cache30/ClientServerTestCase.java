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
package com.gemstone.gemfire.cache30;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Provides helper methods for testing clients and servers. This
 * test case was created by refactoring methods from ConnectionPoolDUnitTest into
 * this class.
 *
 * @author Kirk Lund
 * @since 4.2.1
 */
public class ClientServerTestCase extends CacheTestCase {
  
  public static String NON_EXISTENT_KEY = "NON_EXISTENT_KEY";
  
  public static boolean AUTO_LOAD_BALANCE = false;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // this makes sure we don't have any connection left over from previous tests
    disconnectAllFromDS();
  }
  
  @Override
  public void tearDown2() throws Exception {
    // this makes sure we don't leave anything for the next tests
    disconnectAllFromDS();
  }

  public ClientServerTestCase(String name) {
    super(name);
  }

  /**
   * Starts a bridge server on the given port
   *
   * @since 4.0
   */
  public int startBridgeServer(int port)
    throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setMaxThreads(getMaxThreads());
    bridge.start();
    return bridge.getPort();
  }

  /**
   * Defaults to 0 which means no selector in server.
   * Subclasses can override setting this to a value > 0 to enable selector.
   */
  protected int getMaxThreads() {
    return 0;
  }
  
  /**
   * Stops the bridge server that serves up the given cache.
   *
   * @since 4.0
   */
  public void stopBridgeServers(Cache cache) {
    CacheServer bridge = null;
    for (Iterator bsI = cache.getCacheServers().iterator();bsI.hasNext(); ) {
      bridge = (CacheServer) bsI.next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }
  }

  /**
   * Returns region attributes for a <code>LOCAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    return factory.create();
  }

  public static String createBridgeClientConnection(String host, int[] ports) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < ports.length; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("name" + i + "=");
      sb.append(host + ":" + ports[i]);
    }
    return sb.toString();
  }

  public static Pool configureConnectionPool(AttributesFactory factory,
      String host, int port1, int port2, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup, int pingInterval,
      int idleTimeout, boolean threadLocalCnxs, int lifetimeTimeout) {
    int[] ports;
    if (port2 != -1) {
      ports = new int[] { port1, port2 };
    }
    else {
      ports = new int[] { port1 };
    }
    return configureConnectionPool(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, pingInterval, idleTimeout,
        threadLocalCnxs, lifetimeTimeout);
  }

  public static Pool configureConnectionPool(AttributesFactory factory,
      String host, int port1, int port2, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup, int pingInterval,
      int idleTimeout, boolean threadLocalCnxs) {
    return configureConnectionPool(factory, host, port1, port2, establish,
        redundancy, connectionsPerServer, serverGroup, pingInterval,
        idleTimeout, threadLocalCnxs, -2/*lifetimeTimeout*/);
  }

  public static Pool configureConnectionPool(AttributesFactory factory,
      String host, int port1, int port2, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup, int pingInterval) {
    return configureConnectionPool(factory, host, port1, port2, establish,
        redundancy, connectionsPerServer, serverGroup, pingInterval, -1, false);
  }

  public static Pool configureConnectionPool(AttributesFactory factory,
      String host, int port1, int port2, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup) {
    return configureConnectionPool(factory, host, port1, port2, establish,
        redundancy, connectionsPerServer, serverGroup, -1/*pingInterval*/);
  }

  public static Pool configureConnectionPoolWithName(AttributesFactory factory,
      String host, int[] ports, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup, String poolName) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports,
        establish, redundancy, connectionsPerServer, serverGroup, poolName,
        PoolManager.createFactory(), -1, -1, false, -2, -1);
  }

  public static Pool configureConnectionPoolWithName(AttributesFactory factory,
      String host, int[] ports, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup, String poolName,
      int pingInterval, int idleTimeout, boolean threadLocalCnxs,
      int lifetimeTimeout, int statisticInterval) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports,
        establish, redundancy, connectionsPerServer, serverGroup, poolName,
        PoolManager.createFactory(), pingInterval, idleTimeout,
        threadLocalCnxs, lifetimeTimeout, statisticInterval);
  }

  public static Pool configureConnectionPoolWithNameAndFactory(
      AttributesFactory factory, String host, int[] ports, boolean establish,
      int redundancy, int connectionsPerServer, String serverGroup,
      String poolName, PoolFactory pf) {
    return configureConnectionPoolWithNameAndFactory(factory, host, ports,
        establish, redundancy, connectionsPerServer, serverGroup, poolName, pf,
        -1, -1, false, -2, -1);
  }

  public static Pool configureConnectionPoolWithNameAndFactory(
      AttributesFactory factory, String host, int[] ports, boolean establish,
      int redundancy, int connectionsPerServer, String serverGroup,
      String poolName, PoolFactory pf, int pingInterval, int idleTimeout,
      boolean threadLocalCnxs, int lifetimeTimeout, int statisticInterval) {

    if(AUTO_LOAD_BALANCE) {
      pf.addLocator(host,getDUnitLocatorPort());
    } else {
      for(int z=0;z<ports.length;z++) {
        pf.addServer(host,ports[z]);
      }
    }
    
    //TODO - probably should pass in minConnections rather than connecions per server
    if(connectionsPerServer!=-1) {
      pf.setMinConnections(connectionsPerServer * ports.length);
    }
    if (threadLocalCnxs) {
      pf.setThreadLocalConnections(true);
    }
    if (pingInterval != -1) {
      pf.setPingInterval(pingInterval);
    }
    if (idleTimeout != -1) {
      pf.setIdleTimeout(idleTimeout);
    }
    if (statisticInterval != -1) {
      pf.setStatisticInterval(statisticInterval);
    }
    if (lifetimeTimeout != -2) {
      pf.setLoadConditioningInterval(lifetimeTimeout);
    }
    if(establish) {
      pf.setSubscriptionEnabled(true);
      pf.setSubscriptionRedundancy(redundancy);
      pf.setSubscriptionAckInterval(1);
    }
    if(serverGroup!=null) {
      pf.setServerGroup(serverGroup);
    }
    String rpoolName = "testPool";
    if(poolName!=null) {
      rpoolName = poolName;
    }
    Pool pool  = pf.create(rpoolName);
    if(factory!=null) {
      factory.setPoolName(rpoolName);
    }
    return pool;
  }

  public static Pool configureConnectionPool(AttributesFactory factory,
      String host, int[] ports, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup) {
    return configureConnectionPool(factory, host, ports, establish, redundancy,
        connectionsPerServer, serverGroup, -1/*pingInterval*/,
        -1/*idleTimeout*/, false/*threadLocalCnxs*/, -2/*lifetimeTimeout*/);
  }

  public static Pool configureConnectionPool(AttributesFactory factory,
      String host, int[] ports, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup, int pingInterval,
      int idleTimeout, boolean threadLocalCnxs, int lifetimeTimeout) {
    return configureConnectionPoolWithName(factory, host, ports, establish,
        redundancy, connectionsPerServer, serverGroup, null/*poolName*/,
        pingInterval, idleTimeout, threadLocalCnxs, lifetimeTimeout, -1);
  }

  public static Pool configureConnectionPool(AttributesFactory factory,
      String host, int[] ports, boolean establish, int redundancy,
      int connectionsPerServer, String serverGroup, int pingInterval,
      int idleTimeout, boolean threadLocalCnxs, int lifetimeTimeout,
      int statisticInterval) {
    return configureConnectionPoolWithName(factory, host, ports, establish,
        redundancy, connectionsPerServer, serverGroup, null/*poolName*/,
        pingInterval, idleTimeout, threadLocalCnxs, lifetimeTimeout,
        statisticInterval);
  }

  /*protected static InternalDistributedMember findDistributedMember() {
    DM dm = ((InternalDistributedSystem)
      InternalDistributedSystem.getAnyInstance()).getDistributionManager();
    return dm.getDistributionManagerId();
  }*/

  protected static String getMemberId() {
    final InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    WaitCriterion w = new WaitCriterion() {

      public String description() {
        return "bridge never finished connecting: " + system.getMemberId();
      }

      public boolean done() {
//        getLogWriter().warning("checking member id " + system.getMemberId() +
//            " for member " + system.getDistributedMember() + " hash " +
//            System.identityHashCode(system.getDistributedMember()));
        return !system.getMemberId().contains("):0:");
      }
      
    };
    int waitMillis = 10000;
    int interval = 100;
    boolean throwException = true;
    waitForCriterion(w, waitMillis, interval, throwException);
    return system.getMemberId();
  }

  protected static DistributedMember getDistributedMember() {
    DistributedSystem system = InternalDistributedSystem.getAnyInstance();
    return system.getDistributedMember();
  }

  protected static Properties getSystemProperties() {
    DistributedSystem system = InternalDistributedSystem.getAnyInstance();
    return system.getProperties();
  }

  public static class CacheServerCacheLoader extends TestCacheLoader implements Declarable {

    public CacheServerCacheLoader() {}

    @Override
    public Object load2(LoaderHelper helper) {
      if (helper.getArgument() instanceof Integer) {
        try {
          Thread.sleep(((Integer) helper.getArgument()).intValue());
        }
        catch (InterruptedException ugh) { fail("interrupted"); }
      }
      Object ret = helper.getKey();
      
      if( ret instanceof String)
      {
        if(ret != null && ret.equals(NON_EXISTENT_KEY))
          return null;//return null
      }
      return ret;
      
    }

    public void init(Properties props)  {}
  }

  public final static String BridgeServerKey = "BridgeServerKey";
  /**
   * Create a server that has a value for every key queried and a unique
   * key/value in the specified Region that uniquely identifies each instance.
   *
   * @param vm
   *          the VM on which to create the server
   * @param rName
   *          the name of the Region to create on the server
   * @param port
   *          the TCP port on which the server should listen
   */
  public void createBridgeServer(VM vm, final String rName, final int port) {
    vm.invoke(new CacheSerializableRunnable("Create Region on Server") {
    @Override
    public void run2() {
      try {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK); // can't be local since used with registerInterest
        factory.setCacheLoader(new CacheServerCacheLoader());
        beginCacheXml();
        createRootRegion(rName, factory.create());
        startBridgeServer(port);
        finishCacheXml(rName + "-" + port);

        Region region = getRootRegion(rName);
        assertNotNull(region);
        region.put(BridgeServerKey, new Integer(port)); // A unique key/value to identify the BridgeServer
      }
      catch(Exception e) {
        getSystem().getLogWriter().severe(e);
        fail("Failed to start CacheServer " + e);
      }
    }
  });
  }

  public static int[] createUniquePorts(int numToCreate) {
    return AvailablePortHelper.getRandomAvailableTCPPorts(numToCreate);
  }

}

