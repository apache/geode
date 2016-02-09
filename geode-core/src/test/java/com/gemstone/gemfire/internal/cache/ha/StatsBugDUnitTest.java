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

import java.util.Iterator;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * This is Dunit test for bug 36109. This test has a cache-client having a primary
 * and a secondary cache-server as its endpoint. Primary does some operations
 * and is stopped, the client fails over to secondary and does some operations
 * and it is verified that the 'invalidates' stats at the client is same as the
 * total number of operations done by both primary and secondary. The bug was
 * appearing because invalidate stats was part of Endpoint which used to get
 * closed during fail over , with the failed endpoint getting closed. This bug
 * has been fixed by moving the invalidate stat to be part of our implementation.
 * 
 * @author Dinesh Patel
 * 
 */
@Category(DistributedTest.class)
@Ignore("Test was disabled by renaming to DisabledTest")
public class StatsBugDUnitTest extends DistributedTestCase
{
  /** primary cache server */
  VM primary = null;

  /** secondary cache server */
  VM secondary = null;

  /** the cache client */
  VM client1 = null;

  /** the cache */
  private static Cache cache = null;

  /** port for the primary cache server */
  private static int PORT1;

  /** port for the secondary cache server */
  private static int PORT2;

  /** name of the test region */
  private static final String REGION_NAME = "StatsBugDUnitTest_Region";

  /** brige-writer instance( used to get connection proxy handle) */
  private static PoolImpl pool = null;

  /** total number of cache servers */
  private static final int TOTAL_SERVERS = 2;

  /** number of puts done by each server */
  private static final int PUTS_PER_SERVER = 10;

  /** prefix added to the keys of events generated on primary */
  private static final String primaryPrefix = "primary_";

  /** prefix added to the keys of events generated on secondary */
  private static final String secondaryPrefix = "secondary_";

  /**
   * Constructor
   * 
   * @param name -
   *          name for this test instance
   */
  public StatsBugDUnitTest(String name) {
    super(name);
  }

  /**
   * Creates the primary and the secondary cache servers
   * 
   * @throws Exception -
   *           thrown if any problem occurs in initializing the test
   */
  public void setUp() throws Exception
  {
    disconnectAllFromDS();
    super.setUp();
    final Host host = Host.getHost(0);
    primary = host.getVM(0);
    secondary = host.getVM(1);
    client1 = host.getVM(2);
    PORT1 = ((Integer)primary.invoke(StatsBugDUnitTest.class,
        "createServerCache")).intValue();
    PORT2 = ((Integer)secondary.invoke(StatsBugDUnitTest.class,
        "createServerCache")).intValue();
  }

  /**
   * Create the cache
   * 
   * @param props -
   *          properties for DS
   * @return the cache instance
   * @throws Exception -
   *           thrown if any problem occurs in cache creation
   */
  private Cache createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    Cache cache = null;
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  /**
   * close the cache instances in server and client during tearDown
   * 
   * @throws Exception
   *           thrown if any problem occurs in closing cache
   */
  @Override
  protected final void preTearDown() throws Exception {
    // close client
    client1.invoke(StatsBugDUnitTest.class, "closeCache");

    // close server
    primary.invoke(StatsBugDUnitTest.class, "closeCache");
    secondary.invoke(StatsBugDUnitTest.class, "closeCache");
  }

  /**
   * This test does the following:<br>
   * 1)Create and populate the client<br>
   * 2)Do some operations from the primary cache-server<br>
   * 3)Stop the primary cache-server<br>
   * 4)Wait some time to allow client to failover to secondary and do some
   * operations from secondary<br>
   * 5)Verify that the invalidates stats at the client accounts for the
   * operations done by both, primary and secondary.
   * 
   * @throws Exception -
   *           thrown if any problem occurs in test execution
   */
  public void testBug36109() throws Exception
  {
    LogWriterUtils.getLogWriter().info("testBug36109 : BEGIN");
    client1.invoke(StatsBugDUnitTest.class, "createClientCacheForInvalidates", new Object[] {
        NetworkUtils.getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2) });
    client1.invoke(StatsBugDUnitTest.class, "prepopulateClient");
    primary.invoke(StatsBugDUnitTest.class, "doEntryOperations",
        new Object[] { primaryPrefix });
    Wait.pause(3000);
    primary.invoke(StatsBugDUnitTest.class, "stopServer");
    try {
      Thread.sleep(5000);
    }
    catch (InterruptedException ignore) {
      fail("interrupted");
    }

    secondary.invoke(StatsBugDUnitTest.class, "doEntryOperations",
        new Object[] { secondaryPrefix });
    try {
      Thread.sleep(5000);
    }
    catch (InterruptedException ignore) {
      fail("interrupted");
    }

    client1.invoke(StatsBugDUnitTest.class, "verifyNumInvalidates");
    LogWriterUtils.getLogWriter().info("testBug36109 : END");
  }

  /**
   * Creates and starts the cache-server
   * 
   * @return - the port on which cache-server is running
   * @throws Exception -
   *           thrown if any problem occurs in cache/server creation
   */
  public static Integer createServerCache() throws Exception
  {
    StatsBugDUnitTest test = new StatsBugDUnitTest("temp");
    Properties props = new Properties();
    cache = test.createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    RegionAttributes attrs = factory.create();

    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(false);
    server.setSocketBufferSize(32768);
    server.start();
    LogWriterUtils.getLogWriter().info("Server started at PORT = " + port);
    return new Integer(port);
  }

  /**
   * Initializes the cache client
   * 
   * @param port1 -
   *          port for the primary cache-server
   * @param port2
   *          for the secondary cache-server
   * @throws Exception-thrown
   *           if any problem occurs in initializing the client
   */
  public static void createClientCache(String host, Integer port1, Integer port2)
      throws Exception
  {
    StatsBugDUnitTest test = new StatsBugDUnitTest("temp");
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    pool = (PoolImpl)ClientServerTestCase.configureConnectionPool(factory, host, new int[] {port1.intValue(),port2.intValue()}, true, -1, 3, null);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    region.registerInterest("ALL_KEYS");
    LogWriterUtils.getLogWriter().info("Client cache created");
  }

  /**
   * Initializes the cache client
   * 
   * @param port1 -
   *          port for the primary cache-server
   * @param port2
   *          for the secondary cache-server
   * @throws Exception-thrown
   *           if any problem occurs in initializing the client
   */
  public static void createClientCacheForInvalidates(String host, Integer port1, Integer port2)
      throws Exception
  {
    StatsBugDUnitTest test = new StatsBugDUnitTest("temp");
    cache = test.createCache(createProperties1());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    pool = (PoolImpl)ClientServerTestCase.configureConnectionPool(factory, host, new int[] {port1.intValue(),port2.intValue()}, true, -1, 3, null);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    region.registerInterest("ALL_KEYS", false, false);
    LogWriterUtils.getLogWriter().info("Client cache created");
  }
  
  /**
   * Verify that the invalidates stats at the client accounts for the operations
   * done by both, primary and secondary.
   * 
   */
  public static void verifyNumInvalidates()
  {
    long invalidatesRecordedByStats = pool.getInvalidateCount();
    LogWriterUtils.getLogWriter().info(
        "invalidatesRecordedByStats = " + invalidatesRecordedByStats);

    int expectedInvalidates = TOTAL_SERVERS * PUTS_PER_SERVER;
    LogWriterUtils.getLogWriter().info("expectedInvalidates = " + expectedInvalidates);

    if (invalidatesRecordedByStats != expectedInvalidates) {
      fail("Invalidates received by client(" + invalidatesRecordedByStats
          + ") does not match with the number of operations("
          + expectedInvalidates + ") done at server");
    }
  }

  /**
   * Stops the cache server
   * 
   */
  public static void stopServer()
  {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        server.stop();
      }
    }
    catch (Exception e) {
      fail("failed while stopServer()" + e);
    }
  }

  /**
   * create properties for a loner VM
   */
  private static Properties createProperties1()
  {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    return props;
  }


  /**
   * Do PUT operations
   * 
   * @param keyPrefix -
   *          string prefix for the keys for all the entries do be done
   * @throws Exception -
   *           thrown if any exception occurs in doing PUTs
   */
  public static void doEntryOperations(String keyPrefix) throws Exception
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    for (int i = 0; i < PUTS_PER_SERVER; i++) {
      r1.put(keyPrefix + i, keyPrefix + "val-" + i);
    }
  }

  /**
   * Prepopulate the client with the entries that will be done by cache-servers
   * 
   * @throws Exception
   */
  public static void prepopulateClient() throws Exception
  {
    doEntryOperations(primaryPrefix);
    doEntryOperations(secondaryPrefix);
  }

  /**
   * Close the cache
   * 
   */
  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
