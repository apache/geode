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
package com.gemstone.gemfire.cache.query.dunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import cacheRunner.Portfolio;
import cacheRunner.Position;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;

/**
 * Tests remote (client/server) query execution.
 *
 * @author Barry Oglesby
 * @author Asif
 * @since 5.0.1
 */
public class QueryUsingPoolDUnitTest extends CacheTestCase {

  /** The port on which the bridge server was started in this VM */
  private static int bridgeServerPort;
  
  final String rootRegionName = "root";
  
  private final String regionName = this.getName();
  
  private final String regName = "/" + rootRegionName + "/" + regionName;
  
  // Used with compiled queries.
  private final String[] queryString = new String[] {
      "SELECT itr.value FROM " + regName + ".entries itr where itr.key = $1", // 0
      "SELECT DISTINCT * FROM " + regName + " WHERE id < $1 ORDER BY   id", // 1
      "SELECT DISTINCT * FROM " + regName + " WHERE id < $1 ORDER BY id", // 2
      "(SELECT DISTINCT * FROM " + regName + " WHERE id < $1).size", // 3
      "SELECT * FROM " + regName + " WHERE id = $1 and Ticker = $2", // 4
      "SELECT * FROM " + regName + " WHERE id < $1 and Ticker = $2", // 5
  };

  /**
   * Creates a new <code>GemFireMemberStatusDUnitTest</code>
   */
  public QueryUsingPoolDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  @Override
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Socket input is shutdown");
  }

  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  public void createPool(VM vm, String poolName, String server, int port, boolean subscriptionEnabled) {
    createPool(vm, poolName, new String[]{server}, new int[]{port}, subscriptionEnabled);  
  }

  public void createPool(VM vm, String poolName, String server, int port) {
    createPool(vm, poolName, new String[]{server}, new int[]{port}, false);  
  }

  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled) {
    vm.invoke(new CacheSerializableRunnable("createPool :" + poolName) {
      public void run2() throws CacheException {
        // Create Cache.
        getCache();

        PoolFactory cpf = PoolManager.createFactory();
        cpf.setSubscriptionEnabled(subscriptionEnabled);
        for (int i=0; i < servers.length; i++){
          LogWriterUtils.getLogWriter().info("### Adding to Pool. ### Server : " + servers[i] + " Port : " + ports[i]);
          cpf.addServer(servers[i], ports[i]);
        }

        cpf.create(poolName);
      }
    });   
  }

  public void validateCompiledQuery(VM vm, final long compiledQueryCount) {
  vm.invoke(new CacheSerializableRunnable("validate compiled query.") {
    public void run2() throws CacheException {
      long count = 0;
      for (int i=0; i < 100; i++) {
        count = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        if (count == compiledQueryCount){
          break;
        } else {
          Wait.pause(1 * 100);
        }
      }
      assertEquals(compiledQueryCount, count);
    }
  });
 }
  
  /**
   * Tests remote import query execution.
   */
  public void testRemoteImportQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        createRegion(name, rootRegionName, factory.create());
        Wait.pause(1000);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });


    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());
    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName = "testRemoteImportQueries"; 
    createPool(vm1, poolName, host0, port);

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        String queryString = null;
        SelectResults results = null;

        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from " + regionName;

        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }          

        assertEquals(numberOfEntries, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from " + regionName + " where ticker = 'ibm'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from " + regionName + " where ticker = 'IBM'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(0, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from " + regionName + " where price > 49";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries/2, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from " + regionName + " where price = 50";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from " + regionName + " where ticker = 'ibm' and price = 50";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates());
      }
    });


    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote struct query execution.
   */
  public void testRemoteStructQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        createRegion(name, factory.create());
        Wait.pause(1000);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });

    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName = "testRemoteStructQueries"; 
    createPool(vm1, poolName, host0, port);

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        String queryString = null;
        SelectResults results = null;

        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from " + regionName;
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from " + regionName + " where ticker = 'ibm'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from " + regionName + " where ticker = 'IBM'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(0, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from " + regionName + " where price > 49";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries/2, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from " + regionName + " where price = 50";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

        queryString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from " + regionName + " where ticker = 'ibm' and price = 50";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());
      }
    });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote complex query execution.
   */
  public void __testRemoteComplexQueries() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        createRegion(name, factory.create());
        Wait.pause(1000);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        Portfolio portfolio = null;
        Position position1 = null;
        Position position2 = null;
        Properties portfolioProperties= null;
        Properties position1Properties = null;
        Properties position2Properties = null;

        // Create portfolio 1
        portfolio = new Portfolio();
        portfolioProperties = new Properties();
        portfolioProperties.put("id", new Integer(1));
        portfolioProperties.put("type", "type1");
        portfolioProperties.put("status", "active");

        position1 = new Position();
        position1Properties = new Properties();
        position1Properties.put("secId", "SUN");
        position1Properties.put("qty", new Double(34000.0));
        position1Properties.put("mktValue", new Double(24.42));
        position1.init(position1Properties);
        portfolioProperties.put("position1", position1);

        position2 = new Position();
        position2Properties = new Properties();
        position2Properties.put("secId", "IBM");
        position2Properties.put("qty", new Double(8765.0));
        position2Properties.put("mktValue", new Double(34.29));
        position2.init(position2Properties);
        portfolioProperties.put("position2", position2);

        portfolio.init(portfolioProperties);
        region.put(new Integer(1), portfolio);

        // Create portfolio 2
        portfolio = new Portfolio();
        portfolioProperties = new Properties();
        portfolioProperties.put("id", new Integer(2));
        portfolioProperties.put("type", "type2");
        portfolioProperties.put("status", "inactive");

        position1 = new Position();
        position1Properties = new Properties();
        position1Properties.put("secId", "YHOO");
        position1Properties.put("qty", new Double(9834.0));
        position1Properties.put("mktValue", new Double(12.925));
        position1.init(position1Properties);
        portfolioProperties.put("position1", position1);

        position2 = new Position();
        position2Properties = new Properties();
        position2Properties.put("secId", "GOOG");
        position2Properties.put("qty", new Double(12176.0));
        position2Properties.put("mktValue", new Double(21.972));
        position2.init(position2Properties);
        portfolioProperties.put("position2", position2);

        portfolio.init(portfolioProperties);
        region.put(new Integer(2), portfolio);

        // Create portfolio 3
        portfolio = new Portfolio();
        portfolioProperties = new Properties();
        portfolioProperties.put("id", new Integer(3));
        portfolioProperties.put("type", "type3");
        portfolioProperties.put("status", "active");

        position1 = new Position();
        position1Properties = new Properties();
        position1Properties.put("secId", "MSFT");
        position1Properties.put("qty", new Double(98327.0));
        position1Properties.put("mktValue", new Double(23.32));
        position1.init(position1Properties);
        portfolioProperties.put("position1", position1);

        position2 = new Position();
        position2Properties = new Properties();
        position2Properties.put("secId", "AOL");
        position2Properties.put("qty", new Double(978.0));
        position2Properties.put("mktValue", new Double(40.373));
        position2.init(position2Properties);
        portfolioProperties.put("position2", position2);

        portfolio.init(portfolioProperties);
        region.put(new Integer(3), portfolio);

        // Create portfolio 4
        portfolio = new Portfolio();
        portfolioProperties = new Properties();
        portfolioProperties.put("id", new Integer(4));
        portfolioProperties.put("type", "type1");
        portfolioProperties.put("status", "inactive");

        position1 = new Position();
        position1Properties = new Properties();
        position1Properties.put("secId", "APPL");
        position1Properties.put("qty", new Double(90.0));
        position1Properties.put("mktValue", new Double(67.356572));
        position1.init(position1Properties);
        portfolioProperties.put("position1", position1);

        position2 = new Position();
        position2Properties = new Properties();
        position2Properties.put("secId", "ORCL");
        position2Properties.put("qty", new Double(376.0));
        position2Properties.put("mktValue", new Double(101.34));
        position2.init(position2Properties);
        portfolioProperties.put("position2", position2);

        portfolio.init(portfolioProperties);
        region.put(new Integer(4), portfolio);
      }
    });

    // Create client region
    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());
    vm1.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("mcast-port", "0");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);
        getCache();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port,-1, true, -1, -1, null);
        createRegion(name, factory.create());
      }
    });

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        String queryString = null;
        SelectResults results = null;

        queryString =
          "IMPORT cacheRunner.Position; " +
          "SELECT DISTINCT id, status FROM " + region.getFullPath() +
          "WHERE NOT (SELECT DISTINCT * FROM positions.values posnVal TYPE Position " +
          "WHERE posnVal.secId='AOL' OR posnVal.secId='SAP').isEmpty";
        try {
          results = region.query(queryString);
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        LogWriterUtils.getLogWriter().fine("size: " + results.size());
        //assertEquals(numberOfEntries, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());
      }
    });


    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote full region query execution.
   */
  public void testRemoteFullRegionQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        createRegion(name, factory.create());
        Wait.pause(1000);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });

    // Create client region
    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName = "testRemoteFullRegionQueries"; 
    createPool(vm1, poolName, host0, port);


    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        String queryString = null;
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          

        // value query
        queryString = "SELECT DISTINCT itr.value FROM " + regionName + ".entries itr where itr.key = 'key-1'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates());
        assertTrue(results.asList().get(0) instanceof TestObject);

        // key query
        queryString = "SELECT DISTINCT itr.key FROM " + regionName + ".entries itr where itr.key = 'key-1'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates());
        assertEquals("key-1", results.asList().get(0));

        // order by value query
        queryString = "SELECT DISTINCT * FROM " + regionName + " WHERE id < 101 ORDER BY id";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries, results.size());
        // All order-by query results are stored in a ResultsCollectionWrapper
        // wrapping a list, so the assertion below is not correct even though
        // it should be.
        //assertTrue(!results.getCollectionType().allowsDuplicates());
        assertTrue(results.getCollectionType().isOrdered());
        comparator = new IdComparator();
        resultsArray = results.toArray();
        for (int i=0; i<resultsArray.length; i++) {
          if (i+1 != resultsArray.length) {
            // The id of the current element in the result set must be less
            // than the id of the next one to pass.
            assertTrue("The id for " + resultsArray[i] + " should be less than the id for " + resultsArray[i+1], comparator.compare(resultsArray[i], resultsArray[i+1]) == -1);
          }
        }

        // order by struct query
        queryString = "SELECT DISTINCT id, ticker, price FROM " + regionName + " WHERE id < 101 ORDER BY id";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries, results.size());
        // All order-by query results are stored in a ResultsCollectionWrapper
        // wrapping a list, so the assertion below is not correct even though
        // it should be.
        //assertTrue(!results.getCollectionType().allowsDuplicates());
        assertTrue(results.getCollectionType().isOrdered());
        comparator = new StructIdComparator();
        resultsArray = results.toArray();
        for (int i=0; i<resultsArray.length; i++) {
          if (i+1 != resultsArray.length) {
            // The id of the current element in the result set must be less
            // than the id of the next one to pass.
            assertTrue("The id for " + resultsArray[i] + " should be less than the id for " + resultsArray[i+1], comparator.compare(resultsArray[i], resultsArray[i+1]) == -1);
          }
        }

        // size query
        queryString = "(SELECT DISTINCT * FROM " + regionName + " WHERE id < 101).size";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        Object result = results.iterator().next();
        assertTrue(result instanceof Integer);
        int resultInt = ((Integer) result).intValue();
        assertEquals(resultInt, 100);

        // query with leading/trailing spaces
        queryString = " SELECT DISTINCT itr.key FROM " + regionName + ".entries itr where itr.key = 'key-1' ";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertEquals("key-1", results.asList().get(0));
      }
    });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests client-server query using parameters (compiled queries).
   */
  public void testClientServerQueriesWithParams() throws CacheException {

    final String name = this.getName();

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {
        {"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };

    final int[] expectedResults = new int[] {
        1, // 0
        100, // 1
        100, // 2
        1, // 3
        1, // 4
        50, // 5
    };


    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create and populate region") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });

    // Create client region
    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + this.rootRegionName + "/" + this.regionName;

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams"; 
    createPool(vm1, poolName, host0, port);


    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          

        for (int i=0; i < queryString.length; i++){
          try {
            LogWriterUtils.getLogWriter().info("### Executing Query :" + queryString[i]);
            Query query = qService.newQuery(queryString[i]);
            results = (SelectResults)query.execute(params[i]);
          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString[i], e);
          }
          try {
            assertEquals(expectedResults[i], results.size());
          }catch(Throwable th) {
            fail("Result mismatch for query= " + queryString[i] + " expected = "+expectedResults[i] + " actual="+results.size());
          }
        }        
      }
    });

    final int useMaintainedCompiledQueries = queryString.length;

    // Execute the same compiled queries multiple time
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          
        for (int x=0; x < useMaintainedCompiledQueries; x++){
          for (int i=0; i < queryString.length; i++){
            try {
              LogWriterUtils.getLogWriter().info("### Executing Query :" + queryString[i]);
              Query query = qService.newQuery(queryString[i]);
              results = (SelectResults)query.execute(params[i]);
            } catch (Exception e) {
              Assert.fail("Failed executing " + queryString[i], e);
            }
            try {
              assertEquals(expectedResults[i], results.size());
            }catch(Throwable th) {
              fail("Result mismatch for query= " + queryString[i] + " expected = "+expectedResults[i] + " actual="+results.size());
            }
          }        
        }
      }
    });

    // Validate maintained compiled queries.
    // There should be only queryString.length compiled queries registered.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(queryString.length, compiledQueryCount);
      }
    });

    // Check to see if maintained compiled queries are used.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryUsedCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryUsedCount();
        int numTimesUsed = (useMaintainedCompiledQueries + 1) * queryString.length;
        assertEquals(numTimesUsed, compiledQueryUsedCount);
      }
    });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests client-server query using parameters (compiled queries).
   */
  public void testMulitipleClientServerQueriesWithParams() throws CacheException {
    final String name = this.getName();

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {
        {"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };

    final int[] expectedResults = new int[] {
        1, // 0
        100, // 1
        100, // 2
        1, // 3
        1, // 4
        50, // 5
    };


    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });


    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });

    // Create client region
    final int port0 = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + this.rootRegionName + "/" + this.regionName;

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          
        for (int j=0; j < queryString.length; j++){
          for (int i=0; i < queryString.length; i++){
            try {
              LogWriterUtils.getLogWriter().info("### Executing Query :" + queryString[i]);
              Query query = qService.newQuery(queryString[i]);
              results = (SelectResults)query.execute(params[i]);
            } catch (Exception e) {
              Assert.fail("Failed executing " + queryString[i], e);
            }
            try {
              assertEquals(expectedResults[i], results.size());
            }catch(Throwable th) {
              fail("Result mismatch for query= " + queryString[i] + " expected = "+expectedResults[i] + " actual="+results.size());
            }
          }
        }
      }
    };

    vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    // Validate maintained compiled queries.
    // There should be only queryString.length compiled queries registered.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(queryString.length, compiledQueryCount);
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    
    // Validate maintained compiled queries.
    // All the queries will be still present in the server.
    // They will be cleaned up periodically.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(queryString.length, compiledQueryCount);
      }
    });
    
    vm1.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(queryString.length, compiledQueryCount);
      }
    });
 
    // recreate clients and execute queries.
    createPool(vm2, poolName, new String[]{host0, host0}, new int[]{port1, port0}, true);
    createPool(vm3, poolName, new String[]{host0, host0}, new int[]{port0, port1}, true);

    vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);
    
    // Validate maintained compiled queries.
    // All the queries will be still present in the server.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(queryString.length, compiledQueryCount);
      }
    });
    
    vm1.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(queryString.length, compiledQueryCount);
      }
    });
    
    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
    vm1.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });

  }

  /**
   * Tests client-server compiled query register and cleanup.
   */
  public void testClientServerCompiledQueryRegisterAndCleanup() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {
        {"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };
    
    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create and populate region.") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });

    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + this.rootRegionName + "/" + this.regionName;

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams"; 
    createPool(vm1, poolName, host0, port);
    createPool(vm2, poolName, host0, port);

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        executeCompiledQueries(poolName, params);      }
    });

    // Execute client queries
    vm2.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        executeCompiledQueries(poolName, params);      }
    });
    
    // Validate maintained compiled queries.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(queryString.length, compiledQueryCount);
      }
    });

    closeClient(vm1);

    // Validate maintained compiled queries.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(queryString.length, compiledQueryCount);
      }
    });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });    
  }

  /**
   * Tests client-server compiled query register and cleanup.
   */
  public void testClientServerCompiledQueryTimeBasedCleanup() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {
        {"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };
    
    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create and populate region.") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
        QueryService qs = getCache().getQueryService();
        DefaultQuery query = (DefaultQuery)qs.newQuery("Select * from " + regName);
        query.setTestCompiledQueryClearTime(2 * 1000); 
      }
    });

    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + this.rootRegionName + "/" + this.regionName;

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams"; 
    createPool(vm1, poolName, host0, port);
    createPool(vm2, poolName, host0, port);

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        executeCompiledQueries(poolName, params);
      }
    });

    // Execute client queries
    vm2.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        executeCompiledQueries(poolName, params);
      }
    });
        
    // Validate maintained compiled queries.
    this.validateCompiledQuery(vm0, 0);
        
    // Recreate compiled queries.
    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        executeCompiledQueries(poolName, params);
      }
    });

    // Execute client queries
    // The client2 will be using the queries.
    vm2.invokeAsync(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        for (int i=0; i < 10; i++) {
          Wait.pause(200);
          executeCompiledQueries(poolName, params);
        }
      }
    });
       
    // Validate maintained compiled queries.
    validateCompiledQuery(vm0, queryString.length);

    // Let the compiled queries to be idle (not used).
    Wait.pause(2 * 1000);    
    
    // Validate maintained compiled queries.
    this.validateCompiledQuery(vm0, 0);

    // Close clients
    closeClient(vm2);
    closeClient(vm1);
    
    // Validate maintained compiled queries.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(0, compiledQueryCount);
      }
    });
    
    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });    
  }

  /**
   * Tests client-server compiled query register and cleanup.
   * It creates the client connections without the subscription 
   * enabled. This doesn't create any client proxy on the server.
   */
  public void testClientServerCompiledQueryCleanup() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {
        {"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };
    
    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create and populate region.") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
        QueryService qs = getCache().getQueryService();
        DefaultQuery query = (DefaultQuery)qs.newQuery("Select * from " + regName);
        query.setTestCompiledQueryClearTime(2 * 1000); 
      }
    });

    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + this.rootRegionName + "/" + this.regionName;

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams"; 
    final boolean subscriptiuonEnabled = false;
    createPool(vm1, poolName, host0, port, subscriptiuonEnabled);
    createPool(vm2, poolName, host0, port, subscriptiuonEnabled);

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        executeCompiledQueries(poolName, params);
      }
    });

    // Execute client queries
    vm2.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        executeCompiledQueries(poolName, params);
      }
    });
    
    // Validate maintained compiled queries.
    this.validateCompiledQuery(vm0, 0);
    
    
    // Recreate compiled queries.
    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        executeCompiledQueries(poolName, params);
      }
    });

    // Execute client queries
    // The client2 will be using the queries.
    vm2.invokeAsync(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        for (int i=0; i < 10; i++) {
          Wait.pause(10);
          executeCompiledQueries(poolName, params);
        }
      }
    });
       
    // Validate maintained compiled queries.
    this.validateCompiledQuery(vm0, queryString.length);

    // Close clients
    // Let the compiled queries to be idle (not used).
    //pause(2 * 1000);    
    
    // Validate maintained compiled queries.
    this.validateCompiledQuery(vm0, 0);
    
    // Recreate compiled queries.
    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        executeCompiledQueries(poolName, params);
      }
    });
    
    
    // Validate maintained compiled queries.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
        assertEquals(queryString.length, compiledQueryCount);
      }
    });

    // Close clients
    closeClient(vm2);
    closeClient(vm1);

    // Validate maintained compiled queries.
    // since not used it should get cleaned up after sometime.
    this.validateCompiledQuery(vm0, 0);
    
    
    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });    
  }

  /**
   * Tests client-server query using parameters (compiled queries).
   */
  public void testBindParamsWithMulitipleClients() throws CacheException {

    final String name = this.getName();

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {
        {"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };

    final String[] querys = new String[] {
        "SELECT itr.value FROM " + regName + ".entries itr where itr.key = 'key-1'", // 0
        "SELECT DISTINCT * FROM " + regName + " WHERE id < 101 ORDER BY id", // 1
        "SELECT DISTINCT * FROM " + regName + " WHERE id < 101 ORDER BY id", // 2
        "(SELECT DISTINCT * FROM " + regName + " WHERE id < 101).size", // 3
        "SELECT * FROM " + regName + " WHERE id = 50 and Ticker = 'ibm'", // 4
        "SELECT * FROM " + regName + " WHERE id < 50 and Ticker = 'ibm'", // 5
    };

    final int[] expectedResults = new int[] {
        1, // 0
        100, // 1
        100, // 2
        1, // 3
        1, // 4
        50, // 5
    };

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });

    // Create client region
    final int port0 = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + this.rootRegionName + "/" + this.regionName;

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams"; 
    createPool(vm1, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port0}, true);

    // Execute client query multiple times and validate results.
    SerializableRunnable executeSameQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService qService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }      
        // Use minimal query, so that there will be multiple 
        // clients using the same compiled query at server.
        for (int j=0; j < 5; j++){
          for (int i=0; i < 2; i++){
            try {
              LogWriterUtils.getLogWriter().info("### Executing Query :" + queryString[i]);
              Query query = qService.newQuery(queryString[i]);
              rs[0][0] = (SelectResults)query.execute(params[i]);
              Query query2 = qService.newQuery(querys[i]);
              rs[0][1] = (SelectResults)query2.execute();
              // Compare results.
            } catch (Exception e) {
              Assert.fail("Failed executing " + queryString[i], e);
            }
            LogWriterUtils.getLogWriter().info("### Comparing results for Query :" + ((i+1) * (j+1)) + " : " + queryString[i]);
            compareQueryResultsWithoutAndWithIndexes(rs, 1);
            LogWriterUtils.getLogWriter().info("### Done Comparing results for Query :" + ((i+1) * (j+1)) + " : " + queryString[i]); 
          }
        }
      }
    };

    vm1.invokeAsync(executeSameQueries);
    vm2.invokeAsync(executeSameQueries);
    vm3.invokeAsync(executeSameQueries);
    
    // Wait till the query execution completes.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryUsedCount = -1;
        while (true) {
          LogWriterUtils.getLogWriter().info("### CompiledQueryUsedCount :" + compiledQueryUsedCount);
          if (compiledQueryUsedCount == CacheClientNotifier.getInstance().getStats().getCompiledQueryUsedCount()) {
            LogWriterUtils.getLogWriter().info("### previous and current CompiledQueryUsedCounts are same :" + compiledQueryUsedCount);
            break;
          } else {
            compiledQueryUsedCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryUsedCount();
            try {
              Thread.currentThread().sleep(3 * 1000);
            } catch (Exception ex) {
              break;
            }
          }
        }
      }
    });
    
    // Execute client queries and validate results.
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        SelectResults results2 = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService qService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          
        for (int j=0; j < queryString.length; j++){
          for (int i=0; i < queryString.length; i++){
            try {
              LogWriterUtils.getLogWriter().info("### Executing Query :" + queryString[i]);
              Query query = qService.newQuery(queryString[i]);
              rs[0][0] = (SelectResults)query.execute(params[i]);
              Query query2 = qService.newQuery(querys[i]);
              rs[0][1] = (SelectResults)query2.execute();
              // Compare results.
              compareQueryResultsWithoutAndWithIndexes(rs, 1);
            } catch (Exception e) {
              Assert.fail("Failed executing " + queryString[i], e);
            }
          }
        }
      }
    };

    vm1.invokeAsync(executeQueries);
    vm2.invokeAsync(executeQueries);
    vm3.invokeAsync(executeQueries);
    
    // Wait till the query execution completes.
    vm0.invoke(new CacheSerializableRunnable("validate compiled query.") {
      public void run2() throws CacheException {
        long compiledQueryUsedCount = -1;
        while (true) {
          LogWriterUtils.getLogWriter().info("### previous CompiledQueryUsedCount :" + compiledQueryUsedCount);
          if (compiledQueryUsedCount == CacheClientNotifier.getInstance().getStats().getCompiledQueryUsedCount()) {
            LogWriterUtils.getLogWriter().info("### previous and current CompiledQueryUsedCounts are same :" + compiledQueryUsedCount);
            break;
          } else {
            compiledQueryUsedCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryUsedCount();
            try {
              Thread.currentThread().sleep(3 * 1000);
            } catch (Exception ex) {
              break;
            }
          }
        }
      }
    });
      
    this.closeClient(vm1);
    this.closeClient(vm2);
    this.closeClient(vm3);
    
    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
    
  }

  /**
   * Tests remote join query execution.
   */
  public void testRemoteJoinRegionQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        createRegion(name+"1", factory.create());
        createRegion(name+"2", factory.create());
        Wait.pause(1000);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(name+"1");
        for (int i=0; i<numberOfEntries; i++) {
          region1.put("key-"+i, new TestObject(i, "ibm"));
        }
        Region region2 = getRootRegion().getSubregion(name+"2");
        for (int i=0; i<numberOfEntries; i++) {
          region2.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });

    // Create client region
    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());
    final String regionName1 = "/" + rootRegionName + "/" + name+"1";
    final String regionName2 = "/" + rootRegionName + "/" + name+"2";

    // Create client pool.
    final String poolName = "testRemoteJoinRegionQueries"; 
    createPool(vm1, poolName, host0, port);

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        String queryString = null;
        SelectResults results = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          

        queryString =
          "select distinct a, b.price from " + regionName1 + " a, " + regionName2 + " b where a.price = b.price";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());

        queryString =
          "select distinct a, b.price from " + regionName1 + " a, " + regionName2 + " b where a.price = b.price and a.price = 50";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && results.getCollectionType().getElementType().isStructType());
      }
    });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * Tests remote query execution using a BridgeClient as the CacheWriter
   * and CacheLoader.
   */
  public void testRemoteBridgeClientQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        createRegion(name, factory.create());
        Wait.pause(1000);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
      }
    });

    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName1 = "testRemoteBridgeClientQueries1"; 
    final String poolName2 = "testRemoteBridgeClientQueries2"; 

    createPool(vm1, poolName1, host0, port);
    createPool(vm2, poolName2, host0, port);

    // Execute client queries in VM1
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        String queryString = null;
        SelectResults results = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName1)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          

        queryString = "SELECT DISTINCT itr.value FROM " + regionName + ".entries itr where itr.key = 'key-1'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());
        assertTrue(results.asList().get(0) instanceof TestObject);

        queryString = "SELECT DISTINCT itr.key FROM " + regionName + ".entries itr where itr.key = 'key-1'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());
        assertEquals("key-1", results.asList().get(0));
      }
    });

    // Execute client queries in VM2
    vm2.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        String queryString = null;
        SelectResults results = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName2)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          

        queryString = "SELECT DISTINCT itr.value FROM " + regionName + ".entries itr where itr.key = 'key-1'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());
        assertTrue(results.asList().get(0) instanceof TestObject);

        queryString = "SELECT DISTINCT itr.key FROM " + regionName + ".entries itr where itr.key = 'key-1'";
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(1, results.size());
        assertTrue(!results.getCollectionType().allowsDuplicates() && !results.getCollectionType().getElementType().isStructType());
        assertEquals("key-1", results.asList().get(0));
      }
    });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }

  /**
   * This the dunit test for the bug no : 36969
   * @throws Exception
   */

  public void testBug36969() throws Exception
  {
    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);

        Wait.pause(1000);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Create two regions") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        final Region region1 = createRegion(name, factory.createRegionAttributes());
        final Region region2 = createRegion(name+"_2", factory.createRegionAttributes());
        QueryObserverHolder.setInstance(new QueryObserverAdapter() {
          public void afterQueryEvaluation(Object result) {
            //Destroy the region in the test
            region1.close();
          }

        });
        Wait.pause(1000);
        for (int i=0; i<numberOfEntries; i++) {
          region1.put("key-"+i, new TestObject(i, "ibm"));
          region2.put("key-"+i, new TestObject(i, "ibm"));
        }

      }
    });

    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName1 = "/" + rootRegionName + "/" + name;
    final String regionName2 = "/" + rootRegionName + "/" + name + "_2";

    // Create client pool.
    final String poolName = "testBug36969";
    createPool(vm1, poolName, host0, port);

    // Execute client queries in VM1      
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        String queryString = "select distinct * from " + regionName1 + ", "+regionName2;
//        SelectResults results = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        try {
          Query query = qService.newQuery(queryString);
          query.execute();
          fail("The query should have experienced RegionDestroyedException");
        } catch(Exception e) {
          // OK
        }
      }
    });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        QueryObserverHolder.setInstance(new QueryObserverAdapter());
        stopBridgeServer(getCache());
      }
    });



  }

  /**
   * Tests remote full region query execution.
   */
  public void testRemoteSortQueriesUsingIndex() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        createRegion(name, factory.create());
        Wait.pause(1000);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    // Initialize server region
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "ibm"));
        }
        // Create index
        try {
          QueryService qService = region.getCache().getQueryService();
          qService.createIndex("idIndex",IndexType.FUNCTIONAL, "id", region.getFullPath());
        } catch (Exception e) {
          Assert.fail("Failed to create index.", e);
        }          

      }
    });

    // Create client region
    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName = "testRemoteFullRegionQueries"; 
    createPool(vm1, poolName, host0, port);


    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        String queryString = null;
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService qService = null;
        Integer v1 = 0;
        Integer v2 = 0;
        
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }          

        // order by value query
        String[] qString = {
            "SELECT DISTINCT * FROM " + regionName + " WHERE id < 101 ORDER BY id",
            "SELECT DISTINCT id FROM " + regionName + " WHERE id < 101 ORDER BY id",
        };
        
        for (int cnt=0; cnt < qString.length; cnt++) {
          queryString = qString[cnt];
          try {
            Query query = qService.newQuery(queryString);
            results = (SelectResults)query.execute();
          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries, results.size());
          // All order-by query results are stored in a ResultsCollectionWrapper
          // wrapping a list, so the assertion below is not correct even though
          // it should be.
          //assertTrue(!results.getCollectionType().allowsDuplicates());
          assertTrue(results.getCollectionType().isOrdered());
          comparator = new IdValueComparator();

          resultsArray = results.toArray();
          for (int i=0; i<resultsArray.length; i++) {
            if (i+1 != resultsArray.length) {
              // The id of the current element in the result set must be less
              // than the id of the next one to pass.
              if (resultsArray[i] instanceof TestObject) {
                v1 = ((TestObject)resultsArray[i]).getId();
                v2 = ((TestObject)resultsArray[i+1]).getId();
              } else {
                v1 = (Integer)resultsArray[i];
                v2 = (Integer)resultsArray[i + 1];
              }
              assertTrue("The id for " + resultsArray[i] + " should be less than the id for " + resultsArray[i+1], 
                  comparator.compare(v1, v2) == -1);
            }
          }
        }
      
        // order by struct query
        String[] qString2 = {
            "SELECT DISTINCT id, ticker, price FROM " + regionName + " WHERE id < 101 ORDER BY id",
            "SELECT DISTINCT ticker, id FROM " + regionName + " WHERE id < 101  ORDER BY id",
            "SELECT DISTINCT id, ticker FROM " + regionName + " WHERE id < 101  ORDER BY id asc",
        };

        for (int cnt=0; cnt < qString2.length; cnt++) {
          queryString = qString2[cnt];
          try {
            Query query = qService.newQuery(queryString);
            results = (SelectResults)query.execute();
          } catch (Exception e) {
            Assert.fail("Failed executing " + queryString, e);
          }
          assertEquals(numberOfEntries, results.size());
          // All order-by query results are stored in a ResultsCollectionWrapper
          // wrapping a list, so the assertion below is not correct even though
          // it should be.
          //assertTrue(!results.getCollectionType().allowsDuplicates());
          assertTrue(results.getCollectionType().isOrdered());
          comparator = new StructIdComparator();
          resultsArray = results.toArray();
          for (int i=0; i<resultsArray.length; i++) {
            if (i+1 != resultsArray.length) {
              // The id of the current element in the result set must be less
              // than the id of the next one to pass.
              assertTrue("The id for " + resultsArray[i] + " should be less than the id for " + resultsArray[i+1], comparator.compare(resultsArray[i], resultsArray[i+1]) == -1);
            }
          }
        }
      }
    });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    });
  }


  public void testUnSupportedOps() throws Exception
  {
    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        system = (InternalDistributedSystem) DistributedSystem.connect(config);

        Wait.pause(1000);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Create two regions") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        final Region region1 = createRegion(name, factory.createRegionAttributes());
        final Region region2 = createRegion(name+"_2", factory.createRegionAttributes());
        for (int i=0; i<numberOfEntries; i++) {
          region1.put("key-"+i, new TestObject(i, "ibm"));
          region2.put("key-"+i, new TestObject(i, "ibm"));
        }

      }
    });

    final int port = vm0.invokeInt(QueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName1 = "/" + rootRegionName + "/" + name;
//    final String regionName2 = "/" + rootRegionName + "/" + name + "_2";

    // Create client pool.
    final String poolName = "testUnSupportedOps";
    createPool(vm1, poolName, host0, port);

    // Execute client queries in VM1      
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        final Region region1 = createRegion(name, factory.createRegionAttributes());

        String queryString = "select distinct * from " + regionName1 + " where ticker = $1";
        Object[] params = new Object[] { new String("ibm")};
//        SelectResults results = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        // Testing Remote Query with params.
        try {
          Query query = qService.newQuery(queryString);
          query.execute(params);
        } catch(UnsupportedOperationException e) {
          // Expected behavior.
        } catch (Exception e){
          Assert.fail("Failed with UnExpected Exception.", e);
        }

        // Test with Index.
        try {
          qService.createIndex("test", IndexType.FUNCTIONAL,"ticker",regionName1);
        } catch(UnsupportedOperationException e) {
          // Expected behavior.
        } catch (Exception e){
          Assert.fail("Failed with UnExpected Exception.", e);
        }

        try {
          String importString = "import com.gemstone.gemfire.admin.QueryUsingPoolDUnitTest.TestObject;";
          qService.createIndex("test", IndexType.FUNCTIONAL,"ticker",regionName1, importString);
        } catch(UnsupportedOperationException e) {
          // Expected behavior.
        } catch (Exception e){
          Assert.fail("Failed with UnExpected Exception.", e);
        }

        try {
          qService.getIndex(region1, "test");
        } catch(UnsupportedOperationException e) {
          // Expected behavior.
        } 

        try {
          qService.getIndexes(region1);
        } catch(UnsupportedOperationException e) {
          // Expected behavior.
        } 

        try {
          qService.getIndexes(region1, IndexType.FUNCTIONAL);
        } catch(UnsupportedOperationException e) {
          // Expected behavior.
        } 

        //try {                        
        //  qService.getIndex(Index index);
        //} catch(UnsupportedOperationException e) {
        // // Expected behavior.
        //} 

        try {
          qService.removeIndexes(region1);
        } catch(UnsupportedOperationException e) {
          // Expected behavior.
        } 

        try {
          qService.removeIndexes();
        } catch(UnsupportedOperationException e) {
          // Expected behavior.
        } 

      }
    });

    // Stop server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
        QueryObserverHolder.setInstance(new QueryObserverAdapter());
        stopBridgeServer(getCache());
      }
    });

  }

  /** Creates a new instance of StructSetOrResultsSet */
  private void compareQueryResultsWithoutAndWithIndexes(Object[][] r, int len) {

    Set set1 = null;
    Set set2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;
            
    
    for (int j = 0; j < len; j++) {      
      type1 = ((SelectResults)r[j][0]).getCollectionType().getElementType();
      type2 = ((SelectResults)r[j][1]).getCollectionType().getElementType();
      if ((type1.getClass().getName()).equals(type2.getClass().getName())) {
        LogWriterUtils.getLogWriter().info("Both SelectResults are of the same Type i.e.--> "
            + ((SelectResults)r[j][0]).getCollectionType().getElementType());
      }
      else {
        LogWriterUtils.getLogWriter().info("Classes are : " + type1.getClass().getName() + " "
            + type2.getClass().getName());
        fail("FAILED:Select result Type is different in both the cases");
      }
      if (((SelectResults)r[j][0]).size() == ((SelectResults)r[j][1]).size()) {
        LogWriterUtils.getLogWriter().info("Both SelectResults are of Same Size i.e.  Size= "
            + ((SelectResults)r[j][1]).size());
      }
      else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
            + ((SelectResults)r[j][0]).size() + " Size2 = "
            + ((SelectResults)r[j][1]).size());
      }
      set2 = (((SelectResults)r[j][1]).asSet());
      set1 = (((SelectResults)r[j][0]).asSet());
      
      LogWriterUtils.getLogWriter().info(" SIZE1 = " + set1.size() + " SIZE2 = " + set2.size());
      
//      boolean pass = true;
      itert1 = set1.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        itert2 = set2.iterator();

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          LogWriterUtils.getLogWriter().info("### Comparing results..");
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            LogWriterUtils.getLogWriter().info("ITS a Set");
            Object[] values1 = ((Struct)p1).getFieldValues();
            Object[] values2 = ((Struct)p2).getFieldValues();
            assertEquals(values1.length, values2.length);
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual = elementEqual
                  && ((values1[i] == values2[i]) || values1[i]
                      .equals(values2[i]));
            }
            exactMatch = elementEqual;
          }
          else {
            LogWriterUtils.getLogWriter().info("Not a Set p2:" + p2 + " p1: " + p1);
            if (p2 instanceof TestObject) {
              LogWriterUtils.getLogWriter().info("An instance of TestObject");
              exactMatch = p2.equals(p1);
            } else {
              LogWriterUtils.getLogWriter().info("Not an instance of TestObject" + p2.getClass().getCanonicalName());
              exactMatch = p2.equals(p1);
            }
          }
          if (exactMatch) {
            LogWriterUtils.getLogWriter().info("Exact MATCH");
            break;
          }
        }
        if (!exactMatch) {
          LogWriterUtils.getLogWriter().info("NOT A MATCH");
          fail("Atleast one element in the pair of SelectResults supposedly identical, is not equal ");
        }
      }
      LogWriterUtils.getLogWriter().info("### Done Comparing results..");
    }
  }

  protected void configAndStartBridgeServer() {
    Properties config = new Properties();
    config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
    system = (InternalDistributedSystem) DistributedSystem.connect(config);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    createRegion(this.regionName, this.rootRegionName, factory.create());
    Wait.pause(1000);
    try {
      startBridgeServer(0, false);
    } catch (Exception ex) {
      Assert.fail("While starting CacheServer", ex);
    }
  }

  protected void executeCompiledQueries(String poolName, Object[][] params) {
    SelectResults results = null;
    Comparator comparator = null;
    Object[] resultsArray = null;
    QueryService qService = null;

    try {
      qService = (PoolManager.find(poolName)).getQueryService();
    } catch (Exception e) {
      Assert.fail("Failed to get QueryService.", e);
    }          

    for (int i=0; i < queryString.length; i++){
      try {
        LogWriterUtils.getLogWriter().info("### Executing Query :" + queryString[i]);
        Query query = qService.newQuery(queryString[i]);
        results = (SelectResults)query.execute(params[i]);
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString[i], e);
      }
    }        
  }
  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   */
  protected void startBridgeServer(int port, boolean notifyBySubscription)
  throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /**
   * Stops the bridge server that serves up the given cache.
   */
  protected void stopBridgeServer(Cache cache) {
    CacheServer bridge =
      (CacheServer) cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  /* Close Client */
  public void closeClient(VM client) {
    SerializableRunnable closeCache =
      new CacheSerializableRunnable("Close Client") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Close Client. ###");
        try {
          closeCache();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("### Failed to get close client. ###");
        }
      }
    };
    
    client.invoke(closeCache);
    Wait.pause(2 * 1000);
  }
  
  private static int getCacheServerPort() {
    return bridgeServerPort;
  }

  public static class TestObject implements DataSerializable {
    protected String _ticker;
    protected int _price;
    public int id;
    public int important;
    public int selection;
    public int select;

    public TestObject() {}

    public TestObject(int id, String ticker) {
      this.id = id;
      this._ticker = ticker;
      this._price = id;
      this.important = id;
      this.selection =id;
      this.select =id;
    }

    public int getId() {
      return this.id;
    }

    public String getTicker() {
      return this._ticker;
    }

    public int getPrice() {
      return this._price;
    }

    public void toData(DataOutput out) throws IOException
    {
      //System.out.println("Is serializing in WAN: " + GatewayEventImpl.isSerializingValue());
      out.writeInt(this.id);
      DataSerializer.writeString(this._ticker, out);
      out.writeInt(this._price);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException
    {
      //System.out.println("Is deserializing in WAN: " + GatewayEventImpl.isDeserializingValue());
      this.id = in.readInt();
      this._ticker = DataSerializer.readString(in);
      this._price = in.readInt();
    }

    @Override
    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer
      .append("TestObject [")
      .append("id=")
      .append(this.id)
      .append("; ticker=")
      .append(this._ticker)
      .append("; price=")
      .append(this._price)
      .append("]");
      return buffer.toString();
    }

    @Override
    public boolean equals(Object o){
//      getLogWriter().info("In TestObject.equals()");
      TestObject other = (TestObject)o;
      if ((id == other.id) && (_ticker.equals(other._ticker))) {
        return true;
      } else {
//        getLogWriter().info("NOT EQUALS");  
        return false;
      }
    }
    
    @Override
    public int hashCode(){
      return this.id;
    }

  }

  public static class IdComparator implements Comparator {

    public int compare(Object obj1, Object obj2) {
      int obj1Id = ((TestObject) obj1).getId();
      int obj2Id = ((TestObject) obj2).getId();
      if (obj1Id > obj2Id) {
        return 1;
      } else if (obj1Id < obj2Id) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  public static class IdValueComparator implements Comparator {

    public int compare(Object obj1, Object obj2) {
      int obj1Id = ((Integer) obj1).intValue();
      int obj2Id = ((Integer) obj2).intValue();
      if (obj1Id > obj2Id) {
        return 1;
      } else if (obj1Id < obj2Id) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  public static class StructIdComparator implements Comparator {

    public int compare(Object obj1, Object obj2) {
      int obj1Id = ((Integer) ((Struct) obj1).get("id")).intValue();
      int obj2Id = ((Integer) ((Struct) obj2).get("id")).intValue();
      if (obj1Id > obj2Id) {
        return 1;
      } else if (obj1Id < obj2Id) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}

