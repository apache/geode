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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * Tests remote (client/server) query execution.
 *
 * @since GemFire 5.0.1
 */
@Category({OQLQueryTest.class})
public class QueryUsingPoolDUnitTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();

  /**
   * The port on which the cache server was started in this VM
   */
  private static int bridgeServerPort;

  private String rootRegionName;
  private String regionName;
  private String regName;

  // Used with compiled queries.
  private String[] queryString;

  @Override
  public final void postSetUp() throws Exception {
    this.rootRegionName = "root";
    this.regionName = this.getName();
    this.regName = "/" + this.rootRegionName + "/" + this.regionName;

    this.queryString =
        new String[] {"SELECT itr.value FROM " + this.regName + ".entries itr where itr.key = $1", // 0
            "SELECT DISTINCT * FROM " + this.regName + " WHERE id < $1 ORDER BY   id", // 1
            "SELECT DISTINCT * FROM " + this.regName + " WHERE id < $1 ORDER BY id", // 2
            "(SELECT DISTINCT * FROM " + this.regName + " WHERE id < $1).size", // 3
            "SELECT * FROM " + this.regName + " WHERE id = $1 and Ticker = $2", // 4
            "SELECT * FROM " + this.regName + " WHERE id < $1 and Ticker = $2", // 5
        };

    disconnectAllFromDS();
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Socket input is shutdown");
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  public void createPool(String poolName, String server, int port, boolean subscriptionEnabled) {
    createPool(poolName, new String[] {server}, new int[] {port}, subscriptionEnabled);
  }

  public void createPool(String poolName, String server, int port) {
    createPool(poolName, new String[] {server}, new int[] {port}, false);
  }

  public void createPool(final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled) {
    // Create Cache.
    getCache(true);

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.setSubscriptionEnabled(subscriptionEnabled);
    for (int i = 0; i < servers.length; i++) {
      logger.info("### Adding to Pool. ### Server : " + servers[i] + " Port : " + ports[i]);
      poolFactory.addServer(servers[i], ports[i]);
    }
    poolFactory.setMinConnections(1);
    poolFactory.setMaxConnections(5);
    poolFactory.setRetryAttempts(5);
    poolFactory.create(poolName);
  }

  public void validateCompiledQuery(final long compiledQueryCount) {
    await().timeout(60, TimeUnit.SECONDS)
        .until(() -> CacheClientNotifier.getInstance().getStats()
            .getCompiledQueryCount() == compiledQueryCount);
  }

  /**
   * Tests remote import query execution.
   */
  @Test
  public void testRemoteImportQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke("Create cache server", () -> {
      createAndStartBridgeServer();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      createRegion(name, rootRegionName, factory.create());
    });

    // Initialize server region
    vm0.invoke("Create cache server", () -> {
      Region region = getRootRegion().getSubregion(name);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObject(i, "ibm"));
      }
    });

    final int port =
        vm0.invoke("GetCacheServerPort", () -> QueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());
    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName = "testRemoteImportQueries";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries
    vm1.invoke("Execute queries", () -> {
      String queryString = null;
      SelectResults results = null;

      QueryService qService = null;

      try {
        qService = (PoolManager.find(poolName)).getQueryService();
      } catch (Exception e) {
        Assert.fail("Failed to get QueryService.", e);
      }

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from "
              + regionName;

      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }

      assertEquals(numberOfEntries, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from "
              + regionName + " where ticker = 'ibm'";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(numberOfEntries, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from "
              + regionName + " where ticker = 'IBM'";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(0, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from "
              + regionName + " where price > 49";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(numberOfEntries / 2, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from "
              + regionName + " where price = 50";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(1, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct * from "
              + regionName + " where ticker = 'ibm' and price = 50";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(1, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates());
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests remote struct query execution.
   */
  @Test
  public void testRemoteStructQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    final int port = vm0.invoke("Create cache server", () -> {
      setupBridgeServerAndCreateData(name, numberOfEntries);
      return getCacheServerPort();
    });

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName = "testRemoteStructQueries";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries
    vm1.invoke("Execute queries", () -> {
      String queryString = null;
      SelectResults results = null;

      QueryService qService = null;

      try {
        qService = (PoolManager.find(poolName)).getQueryService();
      } catch (Exception e) {
        Assert.fail("Failed to get QueryService.", e);
      }

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from "
              + regionName;
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(numberOfEntries, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates()
          && results.getCollectionType().getElementType().isStructType());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from "
              + regionName + " where ticker = 'ibm'";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(numberOfEntries, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates()
          && results.getCollectionType().getElementType().isStructType());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from "
              + regionName + " where ticker = 'IBM'";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(0, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates()
          && results.getCollectionType().getElementType().isStructType());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from "
              + regionName + " where price > 49";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(numberOfEntries / 2, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates()
          && results.getCollectionType().getElementType().isStructType());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from "
              + regionName + " where price = 50";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(1, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates()
          && results.getCollectionType().getElementType().isStructType());

      queryString =
          "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject; select distinct ticker, price from "
              + regionName + " where ticker = 'ibm' and price = 50";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(1, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates()
          && results.getCollectionType().getElementType().isStructType());
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  private void setupBridgeServerAndCreateData(String name, int numberOfEntries) {
    createAndStartBridgeServer();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    createRegion(this.regionName, this.rootRegionName, factory.create());
    Region region = getRootRegion().getSubregion(name);
    for (int i = 0; i < numberOfEntries; i++) {
      region.put("key-" + i, new TestObject(i, "ibm"));
    }
  }


  /**
   * Tests remote full region query execution.
   */
  @Test
  public void testRemoteFullRegionQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke("Create cache server", () -> {
      createAndStartBridgeServer();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      createRegion(name, factory.create());
    });

    // Initialize server region
    vm0.invoke("Create cache server", () -> {
      Region region = getRootRegion().getSubregion(name);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObject(i, "ibm"));
      }
    });

    // Create client region
    final int port = vm0.invoke(() -> QueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName = "testRemoteFullRegionQueries";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries
    vm1.invoke("Execute queries", () -> {
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
      queryString =
          "SELECT DISTINCT itr.value FROM " + regionName + ".entries itr where itr.key = 'key-1'";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(1, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates());
      assertTrue(results.asList().get(0) instanceof TestObject);

      // key query
      queryString =
          "SELECT DISTINCT itr.key FROM " + regionName + ".entries itr where itr.key = 'key-1'";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
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
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(numberOfEntries, results.size());
      // All order-by query results are stored in a ResultsCollectionWrapper
      // wrapping a list, so the assertion below is not correct even though
      // it should be.
      // assertTrue(!results.getCollectionType().allowsDuplicates());
      assertTrue(results.getCollectionType().isOrdered());
      comparator = new IdComparator();
      resultsArray = results.toArray();
      for (int i = 0; i < resultsArray.length; i++) {
        if (i + 1 != resultsArray.length) {
          // The id of the current element in the result set must be less
          // than the id of the next one to pass.
          assertTrue(
              "The id for " + resultsArray[i] + " should be less than the id for "
                  + resultsArray[i + 1],
              comparator.compare(resultsArray[i], resultsArray[i + 1]) == -1);
        }
      }

      // order by struct query
      queryString =
          "SELECT DISTINCT id, ticker, price FROM " + regionName + " WHERE id < 101 ORDER BY id";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(numberOfEntries, results.size());
      // All order-by query results are stored in a ResultsCollectionWrapper
      // wrapping a list, so the assertion below is not correct even though
      // it should be.
      // assertTrue(!results.getCollectionType().allowsDuplicates());
      assertTrue(results.getCollectionType().isOrdered());
      comparator = new StructIdComparator();
      resultsArray = results.toArray();
      for (int i = 0; i < resultsArray.length; i++) {
        if (i + 1 != resultsArray.length) {
          // The id of the current element in the result set must be less
          // than the id of the next one to pass.
          assertTrue(
              "The id for " + resultsArray[i] + " should be less than the id for "
                  + resultsArray[i + 1],
              comparator.compare(resultsArray[i], resultsArray[i + 1]) == -1);
        }
      }

      // size query
      queryString = "(SELECT DISTINCT * FROM " + regionName + " WHERE id < 101).size";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(1, results.size());
      Object result = results.iterator().next();
      assertTrue(result instanceof Integer);
      int resultInt = ((Integer) result).intValue();
      assertEquals(resultInt, 100);

      // query with leading/trailing spaces
      queryString =
          " SELECT DISTINCT itr.key FROM " + regionName + ".entries itr where itr.key = 'key-1' ";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(1, results.size());
      assertEquals("key-1", results.asList().get(0));
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  private int createAndStartBridgeServer() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    getSystem(config);
    try {
      return startBridgeServer(0, false);
    } catch (Exception ex) {
      Assert.fail("While starting CacheServer", ex);
      return -1;
    }
  }

  /**
   * Tests client-server query using parameters (compiled queries).
   */
  @Test
  public void testClientServerQueriesWithParams() throws CacheException {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {{"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };

    final int[] expectedResults = new int[] {1, // 0
        100, // 1
        100, // 2
        1, // 3
        1, // 4
        50, // 5
    };

    assertNotNull(this.regionName);

    // Start server
    final int port = vm0.invoke("Create cache server", () -> {
      setupBridgeServerAndCreateData(regionName, numberOfEntries);
      return getCacheServerPort();
    });

    // Create client region
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries
    vm1.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        executeQueriesForClientServerQueriesWithParams(results, qService, params, expectedResults);
      }
    });

    final int useMaintainedCompiledQueries = queryString.length;

    // Execute the same compiled queries multiple time
    vm1.invoke("Execute queries", () -> {
      SelectResults results = null;
      QueryService qService = null;

      try {
        qService = (PoolManager.find(poolName)).getQueryService();
      } catch (Exception e) {
        Assert.fail("Failed to get QueryService.", e);
      }
      for (int x = 0; x < useMaintainedCompiledQueries; x++) {
        executeQueriesForClientServerQueriesWithParams(results, qService, params, expectedResults);
      }
    });

    // Validate maintained compiled queries.
    // There should be only queryString.length compiled queries registered.
    vm0.invoke("validate compiled query.", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(queryString.length, compiledQueryCount);
    });

    // Check to see if maintained compiled queries are used.
    vm0.invoke("validate compiled query.", () -> {
      long compiledQueryUsedCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryUsedCount();
      int numTimesUsed = (useMaintainedCompiledQueries + 1) * queryString.length;
      assertEquals(numTimesUsed, compiledQueryUsedCount);
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  private void executeQueriesForClientServerQueriesWithParams(SelectResults results,
      QueryService qService, Object[][] params, int[] expectedResults) {
    for (int i = 0; i < queryString.length; i++) {
      try {
        logger.info("### Executing Query :" + queryString[i]);
        Query query = qService.newQuery(queryString[i]);
        results = (SelectResults) query.execute(params[i]);
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString[i], e);
      }
      try {
        assertEquals(expectedResults[i], results.size());
      } catch (Throwable th) {
        fail("Result mismatch for query= " + queryString[i] + " expected = " + expectedResults[i]
            + " actual=" + results.size());
      }
    }
  }

  /**
   * Tests client-server query using parameters (compiled queries).
   */
  @Test
  public void testMulitipleClientServerQueriesWithParams() throws CacheException {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {{"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };

    final int[] expectedResults = new int[] {1, // 0
        100, // 1
        100, // 2
        1, // 3
        1, // 4
        50, // 5
    };

    // Start server1
    final int port0 = vm0.invoke("Create cache server", () -> {
      setupBridgeServerAndCreateData(regionName, numberOfEntries);
      return getCacheServerPort();
    });

    // Start server2
    final int port1 = vm1.invoke("Create cache server", () -> {
      setupBridgeServerAndCreateData(regionName, numberOfEntries);
      return getCacheServerPort();
    });

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams";
    vm2.invoke("createPool",
        () -> createPool(poolName, new String[] {host0}, new int[] {port0}, true));
    vm3.invoke("createPool",
        () -> createPool(poolName, new String[] {host0}, new int[] {port1}, true));

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        QueryService qService = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        for (int j = 0; j < queryString.length; j++) {
          executeQueriesForClientServerQueriesWithParams(results, qService, params,
              expectedResults);
        }
      }
    };

    vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    // Validate maintained compiled queries.
    // There should be only queryString.length compiled queries registered.
    vm0.invoke("validate compiled query.", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(queryString.length, compiledQueryCount);
    });

    vm2.invoke("closeClient", () -> closeClient());
    vm3.invoke("closeClient", () -> closeClient());

    // Validate maintained compiled queries.
    // All the queries will be still present in the server.
    // They will be cleaned up periodically1
    vm0.invoke("validate compiled query.", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(queryString.length, compiledQueryCount);
    });

    vm1.invoke("validate compiled query.", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(queryString.length, compiledQueryCount);
    });

    // recreate clients and execute queries.
    vm2.invoke("createPool",
        () -> createPool(poolName, new String[] {host0, host0}, new int[] {port1, port0}, true));
    vm3.invoke("createPool",
        () -> createPool(poolName, new String[] {host0, host0}, new int[] {port0, port1}, true));

    vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    // Validate maintained compiled queries.
    // All the queries will be still present in the server.
    vm0.invoke("validate compiled query.", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(queryString.length, compiledQueryCount);
    });

    vm1.invoke("validate compiled query.", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(queryString.length, compiledQueryCount);
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
    vm1.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests client-server compiled query register and cleanup.
   */
  @Test
  public void testClientServerCompiledQueryRegisterAndCleanup() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {{"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };

    // Start server
    final int port = vm0.invoke("Create cache server", () -> {
      setupBridgeServerAndCreateData(name, numberOfEntries);
      return getCacheServerPort();
    });

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));
    vm2.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries
    vm1.invoke("executeCompiledQueries", () -> executeCompiledQueries(poolName, params));

    // Execute client queries
    vm2.invoke("executeCompiledQueries", () -> executeCompiledQueries(poolName, params));

    // Validate maintained compiled queries.
    vm0.invoke("validate compiled query.", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(queryString.length, compiledQueryCount);
    });

    vm1.invoke("closeClient", () -> closeClient());

    // Validate maintained compiled queries.
    vm0.invoke("validate compiled query.", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(queryString.length, compiledQueryCount);
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests client-server compiled query register and cleanup.
   */
  @Test
  public void testClientServerCompiledQueryTimeBasedCleanup() throws CacheException {

    final String name = this.getName();

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {{"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };

    // Start server
    final int port = vm0.invoke("Create cache server", () -> {
      setupBridgeServerAndCreateData(name, numberOfEntries);
      QueryService queryService = getCache().getQueryService();
      queryService.newQuery("Select * from " + regName);
      DefaultQuery.setTestCompiledQueryClearTime(2 * 1000);
      return getCacheServerPort();
    });

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));
    vm2.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries
    vm1.invoke("Execute queries", () -> executeCompiledQueries(poolName, params));

    // Execute client queries
    vm2.invoke("Execute queries", () -> executeCompiledQueries(poolName, params));

    // Validate maintained compiled queries.
    vm0.invoke("validate Compiled query", () -> validateCompiledQuery(0));

    // Recreate compiled queries.
    // Execute client queries
    vm1.invoke("Execute queries", () -> executeCompiledQueries(poolName, params));

    // Execute client queries
    // The client2 will be using the queries.
    vm2.invokeAsync("Execute queries", () -> {
      for (int i = 0; i < 10; i++) {
        Wait.pause(200);
        executeCompiledQueries(poolName, params);
      }
    });

    // Validate maintained compiled queries.
    vm0.invoke("validate Compiled query", () -> validateCompiledQuery(queryString.length));

    // Validate maintained compiled queries.
    vm0.invoke("validate Compiled query", () -> validateCompiledQuery(0));

    // Close clients

    vm1.invoke("closeClient", () -> closeClient());
    vm2.invoke("closeClient", () -> closeClient());

    // Validate maintained compiled queries.
    vm0.invoke("validate compiled query", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(0, compiledQueryCount);
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests client-server compiled query register and cleanup. It creates the client connections
   * without the subscription enabled. This doesn't create any client proxy on the server.
   */
  @Test
  public void testClientServerCompiledQueryCleanup() throws CacheException {

    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {{"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };

    // Start server
    final int port = vm0.invoke("Create cache server", () -> {
      setupBridgeServerAndCreateData(name, numberOfEntries);
      QueryService queryService = getCache().getQueryService();
      queryService.newQuery("Select * from " + regName);
      DefaultQuery.setTestCompiledQueryClearTime(2 * 1000);
      return getCacheServerPort();
    });

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams";
    final boolean subscriptiuonEnabled = false;
    vm1.invoke("createPool", () -> createPool(poolName, host0, port, subscriptiuonEnabled));
    vm2.invoke("createPool", () -> createPool(poolName, host0, port, subscriptiuonEnabled));

    // Execute client queries
    vm1.invoke("Execute queries", () -> executeCompiledQueries(poolName, params));

    // Execute client queries
    vm2.invoke("Execute queries", () -> executeCompiledQueries(poolName, params));

    // Validate maintained compiled queries.
    vm0.invoke("validate Compiled query", () -> validateCompiledQuery(0));

    // Recreate compiled queries.
    // Execute client queries
    vm1.invoke("Execute queries", () -> executeCompiledQueries(poolName, params));

    // Execute client queries
    // The client2 will be using the queries.
    vm2.invokeAsync("Execute queries", () -> {
      for (int i = 0; i < 10; i++) {
        Wait.pause(10);
        executeCompiledQueries(poolName, params);
      }
    });

    // Validate maintained compiled queries.
    vm0.invoke("validate Compiled query", () -> validateCompiledQuery(queryString.length));

    // Close clients
    // Let the compiled queries to be idle (not used).
    // pause(2 * 1000);

    // Validate maintained compiled queries.
    vm0.invoke("validate Compiled query", () -> validateCompiledQuery(0));

    // Recreate compiled queries.
    // Execute client queries
    vm1.invoke("Execute queries", () -> executeCompiledQueries(poolName, params));

    // Validate maintained compiled queries.
    vm0.invoke("validate compiled query.", () -> {
      long compiledQueryCount =
          CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      assertEquals(queryString.length, compiledQueryCount);
    });

    // Close clients
    vm2.invoke("closeClient", () -> closeClient());
    vm1.invoke("closeClient", () -> closeClient());

    // Validate maintained compiled queries.
    // since not used it should get cleaned up after sometime.
    vm0.invoke("validate Compiled query", () -> validateCompiledQuery(0));

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests client-server query using parameters (compiled queries).
   */
  @Test
  public void testBindParamsWithMulitipleClients() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;

    final Object[][] params = new Object[][] {{"key-1"}, // 0
        {101}, // 1
        {101}, // 2
        {101}, // 3
        {50, "ibm"}, // 4
        {50, "ibm"}, // 5
    };

    final String[] querys =
        new String[] {"SELECT itr.value FROM " + regName + ".entries itr where itr.key = 'key-1'", // 0
            "SELECT DISTINCT * FROM " + regName + " WHERE id < 101 ORDER BY id", // 1
            "SELECT DISTINCT * FROM " + regName + " WHERE id < 101 ORDER BY id", // 2
            "(SELECT DISTINCT * FROM " + regName + " WHERE id < 101).size", // 3
            "SELECT * FROM " + regName + " WHERE id = 50 and Ticker = 'ibm'", // 4
            "SELECT * FROM " + regName + " WHERE id < 50 and Ticker = 'ibm'", // 5
        };

    // Start server1
    final int port0 = vm0.invoke("Create cache server", () -> {
      setupBridgeServerAndCreateData(regionName, numberOfEntries);
      return getCacheServerPort();
    });

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParams";
    vm1.invoke("createPool",
        () -> createPool(poolName, new String[] {host0}, new int[] {port0}, true));
    vm2.invoke("createPool",
        () -> createPool(poolName, new String[] {host0}, new int[] {port0}, true));
    vm3.invoke("createPool",
        () -> createPool(poolName, new String[] {host0}, new int[] {port0}, true));

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
        for (int j = 0; j < 5; j++) {
          for (int i = 0; i < 2; i++) {
            try {
              logger.info("### Executing Query :" + queryString[i]);
              Query query = qService.newQuery(queryString[i]);
              rs[0][0] = (SelectResults) query.execute(params[i]);
              Query query2 = qService.newQuery(querys[i]);
              rs[0][1] = (SelectResults) query2.execute();
              // Compare results.
            } catch (Exception e) {
              Assert.fail("Failed executing " + queryString[i], e);
            }
            logger.info(
                "### Comparing results for Query :" + ((i + 1) * (j + 1)) + " : " + queryString[i]);
            compareQueryResultsWithoutAndWithIndexes(rs, 1);
            logger.info("### Done Comparing results for Query :" + ((i + 1) * (j + 1)) + " : "
                + queryString[i]);
          }
        }
      }
    };

    vm1.invoke("executeSameQueries", () -> executeSameQueries);
    vm2.invoke("executeSameQueries", () -> executeSameQueries);
    vm3.invoke("executeSameQueries", () -> executeSameQueries);

    // Execute client queries and validate results.
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService qService = null;
        SelectResults[][] rs = new SelectResults[1][2];

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        for (int j = 0; j < queryString.length; j++) {
          for (int i = 0; i < queryString.length; i++) {
            try {
              logger.info("### Executing Query :" + queryString[i]);
              Query query = qService.newQuery(queryString[i]);
              rs[0][0] = (SelectResults) query.execute(params[i]);
              Query query2 = qService.newQuery(querys[i]);
              rs[0][1] = (SelectResults) query2.execute();
              // Compare results.
              compareQueryResultsWithoutAndWithIndexes(rs, 1);
            } catch (Exception e) {
              Assert.fail("Failed executing " + queryString[i], e);
            }
          }
        }
      }
    };

    vm1.invoke("executeQueries", () -> executeQueries);
    vm2.invoke("executeQueries", () -> executeQueries);
    vm3.invoke("executeQueries", () -> executeQueries);

    vm1.invoke("closeClient", () -> closeClient());
    vm2.invoke("closeClient", () -> closeClient());
    vm3.invoke("closeClient", () -> closeClient());

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests remote join query execution.
   */
  @Test
  public void testRemoteJoinRegionQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke("Create cache server", () -> {
      createAndStartBridgeServer();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      createRegion(name + "1", factory.create());
      createRegion(name + "2", factory.create());
    });

    // Initialize server region
    vm0.invoke("Create cache server", () -> {
      Region region1 = getRootRegion().getSubregion(name + "1");
      for (int i = 0; i < numberOfEntries; i++) {
        region1.put("key-" + i, new TestObject(i, "ibm"));
      }
      Region region2 = getRootRegion().getSubregion(name + "2");
      for (int i = 0; i < numberOfEntries; i++) {
        region2.put("key-" + i, new TestObject(i, "ibm"));
      }
    });

    // Create client region
    final int port =
        vm0.invoke("getCacheServerPort", () -> QueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());
    final String regionName1 = "/" + rootRegionName + "/" + name + "1";
    final String regionName2 = "/" + rootRegionName + "/" + name + "2";

    // Create client pool.
    final String poolName = "testRemoteJoinRegionQueries";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries
    vm1.invoke("Execute queries", () -> {
      String queryString = null;
      SelectResults results = null;
      QueryService qService = null;

      try {
        qService = (PoolManager.find(poolName)).getQueryService();
      } catch (Exception e) {
        Assert.fail("Failed to get QueryService.", e);
      }

      queryString = "select distinct a, b.price from " + regionName1 + " a, " + regionName2
          + " b where a.price = b.price";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(numberOfEntries, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates()
          && results.getCollectionType().getElementType().isStructType());

      queryString = "select distinct a, b.price from " + regionName1 + " a, " + regionName2
          + " b where a.price = b.price and a.price = 50";
      try {
        Query query = qService.newQuery(queryString);
        results = (SelectResults) query.execute();
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString, e);
      }
      assertEquals(1, results.size());
      assertTrue(!results.getCollectionType().allowsDuplicates()
          && results.getCollectionType().getElementType().isStructType());
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  /**
   * Tests remote query execution using a BridgeClient as the CacheWriter and CacheLoader.
   */
  @Test
  public void testRemoteBridgeClientQueries() throws CacheException {

    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke("Create cache server", () -> {
      createAndStartBridgeServer();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      createRegion(name, factory.create());
    });

    // Initialize server region
    vm0.invoke("Create cache server", () -> {
      Region region = getRootRegion().getSubregion(name);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObject(i, "ibm"));
      }
    });

    final int port = vm0.invoke("getCacheServerPort", () -> getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName1 = "testRemoteBridgeClientQueries1";
    final String poolName2 = "testRemoteBridgeClientQueries2";

    vm1.invoke("createPool", () -> createPool(poolName1, host0, port));
    vm2.invoke("createPool", () -> createPool(poolName2, host0, port));

    // Execute client queries in VM1
    vm1.invoke("Execute queries", () -> {
      executeQueries(regionName, poolName1);
    });

    // Execute client queries in VM2
    vm2.invoke("Execute queries", () -> {
      executeQueries(regionName, poolName2);
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  private void executeQueries(String regionName, String poolName1) {
    String queryString;
    SelectResults results = null;
    QueryService qService = null;

    try {
      qService = (PoolManager.find(poolName1)).getQueryService();
    } catch (Exception e) {
      Assert.fail("Failed to get QueryService.", e);
    }

    queryString =
        "SELECT DISTINCT itr.value FROM " + regionName + ".entries itr where itr.key = 'key-1'";
    try {
      Query query = qService.newQuery(queryString);
      results = (SelectResults) query.execute();
    } catch (Exception e) {
      Assert.fail("Failed executing " + queryString, e);
    }
    assertEquals(1, results.size());
    assertTrue(!results.getCollectionType().allowsDuplicates()
        && !results.getCollectionType().getElementType().isStructType());
    assertTrue(results.asList().get(0) instanceof TestObject);

    queryString =
        "SELECT DISTINCT itr.key FROM " + regionName + ".entries itr where itr.key = 'key-1'";
    try {
      Query query = qService.newQuery(queryString);
      results = (SelectResults) query.execute();
    } catch (Exception e) {
      Assert.fail("Failed executing " + queryString, e);
    }
    assertEquals(1, results.size());
    assertTrue(!results.getCollectionType().allowsDuplicates()
        && !results.getCollectionType().getElementType().isStructType());
    assertEquals("key-1", results.asList().get(0));
  }

  /**
   * This the dunit test for the bug no : 36969
   *
   */

  @Test
  public void testBug36969() throws Exception {
    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    vm0.invoke("Create cache server", () -> {
      createAndStartBridgeServer();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      final Region region1 = createRegion(name, factory.createRegionAttributes());
      final Region region2 = createRegion(name + "_2", factory.createRegionAttributes());
      QueryObserverHolder.setInstance(new QueryObserverAdapter() {
        public void afterQueryEvaluation(Object result) {
          // Destroy the region in the test
          region1.close();
        }

      });
      for (int i = 0; i < numberOfEntries; i++) {
        region1.put("key-" + i, new TestObject(i, "ibm"));
        region2.put("key-" + i, new TestObject(i, "ibm"));
      }
    });

    final int port =
        vm0.invoke("getCachedServerPort", () -> QueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName1 = "/" + rootRegionName + "/" + name;
    final String regionName2 = "/" + rootRegionName + "/" + name + "_2";

    // Create client pool.
    final String poolName = "testBug36969";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries in VM1
    vm1.invoke("Execute queries", () -> {
      String queryString = "select distinct * from " + regionName1 + ", " + regionName2;
      // SelectResults results = null;
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
      } catch (Exception e) {
        // OK
      }
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
      stopBridgeServer(getCache());
    });
  }

  /**
   * Tests remote full region query execution.
   */
  @Test
  public void testRemoteSortQueriesUsingIndex() throws CacheException {
    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 100;

    // Start server
    vm0.invoke("Create cache server", () -> {
      createAndStartBridgeServer();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      createRegion(name, factory.create());
      Region region = getRootRegion().getSubregion(name);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObject(i, "ibm"));
      }
      // Create index
      try {
        QueryService qService = region.getCache().getQueryService();
        qService.createIndex("idIndex", IndexType.FUNCTIONAL, "id", region.getFullPath());
      } catch (Exception e) {
        Assert.fail("Failed to create index.", e);
      }
    });

    // Create client region
    final int port =
        vm0.invoke("getCacheServerPort", () -> QueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName = "testRemoteFullRegionQueries";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries
    vm1.invoke("Execute queries", () -> {
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
      String[] qString = {"SELECT DISTINCT * FROM " + regionName + " WHERE id < 101 ORDER BY id",
          "SELECT DISTINCT id FROM " + regionName + " WHERE id < 101 ORDER BY id",};

      for (int cnt = 0; cnt < qString.length; cnt++) {
        queryString = qString[cnt];
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults) query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries, results.size());
        // All order-by query results are stored in a ResultsCollectionWrapper
        // wrapping a list, so the assertion below is not correct even though
        // it should be.
        // assertTrue(!results.getCollectionType().allowsDuplicates());
        assertTrue(results.getCollectionType().isOrdered());
        comparator = new IdValueComparator();

        resultsArray = results.toArray();
        for (int i = 0; i < resultsArray.length; i++) {
          if (i + 1 != resultsArray.length) {
            // The id of the current element in the result set must be less
            // than the id of the next one to pass.
            if (resultsArray[i] instanceof TestObject) {
              v1 = ((TestObject) resultsArray[i]).getId();
              v2 = ((TestObject) resultsArray[i + 1]).getId();
            } else {
              v1 = (Integer) resultsArray[i];
              v2 = (Integer) resultsArray[i + 1];
            }
            assertTrue("The id for " + resultsArray[i] + " should be less than the id for "
                + resultsArray[i + 1], comparator.compare(v1, v2) == -1);
          }
        }
      }

      // order by struct query
      String[] qString2 = {
          "SELECT DISTINCT id, ticker, price FROM " + regionName + " WHERE id < 101 ORDER BY id",
          "SELECT DISTINCT ticker, id FROM " + regionName + " WHERE id < 101  ORDER BY id",
          "SELECT DISTINCT id, ticker FROM " + regionName + " WHERE id < 101  ORDER BY id asc",};

      for (int cnt = 0; cnt < qString2.length; cnt++) {
        queryString = qString2[cnt];
        try {
          Query query = qService.newQuery(queryString);
          results = (SelectResults) query.execute();
        } catch (Exception e) {
          Assert.fail("Failed executing " + queryString, e);
        }
        assertEquals(numberOfEntries, results.size());
        // All order-by query results are stored in a ResultsCollectionWrapper
        // wrapping a list, so the assertion below is not correct even though
        // it should be.
        // assertTrue(!results.getCollectionType().allowsDuplicates());
        assertTrue(results.getCollectionType().isOrdered());
        comparator = new StructIdComparator();
        resultsArray = results.toArray();
        for (int i = 0; i < resultsArray.length; i++) {
          if (i + 1 != resultsArray.length) {
            // The id of the current element in the result set must be less
            // than the id of the next one to pass.
            assertTrue(
                "The id for " + resultsArray[i] + " should be less than the id for "
                    + resultsArray[i + 1],
                comparator.compare(resultsArray[i], resultsArray[i + 1]) == -1);
          }
        }
      }
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> stopBridgeServer(getCache()));
  }

  @Test
  public void testUnSupportedOps() throws Exception {
    final String name = this.getName();
    final String rootRegionName = "root";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    vm0.invoke("Create cache server", () -> {
      createAndStartBridgeServer();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      final Region region1 = createRegion(name, factory.createRegionAttributes());
      final Region region2 = createRegion(name + "_2", factory.createRegionAttributes());
      for (int i = 0; i < numberOfEntries; i++) {
        region1.put("key-" + i, new TestObject(i, "ibm"));
        region2.put("key-" + i, new TestObject(i, "ibm"));
      }
    });

    final int port =
        vm0.invoke("getCacheServerPort", () -> QueryUsingPoolDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    final String regionName1 = "/" + rootRegionName + "/" + name;

    // Create client pool.
    final String poolName = "testUnSupportedOps";
    vm1.invoke("createPool", () -> createPool(poolName, host0, port));

    // Execute client queries in VM1
    vm1.invoke("Execute queries", () -> {
      final Region region1 = ((ClientCache) getCache())
          .createClientRegionFactory(ClientRegionShortcut.LOCAL).create(name);

      String queryString = "select distinct * from " + regionName1 + " where ticker = $1";
      Object[] params = new Object[] {new String("ibm")};
      // SelectResults results = null;
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
      } catch (UnsupportedOperationException e) {
        // Expected behavior.
      } catch (Exception e) {
        Assert.fail("Failed with UnExpected Exception.", e);
      }

      // Test with Index.
      try {
        qService.createIndex("test", IndexType.FUNCTIONAL, "ticker", regionName1);
      } catch (UnsupportedOperationException e) {
        // Expected behavior.
      } catch (Exception e) {
        Assert.fail("Failed with UnExpected Exception.", e);
      }

      try {
        String importString = "import org.apache.geode.admin.QueryUsingPoolDUnitTest.TestObject;";
        qService.createIndex("test", IndexType.FUNCTIONAL, "ticker", regionName1, importString);
      } catch (UnsupportedOperationException e) {
        // Expected behavior.
      } catch (Exception e) {
        Assert.fail("Failed with UnExpected Exception.", e);
      }

      try {
        qService.getIndex(region1, "test");
      } catch (UnsupportedOperationException e) {
        // Expected behavior.
      }

      try {
        qService.getIndexes(region1);
      } catch (UnsupportedOperationException e) {
        // Expected behavior.
      }

      try {
        qService.getIndexes(region1, IndexType.FUNCTIONAL);
      } catch (UnsupportedOperationException e) {
        // Expected behavior.
      }

      // try {
      // qService.getIndex(Index index);
      // } catch(UnsupportedOperationException e) {
      // // Expected behavior.
      // }

      try {
        qService.removeIndexes(region1);
      } catch (UnsupportedOperationException e) {
        // Expected behavior.
      }

      try {
        qService.removeIndexes();
      } catch (UnsupportedOperationException e) {
        // Expected behavior.
      }
    });

    // Stop server
    vm0.invoke("Stop CacheServer", () -> {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
      stopBridgeServer(getCache());
    });

  }

  /**
   * Creates a new instance of StructSetOrResultsSet
   */
  private void compareQueryResultsWithoutAndWithIndexes(Object[][] r, int len) {

    Set set1 = null;
    Set set2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;

    for (int j = 0; j < len; j++) {
      type1 = ((SelectResults) r[j][0]).getCollectionType().getElementType();
      type2 = ((SelectResults) r[j][1]).getCollectionType().getElementType();
      if ((type1.getClass().getName()).equals(type2.getClass().getName())) {
        logger.info("Both SelectResults are of the same Type i.e.--> "
            + ((SelectResults) r[j][0]).getCollectionType().getElementType());
      } else {
        logger
            .info("Classes are : " + type1.getClass().getName() + " " + type2.getClass().getName());
        fail("FAILED:Select result Type is different in both the cases");
      }
      if (((SelectResults) r[j][0]).size() == ((SelectResults) r[j][1]).size()) {
        logger.info(
            "Both SelectResults are of Same Size i.e.  Size= " + ((SelectResults) r[j][1]).size());
      } else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
            + ((SelectResults) r[j][0]).size() + " Size2 = " + ((SelectResults) r[j][1]).size());
      }
      set2 = (((SelectResults) r[j][1]).asSet());
      set1 = (((SelectResults) r[j][0]).asSet());

      logger.info(" SIZE1 = " + set1.size() + " SIZE2 = " + set2.size());

      // boolean pass = true;
      itert1 = set1.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        itert2 = set2.iterator();

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          logger.info("### Comparing results..");
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            logger.info("ITS a Set");
            Object[] values1 = ((Struct) p1).getFieldValues();
            Object[] values2 = ((Struct) p2).getFieldValues();
            assertEquals(values1.length, values2.length);
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual =
                  elementEqual && ((values1[i] == values2[i]) || values1[i].equals(values2[i]));
            }
            exactMatch = elementEqual;
          } else {
            logger.info("Not a Set p2:" + p2 + " p1: " + p1);
            if (p2 instanceof TestObject) {
              logger.info("An instance of TestObject");
              exactMatch = p2.equals(p1);
            } else {
              logger.info("Not an instance of TestObject" + p2.getClass().getCanonicalName());
              exactMatch = p2.equals(p1);
            }
          }
          if (exactMatch) {
            logger.info("Exact MATCH");
            break;
          }
        }
        if (!exactMatch) {
          logger.info("NOT A MATCH");
          fail(
              "Atleast one element in the pair of SelectResults supposedly identical, is not equal ");
        }
      }
      logger.info("### Done Comparing results..");
    }
  }

  protected void executeCompiledQueries(String poolName, Object[][] params) {
    QueryService qService = null;

    try {
      qService = (PoolManager.find(poolName)).getQueryService();
    } catch (Exception e) {
      Assert.fail("Failed to get QueryService.", e);
    }

    for (int i = 0; i < queryString.length; i++) {
      try {
        logger.info("### Executing Query :" + queryString[i]);
        Query query = qService.newQuery(queryString[i]);
        query.execute(params[i]);
      } catch (Exception e) {
        Assert.fail("Failed executing " + queryString[i], e);
      }
    }
  }

  /**
   * Starts a cache server on the given port, using the given deserializeValues and
   * notifyBySubscription to serve up the given region.
   */
  protected int startBridgeServer(int port, boolean notifyBySubscription) throws IOException {
    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
    return bridge.getPort();
  }

  /**
   * Stops the cache server that serves up the given cache.
   */
  protected void stopBridgeServer(Cache cache) {
    CacheServer bridge = (CacheServer) cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  /* Close Client */
  public void closeClient() {
    logger.info("### Close Client. ###");
    try {
      closeCache();
    } catch (Exception ex) {
      logger.info("### Failed to get close client. ###");
    }
    // Wait.pause(2 * 1000);
  }

  private static int getCacheServerPort() {
    return bridgeServerPort;
  }

  private static class IdComparator implements Comparator {

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

  private static class IdValueComparator implements Comparator {

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

  private static class StructIdComparator implements Comparator {

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
