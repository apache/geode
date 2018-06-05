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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.cq.dunit.CqQueryTestListener;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * Tests for QueryMonitoring service.
 *
 * @since GemFire 6.0
 */
@Category({DistributedTest.class, OQLQueryTest.class})
public class QueryMonitorDUnitTest implements Serializable {

  private static final Logger logger = LogService.getLogger();

  @Rule
  public DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  private final String exampleRegionName = "exampleRegion";


  /* Some of the queries are commented out as they were taking less time */
  String[] queryStr = {"SELECT ID FROM /exampleRegion p WHERE  p.ID > 100",
      "SELECT DISTINCT * FROM /exampleRegion x, x.positions.values WHERE  x.pk != '1000'",
      "SELECT DISTINCT * FROM /exampleRegion x, x.positions.values WHERE  x.pkid != '1'",
      "SELECT DISTINCT * FROM /exampleRegion p, p.positions.values WHERE  p.pk > '1'",
      "SELECT DISTINCT * FROM /exampleRegion p, p.positions.values WHERE  p.pkid != '53'",
      "SELECT DISTINCT pos FROM /exampleRegion p, p.positions.values pos WHERE  pos.Id > 100",
      "SELECT DISTINCT pos FROM /exampleRegion p, p.positions.values pos WHERE  pos.Id > 100 and pos.secId IN SET('YHOO', 'IBM', 'AMZN')",
      "SELECT * FROM /exampleRegion p WHERE  p.ID > 100 and p.status = 'active' and p.ID < 100000",
      "SELECT * FROM /exampleRegion WHERE  ID > 100 and status = 'active'",
      "SELECT DISTINCT * FROM /exampleRegion p WHERE  p.ID > 100 and p.status = 'active' and p.ID < 100000",
      "SELECT DISTINCT ID FROM /exampleRegion WHERE  status = 'active'",
      "SELECT DISTINCT ID FROM /exampleRegion p WHERE  p.status = 'active'",
      "SELECT DISTINCT pos FROM /exampleRegion p, p.positions.values pos WHERE  pos.secId IN SET('YHOO', 'IBM', 'AMZN')",
      "SELECT DISTINCT proj1:p, proj2:itrX FROM /exampleRegion p, (SELECT DISTINCT pos FROM /exampleRegion p, p.positions.values pos"
          + " WHERE  pos.secId = 'YHOO') as itrX",
      "SELECT DISTINCT * FROM /exampleRegion p, (SELECT DISTINCT pos FROM /exampleRegion p, p.positions.values pos"
          + " WHERE  pos.secId = 'YHOO') as itrX",
      "SELECT DISTINCT * FROM /exampleRegion p, (SELECT DISTINCT p.ID FROM /exampleRegion x"
          + " WHERE  x.ID = p.ID) as itrX",
      "SELECT DISTINCT * FROM /exampleRegion p, (SELECT DISTINCT pos FROM /exampleRegion x, x.positions.values pos"
          + " WHERE  x.ID = p.ID) as itrX",
      "SELECT DISTINCT x.ID FROM /exampleRegion x, x.positions.values v WHERE  "
          + "v.secId = element(SELECT DISTINCT vals.secId FROM /exampleRegion p, p.positions.values vals WHERE  vals.secId = 'YHOO')",
      "SELECT DISTINCT * FROM /exampleRegion p, positions.values pos WHERE   (p.ID > 1 or p.status = 'active') or (true AND pos.secId ='IBM')",
      "SELECT DISTINCT * FROM /exampleRegion p, positions.values pos WHERE   (p.ID > 1 or p.status = 'active') or (true AND pos.secId !='IBM')",
      "SELECT DISTINCT structset.sos, structset.key "
          + "FROM /exampleRegion p, p.positions.values outerPos, "
          + "(SELECT DISTINCT key: key, sos: pos.sharesOutstanding "
          + "FROM /exampleRegion.entries pf, pf.value.positions.values pos "
          + "where outerPos.secId != 'IBM' AND "
          + "pf.key IN (SELECT DISTINCT * FROM pf.value.collectionHolderMap['0'].arr)) structset "
          + "where structset.sos > 2000",
      "SELECT DISTINCT * " + "FROM /exampleRegion p, p.positions.values outerPos, "
          + "(SELECT DISTINCT key: key, sos: pos.sharesOutstanding "
          + "FROM /exampleRegion.entries pf, pf.value.positions.values pos "
          + "where outerPos.secId != 'IBM' AND "
          + "pf.key IN (SELECT DISTINCT * FROM pf.value.collectionHolderMap['0'].arr)) structset "
          + "where structset.sos > 2000",
      "SELECT DISTINCT * FROM /exampleRegion p, p.positions.values position "
          + "WHERE (true = null OR position.secId = 'SUN') AND true",};

  String[] prQueryStr = {
      "SELECT ID FROM /exampleRegion p WHERE  p.ID > 100 and p.status = 'active'",
      "SELECT * FROM /exampleRegion WHERE  ID > 100 and status = 'active'",
      "SELECT DISTINCT * FROM /exampleRegion p WHERE   p.ID > 100 and p.status = 'active' and p.ID < 100000",
      "SELECT DISTINCT p.ID FROM /exampleRegion p WHERE p.ID > 100 and p.ID < 100000 and p.status = 'active'",
      "SELECT DISTINCT * FROM /exampleRegion p, positions.values pos WHERE (p.ID > 1 or p.status = 'active') or (pos.secId != 'IBM')",};

  private int numServers;

  @After
  public final void preTearDownCacheTestCase() throws Exception {
    Host host = Host.getHost(0);
    // shut down clients before servers
    for (int i = numServers; i < 4; i++) {
      host.getVM(i).invoke(() -> CacheTestCase.disconnectFromDS());
    }
  }

  public void setup(int numServers) throws Exception {
    Host host = Host.getHost(0);
    this.numServers = numServers;
  }

  public void createExampleRegion() {
    createExampleRegion(false, null);
  }

  private void createExampleRegion(final boolean eviction, final String dirName) {
    RegionFactory regionFactory =
        cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);

    // setting the eviction attributes.
    if (eviction) {
      File[] f = new File[1];
      f[0] = new File(dirName);
      f[0].mkdir();
      DiskStoreFactory dsf = cacheRule.getCache().createDiskStoreFactory();
      dsf.setDiskDirs(f).create("ds1");
      EvictionAttributes evictAttrs =
          EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK);
      regionFactory.setDiskStoreName("ds1").setEvictionAttributes(evictAttrs);
    }
    regionFactory.create(exampleRegionName);
  }

  private void createExamplePRRegion() {
    RegionFactory regionFactory =
        cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION);

    AttributesFactory factory = new AttributesFactory();
    // factory.setDataPolicy(DataPolicy.PARTITION);
    regionFactory
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(8).create());
    regionFactory.create(exampleRegionName);
  }

  private int configServer(final int queryMonitorTime, final String testName) throws IOException {
    cacheRule.createCache();
    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = queryMonitorTime;
    cacheRule.getCache().getLogger().fine("#### RUNNING TEST : " + testName);
    DefaultQuery.testHook = new QueryTimeoutHook(queryMonitorTime);
    return cacheServer.getPort();
  }

  // Stop server
  private void stopServer(VM server) {
    SerializableRunnable stopServer = new SerializableRunnable("Stop CacheServer") {
      public void run() {
        // Reset the test flag.
        Cache cache = cacheRule.getCache();
        DefaultQuery.testHook = null;
        GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = -1;
        stopBridgeServer(cacheRule.getCache());
      }
    };
    server.invoke(stopServer);
  }

  private void configClient(String host, int... ports) {
    configClient(false, host, ports);
  }

  private void configClient(boolean enableSubscriptions, String host, int... ports) {
    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    for (int port : ports) {
      clientCacheFactory.addPoolServer(host, port);
    }
    clientCacheFactory.setPoolSubscriptionEnabled(true);
    clientCacheFactory.setPoolReadTimeout(10 * 60 * 1000); // 10 mins
    clientCacheRule.createClientCache(clientCacheFactory);
  }

  private void verifyException(Exception e) {
    e.printStackTrace();
    String error = e.getMessage();
    if (e.getCause() != null) {
      error = e.getCause().getMessage();
    }

    if (error.contains("Query execution cancelled after exceeding max execution time")
        || error.contains("The Query completed sucessfully before it got canceled")
        || error.contains("The QueryMonitor thread may be sleeping longer than the set sleep time")
        || error.contains(
            "The query task could not be found but the query is marked as having been canceled")) {
      // Expected exception.
      return;
    }

    System.out.println("Unexpected exception:");
    if (e.getCause() != null) {
      e.getCause().printStackTrace();
    } else {
      e.printStackTrace();
    }

    fail("Expected exception Not found. Expected exception with message: \n"
        + "\"Query execution taking more than the max execution time\"" + "\n Found \n" + error);
  }

  /**
   * Tests query execution from client to server (single server).
   */
  @Test
  public void testQueryMonitorClientServer() throws Exception {

    setup(1);

    final Host host = Host.getHost(0);

    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);
    VM client3 = host.getVM(3);

    final int numberOfEntries = 100;
    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort = server.invoke("Create BridgeServer",
        () -> configServer(10, "testQueryMonitorClientServer")); // All the queries taking more than
                                                                 // 20ms should be canceled by Query
                                                                 // monitor.
    server.invoke("createExampleRegion", () -> createExampleRegion());

    // Initialize server regions.
    server.invoke("populatePortfolioRegions", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize Client1
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort));

    // Initialize Client2
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort));

    // Initialize Client3
    client3.invoke("Init client", () -> configClient(serverHostName, serverPort));

    // Execute client queries

    client1.invoke("execute Queries", () -> executeQueriesFromClient(10));
    client2.invoke("execute Queries", () -> executeQueriesFromClient(10));
    client3.invoke("execute Queries", () -> executeQueriesFromClient(10));

    stopServer(server);
  }

  private void executeQueriesFromClient(int timeout) {
    GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = timeout;
    QueryService queryService = clientCacheRule.getClientCache().getQueryService();
    executeQueriesAgainstQueryService(queryService);
  }

  private void executeQueriesOnServer() {
    QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
    executeQueriesAgainstQueryService(queryService);
  }

  private void executeQueriesAgainstQueryService(QueryService queryService) {
    for (int k = 0; k < queryStr.length; k++) {
      String qStr = queryStr[k];
      executeQuery(queryService, qStr);
    }
  }

  private void executeQuery(QueryService queryService, String qStr) {
    try {
      logger.info("Executing query :" + qStr);
      Query query = queryService.newQuery(qStr);
      query.execute();
      fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
    } catch (Exception e) {
      verifyException(e);
    }
  }

  /**
   * Tests query execution from client to server (multi server).
   */
  @Test
  public void testQueryMonitorMultiClientMultiServer() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort1 = server1.invoke("Create BridgeServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServer"));// All the queries taking
                                                                          // more than 20ms should
                                                                          // be canceled by Query
                                                                          // monitor.
    server1.invoke("createExampleRegion", () -> createExampleRegion());

    int serverPort2 = server2.invoke("Create BridgeServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServer"));// All the queries taking
                                                                          // more than 20ms should
                                                                          // be canceled by Query
                                                                          // monitor.
    server2.invoke("createExampleRegion", () -> createExampleRegion());

    // Initialize server regions.
    server1.invoke("Create Bridge Server", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize server regions.
    server2.invoke("Create Bridge Server", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize Client1 and create client regions.
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort1, serverPort2));
    // client1.invoke("createExampleRegion", () -> createExampleRegion());

    // Initialize Client2 and create client regions.
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort1, serverPort2));
    // client2.invoke("createExampleRegion", () -> createExampleRegion());

    // Execute client queries

    client1.invoke("executeQueriesFromClient", () -> executeQueriesFromClient(20));
    client2.invoke("executeQueriesFromClient", () -> executeQueriesFromClient(20));

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on local vm.
   */
  @Test
  public void testQueryExecutionLocally() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    server1.invoke("Create BridgeServer", () -> configServer(20, "testQueryExecutionLocally"));// All
                                                                                               // the
                                                                                               // queries
                                                                                               // taking
                                                                                               // more
                                                                                               // than
                                                                                               // 20ms
                                                                                               // should
                                                                                               // be
                                                                                               // canceled
                                                                                               // by
                                                                                               // Query
                                                                                               // monitor.
    server1.invoke("createExampleRegion", () -> createExampleRegion());

    server2.invoke("Create BridgeServer", () -> configServer(20, "testQueryExecutionLocally"));// All
                                                                                               // the
                                                                                               // queries
                                                                                               // taking
                                                                                               // more
                                                                                               // than
                                                                                               // 20ms
                                                                                               // should
                                                                                               // be
                                                                                               // canceled
                                                                                               // by
                                                                                               // Query
                                                                                               // monitor.
    server2.invoke("createExampleRegion", () -> createExampleRegion());

    // Initialize server regions.
    server1.invoke("Create Bridge Server", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize server regions.
    server2.invoke("Create Bridge Server", () -> populatePortfolioRegions(numberOfEntries));

    // Execute server queries

    server1.invoke("execute queries on Server", () -> executeQueriesOnServer());
    server2.invoke("execute queries on Server", () -> executeQueriesOnServer());

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on local vm.
   */
  @Test
  public void testQueryExecutionLocallyAndCacheOp() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    final int numberOfEntries = 1000;

    // Start server
    server1.invoke("Create BridgeServer", () -> configServer(20, "testQueryExecutionLocally"));// All
                                                                                               // the
                                                                                               // queries
                                                                                               // taking
                                                                                               // more
                                                                                               // than
                                                                                               // 20ms
                                                                                               // should
                                                                                               // be
                                                                                               // canceled
                                                                                               // by
                                                                                               // Query
                                                                                               // monitor.
    server1.invoke("createExampleRegion", () -> createExampleRegion());

    server2.invoke("Create BridgeServer", () -> configServer(20, "testQueryExecutionLocally"));// All
                                                                                               // the
                                                                                               // queries
                                                                                               // taking
                                                                                               // more
                                                                                               // than
                                                                                               // 20ms
                                                                                               // should
                                                                                               // be
                                                                                               // canceled
                                                                                               // by
                                                                                               // Query
                                                                                               // monitor.
    server2.invoke("createExampleRegion", () -> createExampleRegion());

    // Initialize server regions.
    server1.invoke("populatePortfolioRegions", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize server regions.
    server2.invoke("populatePortfolioRegions", () -> populatePortfolioRegions(numberOfEntries));

    // Execute server queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          String qStr =
              "SELECT DISTINCT * FROM /exampleRegion p, (SELECT DISTINCT pos FROM /exampleRegion x, x.positions.values pos"
                  + " WHERE  x.ID = p.ID) as itrX";
          executeQuery(queryService, qStr);

          // Create index and Perform cache op. Bug#44307
          queryService.createIndex("idIndex", "ID", "/exampleRegion");
          queryService.createIndex("statusIndex", "status", "/exampleRegion");
          Region exampleRegion = cacheRule.getCache().getRegion(exampleRegionName);
          for (int i = (1 + 100); i <= (numberOfEntries + 200); i++) {
            exampleRegion.put("" + i, new Portfolio(i));
          }

        } catch (Exception ex) {
          Assert.fail("Exception creating the query service", ex);
        }
      }
    };

    server1.invoke(executeQuery);
    server2.invoke(executeQuery);

    stopServer(server1);
    stopServer(server2);
  }

  private void populatePortfolioRegions(int numberOfEntries) {
    Region exampleRegion = cacheRule.getCache().getRegion(exampleRegionName);;
    for (int i = (1 + 100); i <= (numberOfEntries + 100); i++) {
      exampleRegion.put("" + i, new Portfolio(i));
    }
  }

  /**
   * Tests query execution from client to server (multiple server) on Partition Region .
   */
  @Test
  public void testQueryMonitorOnPR() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort1 = server1.invoke("configServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServerOnPR"));// All the queries
                                                                              // taking more than
                                                                              // 100ms should be
                                                                              // canceled by Query
                                                                              // monitor.
    server1.invoke("createExamplePRRegion", () -> createExamplePRRegion());

    int serverPort2 = server2.invoke("configServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServerOnPR"));// All the queries
                                                                              // taking more than
                                                                              // 100ms should be
                                                                              // canceled by Query
                                                                              // monitor.
    server2.invoke("createExamplePRRegion", () -> createExamplePRRegion());

    // Initialize server regions.
    server1.invoke("bulkInsertPorfolio", () -> bulkInsertPorfolio(101, numberOfEntries));

    // Initialize server regions.
    server2.invoke("bulkInsertPorfolio", () -> bulkInsertPorfolio((numberOfEntries + 100),
        (numberOfEntries + numberOfEntries + 100)));

    // Initialize Client1
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort1));

    // Initialize Client2
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort2));

    // Execute client queries

    client1.invoke("Execute Queries", () -> executeQueriesFromClient(20));
    client2.invoke("Execute Queries", () -> executeQueriesFromClient(20));

    stopServer(server1);
    stopServer(server2);
  }

  private void bulkInsertPorfolio(int startingId, int numberOfEntries) {
    Region exampleRegion = cacheRule.getCache().getRegion(exampleRegionName);
    for (int i = startingId; i <= (numberOfEntries + 100); i++) {
      exampleRegion.put("" + i, new Portfolio(i));
    }
  }

  /**
   * Tests query execution on Partition Region, executes query locally.
   */
  @Test
  public void testQueryMonitorWithLocalQueryOnPR() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    server1.invoke("configServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServerOnPR"));// All the queries
                                                                              // taking more than
                                                                              // 100ms should be
                                                                              // canceled by Query
                                                                              // monitor.
    server1.invoke("Create Partition Regions", () -> createExamplePRRegion());

    server2.invoke("configServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServerOnPR"));// All the queries
                                                                              // taking more than
                                                                              // 100ms should be
                                                                              // canceled by Query
                                                                              // monitor.
    server2.invoke("Create Partition Regions", () -> createExamplePRRegion());

    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        bulkInsertPorfolio(101, numberOfEntries);
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        bulkInsertPorfolio((numberOfEntries + 100), (numberOfEntries + numberOfEntries + 100));
      }
    });

    // Execute client queries
    server1.invoke("execute queries on server", () -> executeQueriesOnServer());
    server2.invoke("execute queries on server", () -> executeQueriesOnServer());

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution from client to server (multiple server) with eviction to disk.
   */
  @Test
  public void testQueryMonitorRegionWithEviction() throws CacheException {

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort1 = server1.invoke("Create BridgeServer",
        () -> configServer(20, "testQueryMonitorRegionWithEviction"));// All the queries taking more
                                                                      // than 20ms should be
                                                                      // canceled by Query monitor.
    server1.invoke("createExampleRegion",
        () -> createExampleRegion(true, "server1_testQueryMonitorRegionWithEviction"));

    int serverPort2 = server2.invoke("Create BridgeServer",
        () -> configServer(20, "testQueryMonitorRegionWithEviction"));// All the queries taking more
                                                                      // than 20ms should be
                                                                      // canceled by Query monitor.
    server2.invoke("createExampleRegion",
        () -> createExampleRegion(true, "server2_testQueryMonitorRegionWithEviction"));

    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        bulkInsertPorfolio(101, numberOfEntries);
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        bulkInsertPorfolio((numberOfEntries + 100), (numberOfEntries + numberOfEntries + 100));
      }
    });

    // Initialize Client1 and create client regions.
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort1));

    // Initialize Client2 and create client regions.
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort2));

    // Execute client queries
    client1.invoke("Execute Queries", () -> executeQueriesFromClient(20));
    client2.invoke("Execute Queries", () -> executeQueriesFromClient(20));

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on region with indexes.
   */
  @Test
  public void testQueryMonitorRegionWithIndex() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort1 =
        server1.invoke("configServer", () -> configServer(20, "testQueryMonitorRegionWithIndex"));// All
                                                                                                  // the
                                                                                                  // queries
                                                                                                  // taking
                                                                                                  // more
                                                                                                  // than
                                                                                                  // 20ms
                                                                                                  // should
                                                                                                  // be
                                                                                                  // canceled
                                                                                                  // by
                                                                                                  // Query
                                                                                                  // monitor.
    server1.invoke("createExampleRegion", () -> createExampleRegion());

    int serverPort2 =
        server2.invoke("configServer", () -> configServer(20, "testQueryMonitorRegionWithIndex"));// All
                                                                                                  // the
                                                                                                  // queries
                                                                                                  // taking
                                                                                                  // more
                                                                                                  // than
                                                                                                  // 20ms
                                                                                                  // should
                                                                                                  // be
                                                                                                  // canceled
                                                                                                  // by
                                                                                                  // Query
                                                                                                  // monitor.
    server2.invoke("createExampleRegion", () -> createExampleRegion());

    // Initialize server regions.
    server1.invoke("Create Indexes", () -> createIndexes(numberOfEntries));

    // Initialize server regions.
    server2.invoke("Create Indexes", () -> createIndexes(numberOfEntries));

    // Initialize Client1
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort1));

    // Initialize Client2
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort2));

    // Execute client queries
    client1.invoke("executeQueriesFromClient", () -> executeQueriesFromClient(20));
    client2.invoke("executeQueriesFromClient", () -> executeQueriesFromClient(20));

    stopServer(server1);
    stopServer(server2);
  }

  private void createIndexes(int numberOfEntries) throws Exception {
    Region exampleRegion = cacheRule.getCache().getRegion(exampleRegionName);

    // create index.
    QueryService cacheQS = cacheRule.getCache().getQueryService();
    cacheQS.createIndex("idIndex", "p.ID", "/exampleRegion p");
    cacheQS.createIndex("statusIndex", "p.status", "/exampleRegion p");
    cacheQS.createIndex("secIdIndex", "pos.secId", "/exampleRegion p, p.positions.values pos");
    cacheQS.createIndex("posIdIndex", "pos.Id", "/exampleRegion p, p.positions.values pos");
    cacheQS.createKeyIndex("pkIndex", "pk", "/exampleRegion");
    cacheQS.createKeyIndex("pkidIndex", "pkid", "/exampleRegion");

    for (int i = (1 + 100); i <= (numberOfEntries + 100); i++) {
      exampleRegion.put("" + i, new Portfolio(i));
    }
  }

  /**
   * The following CQ test is added to make sure testMaxQueryExecutionTime is reset and is not
   * affecting other query related tests.
   *
   */
  @Test
  public void testCqExecuteDoesNotGetAffectedByTimeout() throws Exception {
    setup(1);

    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM producerClient = host.getVM(2);

    // Start server
    int serverPort =
        server.invoke("configServer", () -> configServer(20, "testQueryMonitorRegionWithIndex"));// All
    server.invoke("createExampleRegion", () -> createExampleRegion());


    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    // Create client.
    client.invoke("createClient", () -> configClient(true, host.getHostName(), serverPort));

    final int size = 10;
    final String name = "testQuery_4";
    server.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(exampleRegionName);
      for (int i = 1; i <= size; i++) {
        region.put("key" + i, new Portfolio(i));
      }
    });

    // create and execute cq
    client.invoke(() -> {
      String cqName = "testCQForQueryMonitorDUnitTest";
      String query = "select * from /" + exampleRegionName;
      // Get CQ Service.
      QueryService cqService = null;
      cqService = clientCacheRule.getClientCache().getQueryService();

      // Create CQ Attributes.
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};
      cqf.initCqListeners(cqListeners);
      CqAttributes cqa = cqf.create();

      CqQuery cq1 = cqService.newCq(cqName, query, cqa);
      cq1.execute();
    });
  }

  @Test
  public void testCqProcessingDoesNotGetAffectedByTimeout() throws Exception {
    setup(1);

    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM producerClient = host.getVM(2);

    // Start server
    int serverPort =
        server.invoke("configServer", () -> configServer(20, "testQueryMonitorRegionWithIndex"));// All
    server.invoke("createExampleRegion", () -> createExampleRegion());


    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    // Create client.
    client.invoke("createClient", () -> configClient(true, host.getHostName(), serverPort));

    final int size = 10;
    final String name = "testQuery_4";
    server.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(exampleRegionName);
      for (int i = 1; i <= size; i++) {
        region.put("key" + i, new Portfolio(i));
      }
    });

    // create and execute cq
    client.invoke(() -> {
      String cqName = "testCQForQueryMonitorDUnitTest";
      String query = "select * from /" + exampleRegionName;
      // Get CQ Service.
      QueryService cqService = null;
      cqService = clientCacheRule.getClientCache().getQueryService();

      // Create CQ Attributes.
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};
      cqf.initCqListeners(cqListeners);
      CqAttributes cqa = cqf.create();

      CqQuery cq1 = cqService.newCq(cqName, query, cqa);
      cq1.execute();
    });

    server.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(exampleRegionName);
      for (int i = 1; i <= size; i++) {
        region.put("key" + i, new Portfolio(i));
      }
    });
  }

  /**
   * Tests cache operation right after query cancellation.
   */
  @Test
  public void testCacheOpAfterQueryCancel() throws Exception {

    setup(4);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);

    final int numberOfEntries = 1000;

    // Start server
    server1.invoke("Create BridgeServer", () -> configServer(5, "testQueryExecutionLocally"));
    server1.invoke("Create Partition Regions", () -> createExamplePRRegion());

    server2.invoke("Create BridgeServer", () -> configServer(5, "testQueryExecutionLocally"));
    server2.invoke("Create Partition Regions", () -> createExamplePRRegion());

    server3.invoke("Create BridgeServer", () -> configServer(5, "testQueryExecutionLocally"));
    server3.invoke("Create Partition Regions", () -> createExamplePRRegion());

    server4.invoke("Create BridgeServer", () -> configServer(5, "testQueryExecutionLocally"));
    server4.invoke("Create Partition Regions", () -> createExamplePRRegion());

    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        try {
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          queryService.createIndex("statusIndex", "status", "/exampleRegion");
          queryService.createIndex("secIdIndex", "pos.secId",
              "/exampleRegion p, p.positions.values pos");
        } catch (Exception ex) {
          fail("Failed to create index.");
        }
        Region exampleRegion = cacheRule.getCache().getRegion(exampleRegionName);
        for (int i = 100; i <= (numberOfEntries); i++) {
          exampleRegion.put("" + i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    AsyncInvocation ai1 =
        server1.invokeAsync(new CacheSerializableRunnable("Create Bridge Server") {
          public void run2() throws CacheException {
            Region exampleRegion = cacheRule.getCache().getRegion(exampleRegionName);
            for (int j = 0; j < 5; j++) {
              for (int i = 1; i <= (numberOfEntries + 1000); i++) {
                exampleRegion.put("" + i, new Portfolio(i));
              }
            }
            LogWriterUtils.getLogWriter()
                .info("### Completed updates in server1 in testCacheOpAfterQueryCancel");
          }
        });

    AsyncInvocation ai2 =
        server2.invokeAsync(new CacheSerializableRunnable("Create Bridge Server") {
          public void run2() throws CacheException {
            Region exampleRegion = cacheRule.getCache().getRegion(exampleRegionName);
            for (int j = 0; j < 5; j++) {
              for (int i = (1 + 1000); i <= (numberOfEntries + 2000); i++) {
                exampleRegion.put("" + i, new Portfolio(i));
              }
            }
            LogWriterUtils.getLogWriter()
                .info("### Completed updates in server2 in testCacheOpAfterQueryCancel");
          }
        });

    // Execute server queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          Region exampleRegion = cacheRule.getCache().getRegion(exampleRegionName);
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          String qStr =
              "SELECT DISTINCT * FROM /exampleRegion p, p.positions.values pos1, p.positions.values pos"
                  + " where p.ID < pos.sharesOutstanding OR p.ID > 0 OR p.position1.mktValue > 0 "
                  + " OR pos.secId in SET ('SUN', 'IBM', 'YHOO', 'GOOG', 'MSFT', "
                  + " 'AOL', 'APPL', 'ORCL', 'SAP', 'DELL', 'RHAT', 'NOVL', 'HP')"
                  + " order by p.status, p.ID desc";
          for (int i = 0; i < 500; i++) {
            try {
              GemFireCacheImpl.getInstance().getLogger().info("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
            } catch (QueryExecutionTimeoutException qet) {
              LogWriterUtils.getLogWriter()
                  .info("### Got Expected QueryExecutionTimeout exception. " + qet.getMessage());
              if (qet.getMessage().contains("cancelled after exceeding max execution")) {
                LogWriterUtils.getLogWriter().info("### Doing a put operation");
                exampleRegion.put("" + i, new Portfolio(i));
              }
            } catch (Exception e) {
              fail("Exception executing query." + e.getMessage());
            }
          }
          LogWriterUtils.getLogWriter()
              .info("### Completed Executing queries in testCacheOpAfterQueryCancel");
        } catch (Exception ex) {
          Assert.fail("Exception creating the query service", ex);
        }
      }
    };

    AsyncInvocation ai3 = server3.invokeAsync(executeQuery);
    AsyncInvocation ai4 = server4.invokeAsync(executeQuery);

    LogWriterUtils.getLogWriter()
        .info("### Waiting for async threads to join in testCacheOpAfterQueryCancel");
    try {
      ThreadUtils.join(ai1, 5 * 60 * 1000);
      ThreadUtils.join(ai2, 5 * 60 * 1000);
      ThreadUtils.join(ai3, 5 * 60 * 1000);
      ThreadUtils.join(ai4, 5 * 60 * 1000);
    } catch (Exception ex) {
      fail("Async thread join failure");
    }
    LogWriterUtils.getLogWriter()
        .info("### DONE Waiting for async threads to join in testCacheOpAfterQueryCancel");

    validateQueryMonitorThreadCnt(server1, 0, 1000);
    validateQueryMonitorThreadCnt(server2, 0, 1000);
    validateQueryMonitorThreadCnt(server3, 0, 1000);
    validateQueryMonitorThreadCnt(server4, 0, 1000);

    LogWriterUtils.getLogWriter()
        .info("### DONE validating query monitor threads testCacheOpAfterQueryCancel");

    stopServer(server1);
    stopServer(server2);
    stopServer(server3);
    stopServer(server4);
  }

  private void validateQueryMonitorThreadCnt(VM vm, final int threadCount, final int waitTime) {
    SerializableRunnable validateThreadCnt =
        new CacheSerializableRunnable("validateQueryMonitorThreadCnt") {
          public void run2() throws CacheException {
            Cache cache = cacheRule.getCache();
            QueryMonitor qm = ((GemFireCacheImpl) cache).getQueryMonitor();
            if (qm == null) {
              fail("Didn't found query monitor.");
            }
            int waited = 0;
            while (true) {
              if (qm.getQueryMonitorThreadCount() != threadCount) {
                if (waited <= waitTime) {
                  Wait.pause(10);
                  waited += 10;
                  continue;
                } else {
                  fail("Didn't found expected monitoring thread. Expected: " + threadCount
                      + " found :" + qm.getQueryMonitorThreadCount());
                }
              }
              break;
            }
          }
        };
    vm.invoke(validateThreadCnt);
  }

  /**
   * Starts a bridge server on the given port, using the given deserializeValues and
   * notifyBySubscription to serve up the given region.
   */
  protected int startBridgeServer(int port, boolean notifyBySubscription) throws IOException {

    Cache cache = cacheRule.getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    return bridge.getPort();
  }

  /**
   * Stops the bridge server that serves up the given cache.
   */
  private void stopBridgeServer(Cache cache) {
    CacheServer bridge = (CacheServer) cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  private class QueryTimeoutHook implements DefaultQuery.TestHook {

    long timeout;

    private QueryTimeoutHook(long timeout) {
      this.timeout = timeout;
    }

    public void doTestHook(String description) {
      if (description.equals("6")) {
        try {
          Thread.sleep(timeout * 2);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }

    public void doTestHook(int spot) {
      doTestHook("" + spot);
    }

  }

}
