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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
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
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

/**
 * These tests add a test hook to make sure query execution sleeps for at least 20 ms, so the
 * queryMonitor thread will get a chance to cancel the query before it completes.
 *
 * The MAX_QUERY_EXECUTION_TIME is set as 1 ms, i.e, theoretically all queries will be cancelled
 * after 1ms, but due to thread scheduling, this may not be true. we can only decrease the flakyness
 * of the test by making MAX_QUERY_EXECUTION_TIME the smallest possible (1) and making the query
 * execution time longer (but not too long to make the test run too slow).
 *
 */
@Category({OQLQueryTest.class})
public class QueryMonitorDUnitTest {
  private static int MAX_QUERY_EXECUTE_TIME = 1;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(5);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locator, server1, server2;
  private ClientVM client3, client4;

  @Before
  public void setUpServers() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withoutClusterConfigurationService());
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());

    // configure the server to make the query to wait for at least 1 second in every execution spot
    // and set a MAX_QUERY_EXECUTION_TIME to be 10ms
    VMProvider.invokeInEveryMember(() -> {
      DefaultQuery.testHook = QueryMonitorDUnitTest::delayQueryTestHook;
      GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = MAX_QUERY_EXECUTE_TIME;
    }, server1, server2);
    gfsh.connectAndVerify(locator);
  }

  @After
  public void reset() {
    VMProvider.invokeInEveryMember(() -> {
      DefaultQuery.testHook = null;
      GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = -1;
    }, server1, server2);
  }

  @Test
  public void testMultipleClientToOneServer() throws Exception {
    int server1Port = server1.getPort();
    client3 = cluster.startClientVM(3, true, server1Port);
    client4 = cluster.startClientVM(4, true, server1Port);

    gfsh.executeAndAssertThat("create region --name=exampleRegion --type=REPLICATE")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/exampleRegion", 2);
    server1.invoke(() -> populateRegion(0, 100));

    // execute the query
    VMProvider.invokeInEveryMember(() -> executeQuery(), client3, client4);
  }

  @Test
  public void testOneClientToMultipleServerOnReplicateRegion() throws Exception {
    int server1Port = server1.getPort();
    int server2Port = server2.getPort();
    client3 = cluster.startClientVM(3, true, server1Port, server2Port);

    gfsh.executeAndAssertThat("create region --name=exampleRegion --type=REPLICATE")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/exampleRegion", 2);
    server1.invoke(() -> populateRegion(0, 100));

    // execute the query from client3
    client3.invoke(() -> executeQuery());
  }

  @Test
  public void testOneClientToOneServerOnPartitionedRegion() throws Exception {
    // client3 connects to server1, client4 connects to server2
    int server1Port = server1.getPort();
    int server2Port = server2.getPort();
    client3 = cluster.startClientVM(3, true, server1Port);
    client4 = cluster.startClientVM(4, true, server2Port);

    gfsh.executeAndAssertThat("create region --name=exampleRegion --type=PARTITION")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/exampleRegion", 2);
    server1.invoke(() -> populateRegion(0, 100));
    server2.invoke(() -> populateRegion(100, 200));

    client3.invoke(() -> executeQuery());
    client4.invoke(() -> executeQuery());
  }

  @Test
  public void testQueryExecutionFromServerAndPerformCacheOp() throws Exception {
    gfsh.executeAndAssertThat("create region --name=exampleRegion --type=REPLICATE")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/exampleRegion", 2);
    server1.invoke(() -> populateRegion(0, 100));

    // execute the query from one server
    server1.invoke(() -> executeQuery());

    // Create index and Perform cache op. Bug#44307
    server1.invoke(() -> {
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      queryService.createIndex("idIndex", "ID", "/exampleRegion");
      queryService.createIndex("statusIndex", "status", "/exampleRegion");
      populateRegion(100, 10);
    });

    // destroy indices created in this test
    gfsh.executeAndAssertThat("destroy index --region=/exampleRegion").statusIsSuccess();
  }

  @Test
  public void testQueryExecutionFromServerOnPartitionedRegion() throws Exception {
    gfsh.executeAndAssertThat("create region --name=exampleRegion --type=PARTITION")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/exampleRegion", 2);
    server1.invoke(() -> populateRegion(100, 200));
    server2.invoke(() -> populateRegion(200, 300));

    // execute the query from one server
    server1.invoke(() -> executeQuery());
    server2.invoke(() -> executeQuery());
  }

  @Test
  public void testQueryMonitorRegionWithEviction() throws Exception {
    File server1WorkingDir = server1.getWorkingDir();
    File server2WorkingDir = server2.getWorkingDir();
    server1.invoke(() -> createReplicateRegionWithEviction(server1WorkingDir));
    server2.invoke(() -> createReplicateRegionWithEviction(server2WorkingDir));
    server1.invoke(() -> populateRegion(0, 100));
    server2.invoke(() -> populateRegion(100, 200));

    // client3 connects to server1, client4 connects to server2
    int server1Port = server1.getPort();
    int server2Port = server2.getPort();
    client3 = cluster.startClientVM(3, ccf -> {
      configureClientCacheFactory(ccf, server1Port);
    });

    client4 = cluster.startClientVM(4, ccf -> {
      configureClientCacheFactory(ccf, server2Port);
    });
    client3.invoke(() -> executeQuery());
    client4.invoke(() -> executeQuery());
  }

  @Test
  public void testQueryMonitorRegionWithIndex() throws Exception {
    gfsh.executeAndAssertThat("create region --name=exampleRegion --type=REPLICATE")
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/exampleRegion", 2);

    // create the indices using API
    VMProvider.invokeInEveryMember(() -> {
      // create index.
      QueryService cacheQS = ClusterStartupRule.getCache().getQueryService();
      cacheQS.createIndex("idIndex", "p.ID", "/exampleRegion p");
      cacheQS.createIndex("statusIndex", "p.status", "/exampleRegion p");
      cacheQS.createIndex("secIdIndex", "pos.secId", "/exampleRegion p, p.positions.values pos");
      cacheQS.createIndex("posIdIndex", "pos.Id", "/exampleRegion p, p.positions.values pos");
      cacheQS.createKeyIndex("pkIndex", "pk", "/exampleRegion");
      cacheQS.createKeyIndex("pkidIndex", "pkid", "/exampleRegion");
      populateRegion(0, 150);
    }, server1, server2);

    // client3 connects to server1, client4 connects to server2
    int server1Port = server1.getPort();
    int server2Port = server2.getPort();
    client3 = cluster.startClientVM(3, true, server1Port);
    client4 = cluster.startClientVM(4, true, server2Port);

    client3.invoke(() -> executeQuery());
    client4.invoke(() -> executeQuery());
  }

  @Test
  public void testCqExecuteDoesNotGetAffectedByTimeout() throws Exception {
    // uninstall the test hook installed in @Before
    VMProvider.invokeInEveryMember(() -> {
      DefaultQuery.testHook = null;
    }, server1, server2);

    gfsh.executeAndAssertThat("create region --name=exampleRegion --type=REPLICATE")
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/exampleRegion", 2);
    server1.invoke(() -> populateRegion(0, 100));

    int server1Port = server1.getPort();
    client3 = cluster.startClientVM(3, ccf -> {
      configureClientCacheFactory(ccf, server1Port);
    });

    client3.invoke(() -> {
      String cqName = "testCQForQueryMonitorDUnitTest";
      String query = "select * from /exampleRegion";
      // Get CQ Service.
      QueryService cqService = ClusterStartupRule.getClientCache().getQueryService();

      // Create CQ Attributes.
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};
      cqf.initCqListeners(cqListeners);
      CqAttributes cqa = cqf.create();

      CqQuery cq1 = cqService.newCq(cqName, query, cqa);
      cq1.execute();
    });

    server1.invoke(() -> {
      populateRegion(0, 150);
    });
  }

  @Test
  public void testCacheOpAfterQueryCancel() throws Exception {
    int locatorPort = locator.getPort();
    // start up more servers
    MemberVM server3 = cluster.startServerVM(3, locatorPort);

    server3.invoke(() -> {
      DefaultQuery.testHook = QueryMonitorDUnitTest::delayQueryTestHook;
      GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = MAX_QUERY_EXECUTE_TIME;
    });

    gfsh.executeAndAssertThat("create region --name=exampleRegion --type=REPLICATE")
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/exampleRegion", 3);

    server1.invoke(() -> {
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      queryService.createIndex("statusIndex", "status", "/exampleRegion");
      queryService.createIndex("secIdIndex", "pos.secId",
          "/exampleRegion p, p.positions.values pos");
      populateRegion(100, 1000);
    });

    AsyncInvocation ai1 = server1.invokeAsync(() -> {
      for (int j = 0; j < 5; j++) {
        populateRegion(0, 2000);
      }
    });

    AsyncInvocation ai2 = server2.invokeAsync(() -> {
      for (int j = 0; j < 5; j++) {
        populateRegion(1000, 3000);
      }
    });

    // server3 performs a region put after a query is canceled.
    AsyncInvocation ai3 = server3.invokeAsync(() -> {
      Region exampleRegion = ClusterStartupRule.getCache().getRegion("exampleRegion");
      QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
      String qStr =
          "SELECT DISTINCT * FROM /exampleRegion p, p.positions.values pos1, p.positions.values pos"
              + " where p.ID < pos.sharesOutstanding OR p.ID > 0 OR p.position1.mktValue > 0 "
              + " OR pos.secId in SET ('SUN', 'IBM', 'YHOO', 'GOOG', 'MSFT', "
              + " 'AOL', 'APPL', 'ORCL', 'SAP', 'DELL', 'RHAT', 'NOVL', 'HP')"
              + " order by p.status, p.ID desc";
      for (int i = 0; i < 100; i++) {
        try {
          Query query = queryService.newQuery(qStr);
          query.execute();
          fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
        } catch (QueryExecutionTimeoutException qet) {
          exampleRegion.put("" + i, new Portfolio(i));
        }
      }
    });

    ai1.await();
    ai2.await();
    ai3.await();

    server3.invoke(() -> {
      DefaultQuery.testHook = null;
      GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = -1;
    });
  }


  private static void populateRegion(int startingId, int endingId) {
    Region exampleRegion = ClusterStartupRule.getCache().getRegion("exampleRegion");
    for (int i = startingId; i < endingId; i++) {
      exampleRegion.put("" + i, new Portfolio(i));
    }
  }

  private static void executeQuery() {
    QueryService queryService;
    if (ClusterStartupRule.getClientCache() == null) {
      queryService = ClusterStartupRule.getCache().getQueryService();
    } else {
      queryService = ClusterStartupRule.getClientCache().getQueryService();
    }
    for (int k = 0; k < queryStr.length; k++) {
      String qStr = queryStr[k];
      try {
        Query query = queryService.newQuery(qStr);
        query.execute();
        fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
      } catch (Exception e) {
        verifyException(e);
      }
    }
  }

  private static void configureClientCacheFactory(ClientCacheFactory ccf, int... serverPorts) {
    for (int serverPort : serverPorts) {
      ccf.addPoolServer("localhost", serverPort);
    }
    ccf.setPoolReadTimeout(10 * 60 * 1000); // 10 min
    ccf.setPoolSubscriptionEnabled(true);
  }

  private static void createReplicateRegionWithEviction(File workingDir) {
    InternalCache cache = ClusterStartupRule.getCache();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setDiskDirs(new File[] {workingDir}).create("ds");
    EvictionAttributes evictAttrs =
        EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK);
    cache.createRegionFactory(RegionShortcut.REPLICATE)
        .setDiskStoreName("ds")
        .setEvictionAttributes(evictAttrs)
        .create("exampleRegion");
  }

  private static void verifyException(Exception e) {
    String error = e.getMessage();
    if (e.getCause() != null) {
      error = e.getCause().getMessage();
    }

    if (error.contains("Query execution canceled after exceeding max execution time")
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
   * This method is used as an implementation of the TestHook functional interface, to delay
   * query execution long enough for the QueryMonitor to mark the query as cancelled.
   */
  private static void delayQueryTestHook(final DefaultQuery.TestHook.SPOTS spot,
      final DefaultQuery query) {
    if (spot != DefaultQuery.TestHook.SPOTS.BEFORE_QUERY_DEPENDENCY_COMPUTATION) {
      return;
    }
    if (query.isCqQuery()) {
      return;
    }

    /*
     * The pollDelay() value was chosen to be larger than the
     * GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME (system property) value,
     * set during test initialization.
     */
    await("stall the query execution so that it gets cancelled")
        .pollDelay(10, TimeUnit.MILLISECONDS)
        .until(() -> query.isCanceled());
  }

  private static String[] queryStr = {"SELECT ID FROM /exampleRegion p WHERE  p.ID > 100",
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

}
