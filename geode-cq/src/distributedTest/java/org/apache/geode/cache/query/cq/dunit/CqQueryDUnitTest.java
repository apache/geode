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
package org.apache.geode.cache.query.cq.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqAttributesMutator;
import org.apache.geode.cache.query.CqClosedException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CertifiableTestCacheListener;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.DistributedTombstoneOperation;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This class tests the ContinuousQuery mechanism in GemFire. It does so by creating a cache server
 * with a cache and a pre-defined region and a data loader. The client creates the same region and
 * attaches the connection pool.
 */
@Category({ClientSubscriptionTest.class})
@SuppressWarnings({"serial", "Convert2MethodRef"})
public class CqQueryDUnitTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();
  /**
   * The port on which the cache server was started in this VM
   */
  private static int bridgeServerPort;

  private static final int port = 0;
  protected static int port2 = 0;

  public static final int noTest = -1;

  public final String[] regions = new String[] {"regionA", "regionB"};

  private static final int CREATE = 0;
  private static final int UPDATE = 1;
  private static final int DESTROY = 2;
  private static final int INVALIDATE = 3;
  private static final int CLOSE = 4;
  private static final int REGION_CLEAR = 5;
  private static final int REGION_INVALIDATE = 6;

  public static final String KEY = "key-";

  private static final String WAIT_PROPERTY = "CqQueryTest.maxWaitTime";

  private static final int WAIT_DEFAULT = (20 * 1000);

  private static final long MAX_TIME = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT);

  public final String[] cqs = new String[] {
      // 0 - Test for ">"
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID > 0",

      // 1 - Test for "=" and "and".
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID = 2 and p.status='active'",

      // 2 - Test for "<" and "and".
      "SELECT ALL * FROM /root/" + regions[1] + " p where p.ID < 5 and p.status='active'",

      // FOLLOWING CQS ARE NOT TESTED WITH VALUES; THEY ARE USED TO TEST PARSING LOGIC WITHIN CQ.
      // 3
      "SELECT * FROM /root/" + regions[0] + " ;",
      // 4
      "SELECT ALL * FROM /root/" + regions[0],
      // 5
      "import org.apache.geode.cache.\"query\".data.Portfolio; " + "SELECT ALL * FROM /root/"
          + regions[0] + " TYPE Portfolio",
      // 6
      "import org.apache.geode.cache.\"query\".data.Portfolio; " + "SELECT ALL * FROM /root/"
          + regions[0] + " p TYPE Portfolio",
      // 7
      "SELECT ALL * FROM /root/" + regions[1] + " p where p.ID < 5 and p.status='active';",
      // 8
      "SELECT ALL * FROM /root/" + regions[0] + "  ;",
      // 9
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.description = NULL",
      // 10
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID > 0 and p.status='active'",
      // 11 test for region 0
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID > 0",
      // 12 test for region 1
      "SELECT ALL * FROM /root/" + regions[1] + " p where p.ID > 0",};

  public final String[] prCqs = new String[] {
      // 0 - Test for ">"
      "SELECT ALL * FROM /" + regions[0] + " p where p.ID > 0",

      // 1 - Test for "=" and "and".
      "SELECT ALL * FROM /" + regions[0] + " p where p.ID = 2 and p.status='active'"};

  private final String[] invalidCQs = new String[] {
      // Test for ">"
      "SELECT ALL * FROM /root/invalidRegion p where p.ID > 0"};


  private final String[] shortTypeCQs = new String[] {
      // 11 - Test for "short" number type
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.shortID IN SET(1,2,3,4,5)"};

  @Override
  public final void postSetUp() throws Exception {
    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating connection pools
    getSystem();
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    postSetUpCqQueryDUnitTest();
  }

  protected void postSetUpCqQueryDUnitTest() throws Exception {}

  /* Returns Cache Server Port */
  public static int getCacheServerPort() {
    return bridgeServerPort;
  }

  /* Create Cache Server */
  public void createServer(VM server) {
    createServer(server, 0);
  }

  public void createServer(VM server, final int p) {
    createServer(server, p, false);
  }

  public void createServer(VM server, final int thePort, final boolean eviction) {
    createServer(server, thePort, eviction, DataPolicy.REPLICATE);
  }

  public void createServer(VM server, final int thePort, final boolean eviction,
      final DataPolicy dataPolicy) {
    server.invoke(() -> {
      logger.info("### Create Cache Server. ###");
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(dataPolicy);

      // setting the eviction attributes.
      if (eviction) {
        EvictionAttributes evictAttrs =
            EvictionAttributes.createLRUEntryAttributes(100000, EvictionAction.OVERFLOW_TO_DISK);
        factory.setEvictionAttributes(evictAttrs);
      }

      for (String region : regions) {
        createRegion(region, factory.createRegionAttributes());
        if (getRootRegion("root").getSubregion(region).isEmpty()) {
          logger.info("### CreateServer: Region is empty ###");
        }
      }

      startBridgeServer(thePort, true);
    });
  }

  public void createServerOnly(VM server, final int thePort) {
    server.invoke(() -> {
      logger.info("### Create Cache Server. ###");
      startBridgeServer(thePort, true);
    });
  }

  public void createPartitionRegion(final VM server, final String[] regionNames) {
    SerializableRunnable createRegion = new CacheSerializableRunnable("Create Region") {

      public void run2() throws CacheException {
        RegionFactory rf = getCache().createRegionFactory(RegionShortcut.PARTITION);
        for (String regionName : regionNames) {
          rf.create(regionName);
        }
      }
    };

    server.invoke(createRegion);
  }

  private void createReplicateRegionWithLocalDestroy(final VM server, final String[] regionNames) {
    SerializableRunnable createRegion = new CacheSerializableRunnable("Create Region") {

      public void run2() throws CacheException {
        RegionFactory rf =
            getCache().createRegionFactory(RegionShortcut.REPLICATE).setEvictionAttributes(
                EvictionAttributes.createLIFOEntryAttributes(10, EvictionAction.LOCAL_DESTROY));
        for (String regionName : regionNames) {
          rf.create(regionName);
        }
      }
    };

    server.invoke(createRegion);
  }


  /* Close Cache Server */
  public void closeServer(VM server) {
    server.invoke(new SerializableRunnable("Close CacheServer") {
      public void run() {
        logger.info("### Close CacheServer. ###");
        stopBridgeServer(getCache());
      }
    });

  }

  private void crashServer(VM server) {
    server.invoke(new SerializableRunnable("Crash CacheServer") {
      public void run() {
        org.apache.geode.cache.client.internal.ConnectionImpl.setTEST_DURABLE_CLIENT_CRASH(true);
        logger.info("### Crashing CacheServer. ###");
        stopBridgeServer(getCache());
      }
    });
    Wait.pause(2 * 1000);
  }

  private void closeCrashServer(VM server) {
    server.invoke(new SerializableRunnable("Close CacheServer") {
      public void run() {
        org.apache.geode.cache.client.internal.ConnectionImpl.setTEST_DURABLE_CLIENT_CRASH(false);
        logger.info("### Crashing CacheServer. ###");
        stopBridgeServer(getCache());
      }
    });
    Wait.pause(2 * 1000);
  }

  /* Create Client */
  public void createClient(VM client, final int serverPort, final String serverHost) {
    int[] serverPorts = new int[] {serverPort};
    createClient(client, serverPorts, serverHost, null);
  }

  /* Create Client */
  public void createClient(VM client, final int[] serverPorts, final String serverHost,
      final String redundancyLevel) {
    client.invoke(() -> {
      logger.info("### Create Client. ###");

      // Initialize CQ Service.
      getCache().getQueryService();

      AttributesFactory regionFactory = new AttributesFactory();
      regionFactory.setScope(Scope.LOCAL);
      if (redundancyLevel != null) {
        ClientServerTestCase.configureConnectionPool(regionFactory, serverHost, serverPorts, true,
            Integer.parseInt(redundancyLevel), -1, null);
      } else {
        ClientServerTestCase.configureConnectionPool(regionFactory, serverHost, serverPorts, true,
            -1, -1, null);
      }
      for (String region : regions) {
        createRegion(region, regionFactory.createRegionAttributes());
        logger.info("### Successfully Created Region on Client :" + region);
      }
    });
  }

  /* Create Local Region */
  public void createLocalRegion(VM client, final int[] serverPorts, final String serverHost,
      final String redundancyLevel, final String[] regionNames) {
    SerializableRunnable createQService = new CacheSerializableRunnable("Create Local Region") {
      public void run2() throws CacheException {
        logger.info("### Create Local Region. ###");
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.LOCAL);

        if (redundancyLevel != null) {
          ClientServerTestCase.configureConnectionPool(af, serverHost, serverPorts, true,
              Integer.parseInt(redundancyLevel), -1, null);
        } else {
          ClientServerTestCase.configureConnectionPool(af, serverHost, serverPorts, true, -1, -1,
              null);
        }

        RegionFactory rf = getCache().createRegionFactory(af.create());
        for (int i = 0; i < regionNames.length; i++) {
          rf.create(regionNames[i]);
          logger.info("### Successfully Created Region on Client :" + regions[i]);
        }
      }
    };

    client.invoke(createQService);
  }

  public void createClientWith2Pools(VM client, final int[] serverPorts1, final int[] serverPorts2,
      final String serverHost, final String redundancyLevel) {
    client.invoke(() -> {
      logger.info("### Create Client. ###");

      // Initialize CQ Service.
      getCache().getQueryService();

      AttributesFactory regionFactory0 = new AttributesFactory();
      AttributesFactory regionFactory1 = new AttributesFactory();
      regionFactory0.setScope(Scope.LOCAL);
      regionFactory1.setScope(Scope.LOCAL);
      if (redundancyLevel != null) {
        ClientServerTestCase.configureConnectionPoolWithName(regionFactory0, serverHost,
            serverPorts1, true,
            Integer.parseInt(redundancyLevel), -1,
            null, "testPoolA");
        ClientServerTestCase.configureConnectionPoolWithName(regionFactory1, serverHost,
            serverPorts2, true,
            Integer.parseInt(redundancyLevel), -1,
            null, "testPoolB");
      } else {
        ClientServerTestCase.configureConnectionPoolWithName(regionFactory0, serverHost,
            serverPorts1, true, -1, -1, null,
            "testPoolA");
        ClientServerTestCase.configureConnectionPoolWithName(regionFactory1, serverHost,
            serverPorts2, true, -1, -1, null,
            "testPoolB");
      }
      createRegion(regions[0], regionFactory0.createRegionAttributes());
      createRegion(regions[1], regionFactory1.createRegionAttributes());
      logger.info("### Successfully Created Region on Client :" + regions[0]);
      logger.info("### Successfully Created Region on Client :" + regions[1]);

    });
  }


  /* Close Client */
  public void closeClient(VM client) {
    SerializableRunnable closeCQService = new CacheSerializableRunnable("Close Client") {
      public void run2() throws CacheException {
        logger.info("### Close Client. ###");
        try {
          ((DefaultQueryService) getCache().getQueryService()).closeCqService();
        } catch (Exception ex) {
          logger.info("### Failed to get CqService during ClientClose() ###");
        }

      }
    };

    client.invoke(closeCQService);
    Wait.pause(2 * 1000);
  }


  /* Create/Init values */
  public void createValues(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          region1.put(KEY + i, new Portfolio(i));
        }
        logger.info("### Number of Entries in Region :" + region1.keySet().size());
      }
    });
  }

  /* Create/Init values */
  public void createValuesWithTime(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          Portfolio portfolio = new Portfolio(i);
          portfolio.createTime = System.currentTimeMillis();
          region1.put(KEY + i, portfolio);
        }
        logger.info("### Number of Entries in Region :" + region1.keySet().size());
      }
    });
  }

  private void createValuesWithShort(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          Portfolio portfolio = new Portfolio(i);
          portfolio.shortID = new Short("" + i);
          region1.put(KEY + i, portfolio);
        }
        logger.info("### Number of Entries in Region :" + region1.keySet().size());
      }
    });
  }

  private void createValuesAsPrimitives(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          switch (i % 5) {
            case 0:
              region1.put("key" + i, "seeded");
              break;
            case 1:
              region1.put("key" + i, "seeding");
              break;
            case 2:
              region1.put("key" + i, (double) i);
              break;
            case 3:
              region1.put("key" + i, i);
              break;
            case 4:
              region1.put("key" + i, new Portfolio(i));
              break;
            default:
              region1.put("key" + i, i);
              break;

          }
        }
        logger.info("### Number of Entries in Region :" + region1.keySet().size());
      }
    });
  }

  public void updateValuesAsPrimitives(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          switch (i % 5) {
            case 0:
              region1.put("key" + i, "seeding");
              break;
            case 1:
              region1.put("key" + i, "seeded");
              break;
            case 2:
              region1.put("key" + i, i);
              break;
            case 3:
              region1.put("key" + i, new Portfolio(i));
              break;
            case 4:
              region1.put("key" + i, (double) i);
              break;
            default:
              region1.put("key" + i, i);
              break;

          }
        }
        logger.info("### Number of Entries in Region :" + region1.keySet().size());
      }
    });
  }

  public void createValuesAsPortfolios(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          region1.put("key" + i, new Portfolio(i));
        }
        logger.info("### Number of Entries in Region :" + region1.keySet().size());
      }
    });
  }

  private void createIndex(VM vm, final String indexName, final String indexedExpression,
      final String regionPath) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        try {
          QueryService qs = getCache().getQueryService();
          qs.createIndex(indexName, indexedExpression, regionPath);
        } catch (Exception e) {
          throw new CacheException(e) {};
        }
      }
    });
  }


  /* delete values */
  public void deleteValues(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Delete values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          region1.destroy(KEY + i);
        }
        logger.info("### Number of Entries In Region after Delete :" + region1.keySet().size());
      }

    });
  }

  /**
   * support for invalidating values.
   */
  public void invalidateValues(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          region1.invalidate(KEY + i);
        }
        logger.info("### Number of Entries In Region after Delete :" + region1.keySet().size());
      }

    });
  }

  /* Register CQs */
  public void createCQ(VM vm, final String cqName, final String queryStr) {
    createCQ(vm, cqName, queryStr, false);
  }

  public void createCQ(VM vm, final String cqName, final String queryStr,
      final boolean isBridgeMemberTest) {
    vm.invoke(() -> {

      logger.info("### Create CQ. ###" + cqName);
      // Get CQ Service.
      QueryService cqService = getCache().getQueryService();

      // Create CQ Attributes.
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};

      cqf.initCqListeners(cqListeners);
      CqAttributes cqa = cqf.create();

      // Create CQ.
      CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa);
      assertThat(cq1.getState().isStopped()).describedAs("newCq() state mismatch").isTrue();

    });
  }

  /* Register CQs with no name, execute, and close */
  public void createAndExecCQNoName(VM vm, final String queryStr) {
    vm.invoke(() -> {
      logger.info("### DEBUG CREATE CQ START ####");
      logger.info("### Create CQ with no name. ###");
      String cqName = null;
      QueryService cqService = getCache().getQueryService();

      SelectResults cqResults;
      for (int i = 0; i < 20; ++i) {
        // Create CQ Attributes.
        CqAttributesFactory cqf = new CqAttributesFactory();
        CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};

        cqf.initCqListeners(cqListeners);
        CqAttributes cqa = cqf.create();

        // Create CQ with no name and execute with initial results.

        CqQuery cq1 = cqService.newCq(queryStr, cqa);
        ((CqQueryTestListener) cqListeners[0]).cqName = cq1.getName();

        if (cq1 == null) {
          logger.info("Failed to get CqQuery object for CQ with no name.");
        } else {
          cqName = cq1.getName();
          logger.info("Created CQ with no name, generated CQ name: "
              + cqName + " CQ state:" + cq1.getState());
          assertThat(cq1.getState().isStopped()).describedAs("Create CQ with no name illegal state")
              .isTrue();
        }
        if (i % 2 == 0) {
          cqResults = cq1.executeWithInitialResults();

          logger.info("initial result size = " + cqResults.size());
          logger.info("CQ state after execute with initial results = " + cq1.getState());
          assertThat(cq1.getState().isRunning())
              .describedAs("executeWithInitialResults() state mismatch").isTrue();
        } else {
          try {
            cq1.execute();
          } catch (Exception ex) {
            logger.info("CQService is :" + cqService);
            ex.printStackTrace();
            fail("Failed to execute CQ " + cqName + " . " + ex.getMessage());
          }
          logger.info("CQ state after execute = " + cq1.getState());
          assertThat(cq1.getState().isRunning()).describedAs("execute() state mismatch").isTrue();
        }

        // Close the CQ
        try {
          cq1.close();
        } catch (Exception ex) {
          logger.info("CqService is :" + cqService, ex);
          fail("Failed to close CQ " + cqName + " . " + ex.getMessage());
        }
        assertThat(cq1.getState().isClosed()).describedAs("closeCq() state mismatch").isTrue();
      }
    });
  }

  public void executeCQ(VM vm, final String cqName, final boolean initialResults,
      String expectedErr) {
    executeCQ(vm, cqName, initialResults, noTest, expectedErr);
  }

  /**
   * Execute/register CQ as running.
   *
   * @param initialResults true if initialResults are requested
   * @param expectedResultsSize if >= 0, validate results against this size
   * @param expectedErr if not null, an error we expect
   */
  private void executeCQ(VM vm, final String cqName, final boolean initialResults,
      final int expectedResultsSize, final String expectedErr) {
    vm.invoke(() -> {
      if (expectedErr != null) {
        getCache().getLogger()
            .info("<ExpectedException action=add>" + expectedErr + "</ExpectedException>");
      }
      try {
        QueryService cqService = getCache().getQueryService();
        CqQuery cq1 = cqService.getCq(cqName);
        assertThat(cq1).isNotNull();
        assertThat(cq1.getState().isStopped()).describedAs("newCq() state mismatch").isTrue();

        if (initialResults) {
          SelectResults cqResults;
          cqResults = cq1.executeWithInitialResults();
          assertThat(cq1.getState().isRunning())
              .describedAs("executeWithInitialResults() state mismatch").isTrue();
          if (expectedResultsSize >= 0) {
            assertThat(cqResults.size()).describedAs("unexpected results size")
                .isEqualTo(expectedResultsSize);
          }
        } else {
          cq1.execute();
          assertThat(cq1.getState().isRunning()).describedAs("execute() state mismatch").isTrue();
        }
      } finally {
        if (expectedErr != null) {
          getCache().getLogger()
              .info("<ExpectedException action=remove>" + expectedErr + "</ExpectedException>");
        }
      }
    });
  }

  /* Stop/pause CQ */
  public void stopCQ(VM vm, final String cqName) {
    vm.invoke(() -> {
      logger.info("### Stop CQ. ###" + cqName);
      QueryService cqService = getCache().getQueryService();

      CqQuery cq1 = cqService.getCq(cqName);
      cq1.stop();

      assertThat(cq1.getState().isStopped()).describedAs("Stop CQ state mismatch").isTrue();
    });
  }

  // Stop and execute CQ repeatedly
  /* Stop/pause CQ */
  private void stopExecCQ(VM vm) {
    vm.invoke(() -> {
      logger.info("### Stop and Exec CQ. ###" + "testCQStopExecute_0");
      QueryService cqService = getCache().getQueryService();
      CqQuery cq1 = cqService.getCq("testCQStopExecute_0");

      for (int i = 0; i < 20; ++i) {

        cq1.stop();

        assertThat(cq1.getState().isStopped()).describedAs("Stop CQ state mismatch, count = " + i)
            .isTrue();
        logger.info("After stop in Stop and Execute loop, ran successfully, loop count: " + i);
        logger.info("CQ state: " + cq1.getState());

        cq1.execute();

        assertThat(cq1.getState().isRunning())
            .describedAs("Execute CQ state mismatch, count = " + i).isTrue();
        logger.info("After execute in Stop and Execute loop, ran successfully, loop count: " + i);
        logger.info("CQ state: " + cq1.getState());
      }
    });
  }


  /* UnRegister CQs */
  public void closeCQ(VM vm, final String cqName) {
    vm.invoke(() -> {
      // Get CQ Service.
      QueryService cqService = getCache().getQueryService();

      // Close CQ.
      CqQuery cq1 = cqService.getCq(cqName);
      cq1.close();

      assertThat(cq1.getState().isClosed()).describedAs("Close CQ state mismatch").isTrue();
    });
  }

  /* Register CQs */
  private void registerInterestListCQ(VM vm, final String regionName) {
    vm.invoke(() -> {
      Region region = getRootRegion().getSubregion(regionName);
      region.getAttributesMutator()
          .addCacheListener(new CertifiableTestCacheListener(LogWriterUtils.getLogWriter()));

      List list = new ArrayList();
      for (int i = 1; i <= 10; i++) {
        list.add(KEY + i);
      }
      region.registerInterest(list);
    });
  }

  // helps test case where executeIR is called multiple times as well as after close
  private void executeAndCloseAndExecuteIRMultipleTimes(VM vm,
      final String queryStr) {
    vm.invoke(() -> {
      QueryService cqService = getCache().getQueryService();
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};

      cqf.initCqListeners(cqListeners);
      CqAttributes cqa = cqf.create();

      CqQuery cq1 = cqService.newCq("testCQResultSet_0", queryStr, cqa);
      assertThat(cq1.getState().isStopped()).describedAs("newCq() state mismatch").isTrue();

      cq1.executeWithInitialResults();
      try {
        cq1.executeWithInitialResults();
      } catch (IllegalStateException e) {
        // expected
      }
      cq1.close();

      try {
        cq1.executeWithInitialResults();
      } catch (CqClosedException e) {
        // expected
        return;
      }
      fail("should have received cqClosedException");
    });
  }


  /* Validate CQ Count */
  public void validateCQCount(VM vm, final int cqCnt) {
    vm.invoke(() -> {
      // Get CQ Service.
      QueryService cqService = getCache().getQueryService();

      int numCqs = cqService.getCqs().length;
      assertThat(numCqs).describedAs("Number of cqs mismatch.").isEqualTo(cqCnt);
    });
  }


  /**
   * Throws AssertionError if the CQ can be found or if any other error occurs
   */
  private void failIfCQExists(VM vm, final String cqName) {
    vm.invoke(() -> {
      // Get CQ Service.
      QueryService cqService = getCache().getQueryService();

      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Unexpectedly found CqQuery for CQ : " + cqName).isNull();
    });
  }

  private void validateCQError(VM vm, final String cqName, final int numError) {
    vm.invoke(() -> {
      QueryService cqService = getCache().getQueryService();
      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Failed to get CqQuery for CQ : " + cqName).isNotNull();

      CqAttributes cqAttr = cQuery.getCqAttributes();
      CqListener cqListener = cqAttr.getCqListener();
      CqQueryTestListener listener = (CqQueryTestListener) cqListener;
      listener.printInfo(false);

      // Check for totalEvents count.
      if (numError != noTest) {
        // Result size validation.
        listener.printInfo(true);
        assertThat(listener.getErrorEventCount()).describedAs("Total Event Count mismatch")
            .isEqualTo(numError);
      }
    });
  }

  public void validateCQ(VM vm, final String cqName, final int resultSize, final int creates,
      final int updates, final int deletes) {
    validateCQ(vm, cqName, resultSize, creates, updates, deletes, noTest, noTest, noTest, noTest);
  }

  public void validateCQ(VM vm, final String cqName, final int resultSize, final int creates,
      final int updates, final int deletes, final int queryInserts, final int queryUpdates,
      final int queryDeletes, final int totalEvents) {
    vm.invoke(() -> {
      QueryService cqService = getCache().getQueryService();
      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Failed to get CqQuery for CQ : " + cqName).isNotNull();

      CqAttributes cqAttr = cQuery.getCqAttributes();
      CqListener cqListeners[] = cqAttr.getCqListeners();
      CqQueryTestListener listener = (CqQueryTestListener) cqListeners[0];
      listener.printInfo(false);

      // Check for totalEvents count.
      if (totalEvents != noTest) {
        // Result size validation.
        listener.printInfo(true);
        assertThat(listener.getTotalEventCount()).describedAs("Total Event Count mismatch")
            .isEqualTo(totalEvents);
      }

      assertThat(resultSize).describedAs("test for event counts instead of results size")
          .isEqualTo(noTest);

      // Check for create count.
      if (creates != noTest) {
        // Result size validation.
        listener.printInfo(true);
        assertThat(listener.getCreateEventCount()).describedAs("Create Event mismatch")
            .isEqualTo(creates);
      }

      // Check for update count.
      if (updates != noTest) {
        // Result size validation.
        listener.printInfo(true);
        assertThat(listener.getUpdateEventCount()).describedAs("Update Event mismatch")
            .isEqualTo(updates);
      }

      // Check for delete count.
      if (deletes != noTest) {
        // Result size validation.
        listener.printInfo(true);
        assertThat(listener.getDeleteEventCount()).describedAs("Delete Event mismatch")
            .isEqualTo(deletes);
      }

      // Check for queryInsert count.
      if (queryInserts != noTest) {
        // Result size validation.
        listener.printInfo(true);
        assertThat(listener.getQueryInsertEventCount()).describedAs("Query Insert Event mismatch")
            .isEqualTo(queryInserts);
      }

      // Check for queryUpdate count.
      if (queryUpdates != noTest) {
        // Result size validation.
        listener.printInfo(true);
        assertThat(listener.getQueryUpdateEventCount()).describedAs("Query Update Event mismatch")
            .isEqualTo(queryUpdates);
      }

      // Check for queryDelete count.
      if (queryDeletes != noTest) {
        // Result size validation.
        listener.printInfo(true);
        assertThat(listener.getQueryDeleteEventCount()).describedAs("Query Delete Event mismatch")
            .isEqualTo(queryDeletes);
      }
    });
  }

  public void waitForCreated(VM vm, final String cqName, final String key) {
    waitForEvent(vm, 0, cqName, key);
  }

  public void waitForUpdated(VM vm, final String cqName, final String key) {
    waitForEvent(vm, 1, cqName, key);
  }

  public void waitForDestroyed(VM vm, final String cqName, final String key) {
    waitForEvent(vm, 2, cqName, key);
  }

  public void waitForInvalidated(VM vm, final String cqName, final String key) {
    waitForEvent(vm, 3, cqName, key);
  }

  public void waitForClose(VM vm, final String cqName) {
    waitForEvent(vm, 4, cqName, null);
  }

  public void waitForRegionClear(VM vm, final String cqName) {
    waitForEvent(vm, 5, cqName, null);
  }

  public void waitForRegionInvalidate(VM vm, final String cqName) {
    waitForEvent(vm, 6, cqName, null);
  }

  private void waitForError(VM vm, final String cqName, final String errorMessage) {
    vm.invoke(() -> {
      // Get CQ Service.
      QueryService cqService = getCache().getQueryService();

      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Failed to get CqQuery for CQ : " + cqName).isNotNull();

      CqAttributes cqAttr = cQuery.getCqAttributes();
      CqListener[] cqListener = cqAttr.getCqListeners();
      CqQueryTestListener listener = (CqQueryTestListener) cqListener[0];
      listener.waitForError(errorMessage);
    });
  }

  protected void waitForCqsDisconnected(VM vm, final String cqName, final int count) {
    vm.invoke(() -> {
      // Get CQ Service.
      QueryService cqService = getCache().getQueryService();

      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Failed to get CqQuery for CQ : " + cqName).isNotNull();

      CqAttributes cqAttr = cQuery.getCqAttributes();
      CqListener[] cqListener = cqAttr.getCqListeners();
      CqQueryTestListener listener = (CqQueryTestListener) cqListener[0];
      listener.waitForCqsDisconnectedEvents(count);
    });
  }

  protected void waitForCqsConnected(VM vm, final String cqName, final int count) {
    vm.invoke(() -> {
      // Get CQ Service.
      QueryService cqService = getCache().getQueryService();

      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Failed to get CqQuery for CQ : " + cqName).isNotNull();

      CqAttributes cqAttr = cQuery.getCqAttributes();
      CqListener[] cqListener = cqAttr.getCqListeners();
      CqQueryTestListener listener = (CqQueryTestListener) cqListener[0];
      listener.waitForCqsConnectedEvents(count);
    });
  }

  private void waitForEvent(VM vm, final int event, final String cqName, final String key) {
    vm.invoke(() -> {
      QueryService cqService = getCache().getQueryService();

      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Failed to get CqQuery for CQ : " + cqName).isNotNull();

      CqAttributes cqAttr = cQuery.getCqAttributes();
      CqListener[] cqListener = cqAttr.getCqListeners();
      CqQueryTestListener listener = (CqQueryTestListener) cqListener[0];

      switch (event) {
        case CREATE:
          listener.waitForCreated(key);
          break;

        case UPDATE:
          listener.waitForUpdated(key);
          break;

        case DESTROY:
          listener.waitForDestroyed(key);
          break;

        case INVALIDATE:
          listener.waitForInvalidated(key);
          break;

        case CLOSE:
          listener.waitForClose();
          break;

        case REGION_CLEAR:
          listener.waitForRegionClear();
          break;

        case REGION_INVALIDATE:
          listener.waitForRegionInvalidate();
          break;

      }
    });
  }

  /**
   * Waits till the CQ state is same as the expected. Waits for max time, if the CQ state is not
   * same as expected throws exception.
   */
  public void waitForCqState(VM vm, final String cqName, final int state) {
    vm.invoke(() -> {
      QueryService cqService = getCache().getQueryService();

      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Failed to get CqQuery for CQ : " + cqName).isNotNull();

      final CqStateImpl cqState = (CqStateImpl) cQuery.getState();
      // Wait max time, till the CQ state is as expected.
      await("cqState never became " + state)
          .until(() -> cqState.getState(), Matchers.equalTo(state));
    });
  }

  public void clearCQListenerEvents(VM vm, final String cqName) {
    vm.invoke(() -> {
      QueryService cqService = getCache().getQueryService();

      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Failed to get CqQuery for CQ : " + cqName).isNotNull();

      CqAttributes cqAttr = cQuery.getCqAttributes();
      CqListener cqListener = cqAttr.getCqListener();
      CqQueryTestListener listener = (CqQueryTestListener) cqListener;
      listener.getEventHistory();
    });
  }

  private void validateQuery(VM vm, final String query, final int resultSize) {
    vm.invoke(() -> {
      QueryService cqService = getCache().getQueryService();

      Query q = cqService.newQuery(query);

      Object r = q.execute();
      if (r instanceof Collection) {
        int rSize = ((Collection) r).size();
        logger.info("### Result Size is :" + rSize);
        assertThat(rSize).isEqualTo(rSize);
      }
    });
  }

  private Properties getConnectionProps(String[] hosts, int[] ports, Properties newProps) {

    Properties props = new Properties();
    StringBuilder endPoints = new StringBuilder();
    String host = hosts[0];
    for (int i = 0; i < ports.length; i++) {
      if (hosts.length > 1) {
        host = hosts[i];
      }
      endPoints.append("server").append(i).append("=").append(host).append(":").append(ports[i]);
      if (ports.length > (i + 1)) {
        endPoints.append(",");
      }
    }

    props.setProperty("endpoints", endPoints.toString());
    props.setProperty("retryAttempts", "1");

    // Add other property elements.
    if (newProps != null) {
      Enumeration e = newProps.keys();
      while (e.hasMoreElements()) {
        String key = (String) e.nextElement();
        props.setProperty(key, newProps.getProperty(key));
      }
    }
    return props;
  }


  // Exercise CQ attributes mutator functions
  private void mutateCQAttributes(VM vm, final String cqName, final int mutator_function) {
    vm.invoke(() -> {
      CqQuery cq1;
      QueryService cqService = getCache().getQueryService();

      CqQuery cQuery = cqService.getCq(cqName);
      assertThat(cQuery).describedAs("Failed to get CqQuery for CQ : " + cqName).isNotNull();

      cq1 = cqService.getCq(cqName);

      CqAttributesMutator cqAttrMutator = cq1.getCqAttributesMutator();
      CqAttributes cqAttr = cq1.getCqAttributes();
      CqListener cqListeners[];
      switch (mutator_function) {
        case CREATE:
          // Reinitialize with 2 CQ Listeners
          CqListener cqListenersArray[] = {new CqQueryTestListener(getCache().getLogger()),
              new CqQueryTestListener(getCache().getLogger())};
          cqAttrMutator.initCqListeners(cqListenersArray);
          cqListeners = cqAttr.getCqListeners();
          assertThat(2).describedAs("CqListener count mismatch").isEqualTo(cqListeners.length);
          break;

        case UPDATE:
          // Add 2 new CQ Listeners
          CqListener newListener1 = new CqQueryTestListener(getCache().getLogger());
          CqListener newListener2 = new CqQueryTestListener(getCache().getLogger());
          cqAttrMutator.addCqListener(newListener1);
          cqAttrMutator.addCqListener(newListener2);

          cqListeners = cqAttr.getCqListeners();
          assertThat(3).describedAs("CqListener count mismatch").isEqualTo(cqListeners.length);
          break;

        case DESTROY:
          cqListeners = cqAttr.getCqListeners();
          cqAttrMutator.removeCqListener(cqListeners[0]);
          cqListeners = cqAttr.getCqListeners();
          assertThat(2).describedAs("CqListener count mismatch").isEqualTo(cqListeners.length);

          // Remove a listener and validate
          cqAttrMutator.removeCqListener(cqListeners[0]);
          cqListeners = cqAttr.getCqListeners();
          assertThat(1).describedAs("CqListener count mismatch").isEqualTo(cqListeners.length);
          break;
      }
    });
  }


  private void performGC(VM server, final String regionName) {
    SerializableRunnable task = new CacheSerializableRunnable("perform GC") {
      public void run2() throws CacheException {
        Region subregion = getCache().getRegion("root/" + regionName);
        DistributedTombstoneOperation gc = DistributedTombstoneOperation
            .gc((DistributedRegion) subregion, new EventID(getCache().getDistributedSystem()));
        gc.distribute();
      }
    };
    server.invoke(task);
  }

  private void ensureCQExists(VM server, final String regionName) {
    SerializableRunnable task = new CacheSerializableRunnable("check CQs") {
      public void run2() throws CacheException {
        CqQuery queries[] = getCache().getQueryService().getCqs();
        assertThat(queries.length > 0).describedAs("expected to find a CQ but found none").isTrue();
        System.out.println("found query " + queries[0]);
        assertThat(queries[0].getName().startsWith(
            "testCQResultSet_0")).describedAs("Couldn't find query " + "testCQResultSet_0")
                .isTrue();
        assertThat(!queries[0].isClosed()).describedAs("expected the CQ to be open: " + queries[0])
            .isTrue();
      }
    };
    server.invoke(task);
  }


  /**
   * bug #47494 - CQs were destroyed when a server did a tombstone GC
   */
  @Test
  public void testCQRemainsWhenServerGCs() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);
    VM server2 = VM.getVM(2);

    createServer(server);
    createServer(server2);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);
    try {
      /* CQ Test with initial Values. */
      int size = 5;
      createValuesWithShort(server, regions[0], size);
      Wait.pause(500);

      final String cqName = "testCQResultSet_0";

      // Create CQs.
      createCQ(client, cqName, shortTypeCQs[0]);

      // Check resultSet Size.
      executeCQ(client, cqName, true, 5, null);

      // Simulate a tombstone GC on the server
      performGC(server, regions[0]);

      // Check the CQs
      ensureCQExists(server, regions[0]);
    } finally {
      closeClient(client);
      closeServer(server);
      closeServer(server2);
    }
  }


  /**
   * Test for InterestList and CQ registered from same clients.
   */
  @Test
  public void testInterestListAndCQs() {
    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();
    createClient(client, thePort, host0);


    /* Create CQs. */
    createCQ(client, "testInterestListAndCQs_0", cqs[0]);
    validateCQCount(client, 1);

    /* Init values at server. */
    final int size = 10;

    executeCQ(client, "testInterestListAndCQs_0", false, null);
    registerInterestListCQ(client, regions[0]);

    createValues(server, regions[0], size);
    // Wait for client to Sync.

    for (int i = 1; i <= 10; i++) {
      waitForCreated(client, "testInterestListAndCQs_0", KEY + i);
    }

    // validate CQs.
    validateCQ(client, "testInterestListAndCQs_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ noTest, /* deletes; */ noTest, /* queryInserts: */ size,
        /* queryUpdates: */ 0, /* queryDeletes: */ 0, /* totalEvents: */ size);

    // Validate InterestList.
    // CREATE
    client.invoke(new CacheSerializableRunnable("validate updates") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regions[0]);
        assertThat(region).isNotNull();

        Set keys = region.entrySet();
        assertThat(keys.size())
            .describedAs(
                "Mismatch, number of keys in local region is not equal to the interest list size")
            .isEqualTo(size);

        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForCreated(KEY + i);
          assertThat(region.getEntry(KEY + i)).isNotNull();
        }
      }
    });

    // UPDATE
    createValues(server, regions[0], size);
    // Wait for client to sync.
    for (int i = 1; i <= 10; i++) {
      waitForUpdated(client, "testInterestListAndCQs_0", KEY + i);
    }

    client.invoke(new CacheSerializableRunnable("validate updates") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regions[0]);
        assertThat(region).isNotNull();

        Set keys = region.entrySet();
        assertThat(keys.size())
            .describedAs(
                "Mismatch, number of keys in local region is not equal to the interest list size")
            .isEqualTo(size);

        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForUpdated(KEY + i);
          assertThat(region.getEntry(KEY + i)).isNotNull();
        }
      }
    });

    // INVALIDATE
    server.invoke(new CacheSerializableRunnable("Invalidate values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = 1; i <= size; i++) {
          region1.invalidate(KEY + i);
        }
      }
    });

    waitForInvalidated(client, "testInterestListAndCQs_0", KEY + 10);

    client.invoke(new CacheSerializableRunnable("validate invalidates") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regions[0]);
        assertThat(region).isNotNull();

        Set keys = region.entrySet();
        assertThat(keys.size())
            .describedAs(
                "Mismatch, number of keys in local region is not equal to the interest list size")
            .isEqualTo(size);

        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForInvalidated(KEY + i);
          assertThat(region.getEntry(KEY + i)).isNotNull();
        }
      }
    });

    validateCQ(client, "testInterestListAndCQs_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ size, /* deletes; */ noTest, /* queryInserts: */ size,
        /* queryUpdates: */ size, /* queryDeletes: */ size, /* totalEvents: */ size * 3);

    // DESTROY - this should not have any effect on CQ, as the events are
    // already destroyed from invalidate events.
    server.invoke(new CacheSerializableRunnable("Invalidate values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = 1; i <= size; i++) {
          region1.destroy(KEY + i);
        }
      }
    });

    // Wait for destroyed.
    client.invoke(new CacheSerializableRunnable("validate destroys") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regions[0]);
        assertThat(region).isNotNull();

        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForDestroyed(KEY + i);
        }
      }
    });

    validateCQ(client, "testInterestListAndCQs_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ size, /* deletes; */ noTest, /* queryInserts: */ size,
        /* queryUpdates: */ size, /* queryDeletes: */ size, /* totalEvents: */ size * 3);

    closeClient(client);
    closeServer(server);
  }


  /**
   * Test for CQ register and UnRegister.
   */
  @Test
  public void testCQStopExecute() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();
    createClient(client, thePort, host0);

    /* Create CQs. */
    createCQ(client, "testCQStopExecute_0", cqs[0]);
    validateCQCount(client, 1);

    executeCQ(client, "testCQStopExecute_0", false, null);

    /* Init values at server. */
    int size = 10;
    createValues(server, regions[0], size);
    // Wait for client to sync.

    waitForCreated(client, "testCQStopExecute_0", KEY + size);

    // Check if Client and Server in sync.
    // validateServerClientRegionEntries(server, client, regions[0]);
    validateQuery(server, cqs[0], 10);
    // validate CQs.
    // validateCQ(client, "testCQStopExecute_0", size, noTest, noTest, noTest);
    validateCQ(client, "testCQStopExecute_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ 0, /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ 0,
        /* queryDeletes: */ 0, /* totalEvents: */ size);

    // Test CQ stop
    stopCQ(client, "testCQStopExecute_0");

    // Test CQ re-enable
    executeCQ(client, "testCQStopExecute_0", false, null);

    /* Init values at server. */
    createValues(server, regions[0], 20);
    // Wait for client to sync.
    waitForCreated(client, "testCQStopExecute_0", KEY + 20);
    size = 30;

    // Check if Client and Server in sync.
    // validateServerClientRegionEntries(server, client, regions[0]);
    validateQuery(server, cqs[0], 20);
    // validate CQs.
    // validateCQ(client, "testCQStopExecute_0", size, noTest, noTest, noTest);
    validateCQ(client, "testCQStopExecute_0", /* resultSize: */ noTest, /* creates: */ 20,
        /* updates: */ 10, /* deletes; */ 0, /* queryInserts: */ 20, /* queryUpdates: */ 10,
        /* queryDeletes: */ 0, /* totalEvents: */ size);

    // Stop and execute CQ 20 times
    stopExecCQ(client);

    // Test CQ Close
    closeCQ(client, "testCQStopExecute_0");

    // Close.
    closeClient(client);
    closeServer(server);
  }

  /**
   * Test for CQ Attributes Mutator functions
   */
  @Test
  public void testCQAttributesMutator() {
    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();
    createClient(client, thePort, host0);

    /* Create CQs. */
    String cqName = "testCQAttributesMutator_0";
    createCQ(client, cqName, cqs[0]);
    validateCQCount(client, 1);
    executeCQ(client, cqName, false, null);

    /* Init values at server. */
    int size = 10;
    createValues(server, regions[0], size);
    // Wait for client to sync.
    waitForCreated(client, cqName, KEY + size);

    // validate CQs.
    validateCQ(client, cqName, /* resultSize: */ noTest, /* creates: */ size, /* updates: */ 0,
        /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ 0, /* queryDeletes: */ 0,
        /* totalEvents: */ size);

    // Add 2 new CQ Listeners
    mutateCQAttributes(client, cqName, UPDATE);

    /* Init values at server. */
    createValues(server, regions[0], size * 2);
    waitForCreated(client, cqName, KEY + (size * 2));

    validateCQ(client, cqName, /* resultSize: */ noTest, /* creates: */ 20, /* updates: */ 10,
        /* deletes; */ 0, /* queryInserts: */ 20, /* queryUpdates: */ 10, /* queryDeletes: */ 0,
        /* totalEvents: */ 30);

    // Remove 2 listeners and validate
    mutateCQAttributes(client, cqName, DESTROY);

    validateCQ(client, cqName, /* resultSize: */ noTest, /* creates: */ 10, /* updates: */ 10,
        /* deletes; */ 0, /* queryInserts: */ 10, /* queryUpdates: */ 10, /* queryDeletes: */ 0,
        /* totalEvents: */ 20);

    // Reinitialize with 2 CQ Listeners
    mutateCQAttributes(client, cqName, CREATE);

    /* Delete values at server. */
    deleteValues(server, regions[0], 20);
    // Wait for client to sync.
    waitForDestroyed(client, cqName, KEY + (size * 2));

    validateCQ(client, cqName, /* resultSize: */ noTest, /* creates: */ 0, /* updates: */ 0,
        /* deletes; */ 20, /* queryInserts: */ 0, /* queryUpdates: */ 0, /* queryDeletes: */ 20,
        /* totalEvents: */ 20);

    // Close CQ
    closeCQ(client, cqName);

    // Close.
    closeClient(client);
    closeServer(server);
  }

  /**
   * Test for CQ register and UnRegister.
   */
  @Test
  public void testCQCreateClose() {
    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();
    createClient(client, thePort, host0);

    /* Create CQs. */
    createCQ(client, "testCQCreateClose_0", cqs[0]);
    validateCQCount(client, 1);

    executeCQ(client, "testCQCreateClose_0", false, null);

    /* Init values at server. */
    int size = 10;
    createValues(server, regions[0], size);
    // Wait for client to sync.
    waitForCreated(client, "testCQCreateClose_0", KEY + size);

    // Check if Client and Server in sync.
    validateQuery(server, cqs[0], 10);
    // validate CQs.
    validateCQ(client, "testCQCreateClose_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ 0, /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ 0,
        /* queryDeletes: */ 0, /* totalEvents: */ size);

    // Test CQ stop
    stopCQ(client, "testCQCreateClose_0");

    // Test CQ re-enable
    executeCQ(client, "testCQCreateClose_0", false, null);

    // Test CQ Close
    closeCQ(client, "testCQCreateClose_0");

    // Create CQs with no name, execute, and close.
    createAndExecCQNoName(client, cqs[0]);

    // Accessing the closed CQ.
    failIfCQExists(client, "testCQCreateClose_0");

    // re-Create the cq which is closed.
    createCQ(client, "testCQCreateClose_0", cqs[0]);

    /* Test CQ Count */
    validateCQCount(client, 1);

    // Registering CQ with same name from same client.
    try {
      createCQ(client, "testCQCreateClose_0", cqs[0]);
      fail("Trying to create CQ with same name. Should have thrown CQExistsException");
    } catch (org.apache.geode.test.dunit.RMIException rmiExc) {

      Throwable cause = rmiExc.getCause(); // should be a CQExistsException
      assertThat(cause).describedAs("Got wrong exception: " + cause.getClass().getName())
          .isInstanceOf(CqExistsException.class);
    }

    // Getting values from non-existent CQ.
    failIfCQExists(client, "testCQCreateClose_NO");

    // Server Registering CQ.
    try {
      createCQ(server, "testCQCreateClose_1", cqs[0]);
      fail("Trying to create CQ on Cache Server. Should have thrown Exception.");
    } catch (org.apache.geode.test.dunit.RMIException rmiExc) {

      Throwable cause = rmiExc.getCause(); // should be a IllegalStateException
      assertThat(cause).describedAs("Got wrong exception: " + cause.getClass().getName())
          .isInstanceOf(IllegalStateException.class);
    }

    // Trying to execute CQ on non-existing region.
    createCQ(client, "testCQCreateClose_2", invalidCQs[0]);
    try {
      executeCQ(client, "testCQCreateClose_2", false, "RegionNotFoundException");
      fail("Trying to create CQ on non-existing Region. Should have thrown Exception.");
    } catch (org.apache.geode.test.dunit.RMIException rmiExc) {

      Throwable cause = rmiExc.getCause(); // should be a RegionNotFoundException
      assertThat(cause).describedAs("Expected cause to be RegionNotFoundException")
          .isInstanceOf(RegionNotFoundException.class);
    }

    /* Test CQ Count - Above failed create should not increment the CQ cnt. */
    validateCQCount(client, 2);

    createCQ(client, "testCQCreateClose_3", cqs[2]);

    validateCQCount(client, 3);

    /* Test for closeAllCQs() */

    client.invoke(() -> {
      logger.info("### Close All CQ. ###");
      QueryService cqService = getCache().getQueryService();
      cqService.closeCqs();
    });

    validateCQCount(client, 0);

    // Initialize.
    createCQ(client, "testCQCreateClose_2", cqs[1]);
    createCQ(client, "testCQCreateClose_4", cqs[1]);
    createCQ(client, "testCQCreateClose_5", cqs[1]);

    // Execute few of the initialized cqs
    executeCQ(client, "testCQCreateClose_4", false, null);
    executeCQ(client, "testCQCreateClose_5", false, null);

    // Call close all CQ.
    client.invoke(() -> {
      logger.info("### Close All CQ 2. ###");
      // Get CQ Service.
      QueryService cqService = getCache().getQueryService();
      cqService.closeCqs();
    });

    // Close.
    closeClient(client);
    closeServer(server);
  }

  /**
   * This will test the events after region destroy. The CQs on the destroy region needs to be
   * closed.
   */
  @Test
  public void testRegionDestroy() {
    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();
    createClient(client, thePort, host0);


    /* Create CQs. */
    createCQ(client, "testRegionDestroy_0", cqs[0]);
    createCQ(client, "testRegionDestroy_1", cqs[0]);
    createCQ(client, "testRegionDestroy_2", cqs[0]);

    executeCQ(client, "testRegionDestroy_0", false, null);
    executeCQ(client, "testRegionDestroy_1", false, null);
    executeCQ(client, "testRegionDestroy_2", false, null);

    /* Init values at server. */
    final int size = 10;
    registerInterestListCQ(client, regions[0]);
    createValues(server, regions[0], size);

    // Wait for client to sync.

    waitForCreated(client, "testRegionDestroy_0", KEY + 10);

    // validate CQs.
    validateCQ(client, "testRegionDestroy_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ noTest, /* deletes; */ noTest, /* queryInserts: */ size,
        /* queryUpdates: */ 0, /* queryDeletes: */ 0, /* totalEvents: */ size);

    // Validate InterestList.
    // CREATE
    client.invoke(new CacheSerializableRunnable("validate updates") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regions[0]);
        assertThat(region).isNotNull();

        Set keys = region.entrySet();
        assertThat(keys.size())
            .describedAs(
                "Mismatch, number of keys in local region is not equal to the interest list size")
            .isEqualTo(size);

        CertifiableTestCacheListener ctl =
            (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForCreated(KEY + i);
          assertThat(region.getEntry(KEY + i)).isNotNull();
        }
      }
    });

    // Destroy Region.
    server.invoke(new CacheSerializableRunnable("Destroy Region") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        region1.destroyRegion();
      }
    });

    Wait.pause(4 * 1000);
    validateCQCount(client, 0);

    closeClient(client);
    closeServer(server);

  }

  /**
   * Test for CQ with multiple clients.
   */
  @Test
  public void testCQWithMultipleClients() {

    VM server = VM.getVM(0);
    VM client1 = VM.getVM(1);
    VM client2 = VM.getVM(2);
    VM client3 = VM.getVM(3);

    /* Create Server and Client */
    createServer(server);
    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();
    createClient(client1, thePort, host0);
    createClient(client2, thePort, host0);

    /* Create CQs. and initialize the region */
    createCQ(client1, "testCQWithMultipleClients_0", cqs[0]);
    executeCQ(client1, "testCQWithMultipleClients_0", false, null);
    createCQ(client2, "testCQWithMultipleClients_0", cqs[0]);
    executeCQ(client2, "testCQWithMultipleClients_0", false, null);

    int size = 10;

    // Create Values on Server.
    createValues(server, regions[0], size);

    waitForCreated(client1, "testCQWithMultipleClients_0", KEY + 10);


    /* Validate the CQs */
    validateCQ(client1, "testCQWithMultipleClients_0", /* resultSize: */ noTest,
        /* creates: */ size, /* updates: */ 0, /* deletes; */ 0, /* queryInserts: */ size,
        /* queryUpdates: */ 0, /* queryDeletes: */ 0, /* totalEvents: */ size);

    waitForCreated(client2, "testCQWithMultipleClients_0", KEY + 10);

    validateCQ(client2, "testCQWithMultipleClients_0", /* resultSize: */ noTest,
        /* creates: */ size, /* updates: */ 0, /* deletes; */ 0, /* queryInserts: */ size,
        /* queryUpdates: */ 0, /* queryDeletes: */ 0, /* totalEvents: */ size);


    /* Close test */
    closeCQ(client1, "testCQWithMultipleClients_0");

    validateCQ(client2, "testCQWithMultipleClients_0", /* resultSize: */ noTest,
        /* creates: */ size, /* updates: */ 0, /* deletes; */ 0, /* queryInserts: */ size,
        /* queryUpdates: */ 0, /* queryDeletes: */ 0, /* totalEvents: */ size);

    /* Init new client and create cq */
    createClient(client3, thePort, host0);
    createCQ(client3, "testCQWithMultipleClients_0", cqs[0]);
    createCQ(client3, "testCQWithMultipleClients_1", cqs[1]);
    executeCQ(client3, "testCQWithMultipleClients_0", false, null);
    executeCQ(client3, "testCQWithMultipleClients_1", false, null);

    // Update values on Server. This will be updated on new Client CQs.
    createValues(server, regions[0], size);

    waitForUpdated(client3, "testCQWithMultipleClients_0", KEY + 10);

    validateCQ(client3, "testCQWithMultipleClients_0", /* resultSize: */ noTest, /* creates: */ 0,
        /* updates: */ size, /* deletes; */ 0, /* queryInserts: */ 0, /* queryUpdates: */ size,
        /* queryDeletes: */ 0, /* totalEvents: */ size);

    validateCQ(client3, "testCQWithMultipleClients_1", /* resultSize: */ noTest, /* creates: */ 0,
        /* updates: */ 1, /* deletes; */ 0, /* queryInserts: */ 0, /* queryUpdates: */ 1,
        /* queryDeletes: */ 0, /* totalEvents: */ 1);

    /* Validate the CQ count */
    validateCQCount(client1, 0);
    validateCQCount(client2, 1);
    validateCQCount(client3, 2);

    /* Close Client Test */
    closeClient(client1);

    clearCQListenerEvents(client2, "testCQWithMultipleClients_0");
    clearCQListenerEvents(client3, "testCQWithMultipleClients_1");

    // Update values on server, update again.
    createValues(server, regions[0], size);

    waitForUpdated(client2, "testCQWithMultipleClients_0", KEY + 10);

    validateCQ(client2, "testCQWithMultipleClients_0", /* resultSize: */ noTest,
        /* creates: */ size, /* updates: */ size * 2, /* deletes; */ 0, /* queryInserts: */ size,
        /* queryUpdates: */ size * 2, /* queryDeletes: */ 0, /* totalEvents: */ size * 3);

    waitForUpdated(client3, "testCQWithMultipleClients_1", KEY + 2);

    validateCQ(client3, "testCQWithMultipleClients_1", /* resultSize: */ noTest, /* creates: */ 0,
        /* updates: */ 2, /* deletes; */ 0, /* queryInserts: */ 0, /* queryUpdates: */ 2,
        /* queryDeletes: */ 0, /* totalEvents: */ 2);

    /* Close Server and Client */
    closeClient(client2);
    closeClient(client3);
    closeServer(server);
  }

  /**
   * Test for CQ ResultSet.
   */
  @Test
  public void testCQResultSet() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    /* CQ Test with initial Values. */
    int size = 10;
    createValues(server, regions[0], size);
    Wait.pause(500);

    // Create CQs.
    createCQ(client, "testCQResultSet_0", cqs[0]);

    // Check resultSet Size.
    executeCQ(client, "testCQResultSet_0", true, 10, null);

    /* CQ Test with no Values on Region */
    createCQ(client, "testCQResultSet_1", cqs[2]);
    // Check resultSet Size.
    executeCQ(client, "testCQResultSet_1", true, 0, null);
    stopCQ(client, "testCQResultSet_1");

    // Init values.
    createValues(server, regions[1], 5);
    validateQuery(server, cqs[2], 2);

    executeCQ(client, "testCQResultSet_1", true, 2, null);

    /*
     * compare values... Disabled since we don't currently maintain results on the client
     *
     * validateCQ(client, "testCQResultSet_1", 2, noTest, noTest, noTest); Portfolio[] values = new
     * Portfolio[] {new Portfolio(2), new Portfolio(4)}; HashTable t = new HashTable(); String[]
     * keys = new String[] {"key-2", "key-4"}; t.put(keys[0], values[0]); t.put(keys[1], values[1]);
     *
     * compareValues(client, "testCQResultSet_1", t);
     *
     * deleteValues(server, regions[1], 3); t.remove("key-4"); pause(2 * 1000);
     *
     * try { compareValues(client, "testCQResultSet_1", t);
     * fail("Should have thrown Exception. The value should not be present in cq results region"); }
     * catch (Exception ex) { // @todo check for specific exception type }
     *
     */

    // Close.
    closeClient(client);
    closeServer(server);
  }

  /**
   * Test for CQ Listener events.
   */
  @Test
  public void testCQEvents() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    // Create CQs.
    createCQ(client, "testCQEvents_0", cqs[0]);

    executeCQ(client, "testCQEvents_0", false, null);

    // Init values at server.
    int size = 10;
    createValues(server, regions[0], size);

    waitForCreated(client, "testCQEvents_0", KEY + size);

    // validate Create events.
    validateCQ(client, "testCQEvents_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ 0, /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ 0,
        /* queryDeletes: */ 0, /* totalEvents: */ size);

    // Update values.
    createValues(server, regions[0], 5);
    createValues(server, regions[0], 10);

    waitForUpdated(client, "testCQEvents_0", KEY + size);

    // validate Update events.
    validateCQ(client, "testCQEvents_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ 15, /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ 15,
        /* queryDeletes: */ 0, /* totalEvents: */ size + 15);

    // Validate delete events.
    deleteValues(server, regions[0], 5);
    waitForDestroyed(client, "testCQEvents_0", KEY + 5);

    validateCQ(client, "testCQEvents_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ 15, /* deletes; */5, /* queryInserts: */ size, /* queryUpdates: */ 15,
        /* queryDeletes: */ 5, /* totalEvents: */ size + 15 + 5);

    // Insert invalid Events.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = -1; i >= -5; i--) {

          region1.put(KEY + i, KEY + i);
        }
      }
    });

    Wait.pause(1000);
    // cqs should not get any creates, deletes or updates.
    validateCQ(client, "testCQEvents_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ 15, /* deletes; */5, /* queryInserts: */ size, /* queryUpdates: */ 15,
        /* queryDeletes: */ 5, /* totalEvents: */ size + 15 + 5);

    // Close.
    closeClient(client);
    closeServer(server);
  }

  @Test
  public void testCQMapValues() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);
    String mapQuery = "select * from /root/" + regions[0] + " er where er['field1'] > 'value2'";
    int size = 10;

    createServer(server);

    createMapValues(server, regions[0], size / 2);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    // Create CQs.
    createCQ(client, "testCQEvents_0", mapQuery);

    // execute with initial results
    executeCQ(client, "testCQEvents_0", true, 3, null);

    // updates and creates
    createMapValues(server, regions[0], size);

    // wait for last event which is key-9 as the query
    // > 'value2' returns 'value3' to 'value9'
    waitForCreated(client, "testCQEvents_0", KEY + (size - 1));

    // validate events.
    validateCQ(client, "testCQEvents_0", /* resultSize: */ noTest, /* creates: */ 4,
        /* updates: */ 3, /* deletes; */ 0, /* queryInserts: */ 4, /* queryUpdates: */ 3,
        /* queryDeletes: */ 0, /* totalEvents: */ 7);

    closeCQ(client, "testCQEvents_0");

    createIndex(server, "index1", "er[*]", "/root/" + regions[0] + " er");

    // Create CQs.
    createCQ(client, "testCQEvents_0", mapQuery);

    // execute with initial results
    executeCQ(client, "testCQEvents_0", true, 7, null);

    closeCQ(client, "testCQEvents_0");

    createIndex(server, "index2", "er['field1']", "/root/" + regions[0] + " er");

    // Create CQs.
    createCQ(client, "testCQEvents_0", mapQuery);

    // execute with initial results
    executeCQ(client, "testCQEvents_0", true, 7, null);

    // Close.
    closeClient(client);
    closeServer(server);
  }


  private void createMapValues(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create map values") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          Map<String, String> value = new HashMap<>();
          value.put("field1", "value" + i);
          value.put("field2", "key" + i);
          exampleRegion.put(KEY + i, value);
        }
        logger.info("### Number of Entries in Region :" + exampleRegion.keySet().size());
      }
    });
  }


  /**
   * Test for stopping and restarting CQs.
   */
  @Test
  public void testEnableDisableCQ() {
    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    // Create CQs.
    createCQ(client, "testEnableDisable_0", cqs[0]);
    executeCQ(client, "testEnableDisable_0", false, null);

    /* Test for disableCQ */
    client.invoke(new CacheSerializableRunnable("Client disableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService;
        try {
          cqService = getCache().getQueryService();
          cqService.stopCqs();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }
      }
    });

    Wait.pause(1000);
    // Init values at server.
    int size = 10;
    createValues(server, regions[0], size);
    Wait.pause(500);
    // There should not be any creates.
    validateCQ(client, "testEnableDisable_0", /* resultSize: */ noTest, /* creates: */ 0,
        /* updates: */ 0, /* deletes; */ 0, /* queryInserts: */ 0, /* queryUpdates: */ 0,
        /* queryDeletes: */ 0, /* totalEvents: */ 0);

    /* Test for enable CQ */
    client.invoke(new CacheSerializableRunnable("Client enableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService;
        try {
          cqService = getCache().getQueryService();
          cqService.executeCqs();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }
      }
    });
    Wait.pause(1000);
    createValues(server, regions[0], size);
    waitForUpdated(client, "testEnableDisable_0", KEY + size);
    // It gets created on the CQs
    validateCQ(client, "testEnableDisable_0", /* resultSize: */ noTest, /* creates: */ 0,
        /* updates: */ size, /* deletes; */ 0, /* queryInserts: */ 0, /* queryUpdates: */ size,
        /* queryDeletes: */ 0, /* totalEvents: */ size);

    /* Test for disableCQ on Region */
    client.invoke(new CacheSerializableRunnable("Client disableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService;
        try {
          cqService = getCache().getQueryService();
          cqService.stopCqs("/root/" + regions[0]);
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }
      }
    });

    Wait.pause(2 * 1000);
    deleteValues(server, regions[0], size / 2);
    Wait.pause(500);
    // There should not be any deletes.
    validateCQ(client, "testEnableDisable_0", /* resultSize: */ noTest, /* creates: */ 0,
        /* updates: */ size, /* deletes; */ 0, /* queryInserts: */ 0, /* queryUpdates: */ size,
        /* queryDeletes: */ 0, /* totalEvents: */ size);

    /* Test for enable CQ on region */
    client.invoke(new CacheSerializableRunnable("Client enableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService;
        try {
          cqService = getCache().getQueryService();
          cqService.executeCqs("/root/" + regions[0]);
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }
      }
    });
    Wait.pause(1000);
    createValues(server, regions[0], size / 2);
    waitForCreated(client, "testEnableDisable_0", KEY + (size / 2));
    // Gets updated on the CQ.
    validateCQ(client, "testEnableDisable_0", /* resultSize: */ noTest, /* creates: */ size / 2,
        /* updates: */ size, /* deletes; */ 0, /* queryInserts: */ size / 2,
        /* queryUpdates: */ size, /* queryDeletes: */ 0, /* totalEvents: */ size * 3 / 2);

    // Close.
    closeClient(client);
    closeServer(server);
  }

  /**
   * Test for Complex queries.
   */
  @Test
  public void testQuery() {
    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    // Create CQs.
    createCQ(client, "testQuery_3", cqs[3]);
    executeCQ(client, "testQuery_3", true, null);

    createCQ(client, "testQuery_4", cqs[4]);
    executeCQ(client, "testQuery_4", true, null);

    createCQ(client, "testQuery_5", cqs[5]);
    executeCQ(client, "testQuery_5", true, null);

    createCQ(client, "testQuery_6", cqs[6]);
    executeCQ(client, "testQuery_6", true, null);

    createCQ(client, "testQuery_7", cqs[7]);
    executeCQ(client, "testQuery_7", true, null);

    createCQ(client, "testQuery_8", cqs[8]);
    executeCQ(client, "testQuery_8", true, null);

    // Close.
    closeClient(client);
    closeServer(server);
  }

  /**
   * Test for CQ Fail over.
   */
  @Test
  public void testCQFailOver() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client = VM.getVM(2);

    createServer(server1);

    final int port1 = server1.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();
    // Create client.
    // Properties props = new Properties();
    // Create client with redundancyLevel -1

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    createClient(client, new int[] {port1, ports[0]}, host0, "-1");

    int numCQs = 1;
    for (int i = 0; i < numCQs; i++) {
      // Create CQs.
      createCQ(client, "testCQFailOver_" + i, cqs[i]);
      executeCQ(client, "testCQFailOver_" + i, false, null);
    }
    Wait.pause(1000);

    // CREATE.
    createValues(server1, regions[0], 10);
    createValues(server1, regions[1], 10);
    waitForCreated(client, "testCQFailOver_0", KEY + 10);

    Wait.pause(1000);

    createServer(server2, ports[0]);
    final int thePort2 = server2.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    System.out
        .println("### Port on which server1 running : " + port1 + " Server2 running : " + thePort2);

    Wait.pause(8 * 1000);

    // UPDATE - 1.
    createValues(server1, regions[0], 10);
    createValues(server1, regions[1], 10);

    waitForUpdated(client, "testCQFailOver_0", KEY + 10);

    int[] resultsCnt = new int[] {10, 1, 2};

    for (int i = 0; i < numCQs; i++) {
      validateCQ(client, "testCQFailOver_" + i, noTest, resultsCnt[i], resultsCnt[i], noTest);
    }

    // Close server1.
    closeServer(server1);

    // Fail over should happen.
    Wait.pause(3 * 1000);

    for (int i = 0; i < numCQs; i++) {
      validateCQ(client, "testCQFailOver_" + i, noTest, resultsCnt[i], resultsCnt[i], noTest);
    }

    // UPDATE - 2
    this.clearCQListenerEvents(client, "testCQFailOver_0");
    createValues(server2, regions[0], 10);
    createValues(server2, regions[1], 10);

    for (int i = 1; i <= 10; i++) {
      waitForUpdated(client, "testCQFailOver_0", KEY + i);
    }

    for (int i = 0; i < numCQs; i++) {
      validateCQ(client, "testCQFailOver_" + i, noTest, resultsCnt[i], resultsCnt[i] * 2, noTest);
    }

    // Close.
    closeClient(client);
    closeServer(server2);
  }

  /**
   * Test for CQ Fail over/HA with redundancy level set.
   */
  @Test
  public void testCQHA() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM server3 = VM.getVM(2);

    VM client = VM.getVM(3);

    createServer(server1);

    final int port1 = server1.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    createServer(server2, ports[0]);
    final int thePort2 = server2.invoke(() -> CqQueryDUnitTest.getCacheServerPort());

    createServer(server3, ports[1]);
    final int port3 = server3.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    System.out.println("### Port on which server1 running : " + port1 + " server2 running : "
        + thePort2 + " Server3 running : " + port3);

    // Create client - With 3 server endpoints and redundancy level set to 2.

    // Create client with redundancyLevel 1
    createClient(client, new int[] {port1, thePort2, port3}, host0, "1");

    // Create CQs.
    int numCQs = 1;
    for (int i = 0; i < numCQs; i++) {
      // Create CQs.
      createCQ(client, "testCQHA_" + i, cqs[i]);
      executeCQ(client, "testCQHA_" + i, false, null);
    }

    Wait.pause(1000);

    // CREATE.
    createValues(server1, regions[0], 10);
    createValues(server1, regions[1], 10);

    waitForCreated(client, "testCQHA_0", KEY + 10);

    // Clients expected initial result.
    int[] resultsCnt = new int[] {10, 1, 2};

    // Close server1.
    // To maintain the redundancy; it will make connection to endpoint-3.
    closeServer(server1);
    Wait.pause(3 * 1000);

    // UPDATE-1.
    createValues(server2, regions[0], 10);
    createValues(server2, regions[1], 10);

    waitForUpdated(client, "testCQHA_0", KEY + 10);

    // Validate CQ.
    for (int i = 0; i < numCQs; i++) {
      validateCQ(client, "testCQHA_" + i, noTest, resultsCnt[i], resultsCnt[i], noTest);
    }

    // Close server-2
    closeServer(server2);
    Wait.pause(2 * 1000);

    // UPDATE - 2.
    clearCQListenerEvents(client, "testCQHA_0");

    createValues(server3, regions[0], 10);
    createValues(server3, regions[1], 10);

    // Wait for events at client.

    waitForUpdated(client, "testCQHA_0", KEY + 10);

    for (int i = 0; i < numCQs; i++) {
      validateCQ(client, "testCQHA_" + i, noTest, resultsCnt[i], resultsCnt[i] * 2, noTest);
    }

    // Close.
    closeClient(client);
    closeServer(server3);
  }

  /**
   * Test without CQs. This was added after an exception encountered with CQService, when there was
   * no CQService initiated.
   */
  @Test
  public void testWithoutCQs() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client = VM.getVM(2);

    createServer(server1);
    createServer(server2);

    final int port1 = server1.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    final int thePort2 = server2.invoke(() -> CqQueryDUnitTest.getCacheServerPort());

    SerializableRunnable createConnectionPool = new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        getCache();

        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);

        ClientServerTestCase.configureConnectionPool(regionFactory, host0, port1, thePort2, true,
            -1, -1, null);

        createRegion(regions[0], regionFactory.createRegionAttributes());
      }
    };

    // Create client.
    client.invoke(createConnectionPool);

    server1.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = 0; i < 20; i++) {
          region1.put("key-string-" + i, "value-" + i);
        }
      }
    });

    // Put some values on the client.
    client.invoke(new CacheSerializableRunnable("Put values client") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);

        for (int i = 0; i < 10; i++) {
          region1.put("key-string-" + i, "client-value-" + i);
        }
      }
    });

    Wait.pause(2 * 1000);
    closeServer(server1);
    closeServer(server2);
  }


  /**
   * Test getCQs for a regions
   */
  @Test
  public void testGetCQsForARegionName() {
    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    // Create CQs.
    createCQ(client, "testQuery_3", cqs[3]);
    executeCQ(client, "testQuery_3", true, null);

    createCQ(client, "testQuery_4", cqs[4]);
    executeCQ(client, "testQuery_4", true, null);

    createCQ(client, "testQuery_5", cqs[5]);
    executeCQ(client, "testQuery_5", true, null);

    createCQ(client, "testQuery_6", cqs[6]);
    executeCQ(client, "testQuery_6", true, null);
    // with regions[1]
    createCQ(client, "testQuery_7", cqs[7]);
    executeCQ(client, "testQuery_7", true, null);

    createCQ(client, "testQuery_8", cqs[8]);
    executeCQ(client, "testQuery_8", true, null);

    client.invoke(new CacheSerializableRunnable("Client disableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService;
        try {
          cqService = getCache().getQueryService();
          CqQuery[] cq = cqService.getCqs("/root/" + regions[0]);
          assertThat(cq)
              .describedAs(
                  "CQService should not return null for cqs on this region : /root/" + regions[0])
              .isNotNull();
          getCache().getLogger().info("cqs for region: /root/" + regions[0] + " : " + cq.length);
          // closing on of the cqs.

          cq[0].close();
          cq = cqService.getCqs("/root/" + regions[0]);
          assertThat(cq)
              .describedAs(
                  "CQService should not return null for cqs on this region : /root/" + regions[0])
              .isNotNull();
          getCache().getLogger().info("cqs for region: /root/" + regions[0]
              + " after closing one of the cqs : " + cq.length);

          cq = cqService.getCqs("/root/" + regions[1]);
          getCache().getLogger().info("cqs for region: /root/" + regions[1] + " : " + cq.length);
          assertThat(cq)
              .describedAs(
                  "CQService should not return null for cqs on this region : /root/" + regions[1])
              .isNotNull();
        } catch (Exception cqe) {
          fail("Failed to getCQService", cqe);
        }
      }
    });

    // Close.
    closeClient(client);
    closeServer(server);

  }


  /**
   * Test exception message thrown when replicate region with local destroy is used
   */
  @Test
  public void testCqExceptionForReplicateRegionWithEvictionLocalDestroy() {
    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServerOnly(server, 0);
    createReplicateRegionWithLocalDestroy(server, new String[] {regions[0]});

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createLocalRegion(client, new int[] {thePort}, host0, "-1", new String[] {regions[0]});
    // Create CQs.
    createCQ(client, "testQuery_3", "select * from /" + regions[0]);
    String expectedError =
        String.format("CQ is not supported for replicated region: %s with eviction action: %s",
            "/" + regions[0], EvictionAction.LOCAL_DESTROY);
    try {
      executeCQ(client, "testQuery_3", false, expectedError);
    } catch (Exception e) {
      assertThat(e.getCause().getCause().getMessage().contains(expectedError)).isTrue();

    }
    // Close.
    closeClient(client);
    closeServer(server);

  }

  /**
   * Tests execution of queries with NULL in where clause like where ID = NULL etc.
   */
  @Test
  public void testQueryWithNULLInWhereClause() {
    VM server = VM.getVM(0);
    VM client = VM.getVM(1);
    VM producer = VM.getVM(2);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);
    // producer is not doing any thing.
    createClient(producer, thePort, host0);

    final int size = 50;
    createValues(producer, regions[0], size);

    createCQ(client, "testQuery_9", cqs[9]);
    executeCQ(client, "testQuery_9", true, null);

    createValues(producer, regions[0], (2 * size));

    for (int i = 1; i <= size; i++) {
      if (i % 2 == 0) {
        waitForUpdated(client, "testQuery_9", KEY + i);
      }
    }

    for (int i = (size + 1); i <= 2 * size; i++) {
      if (i % 2 == 0) {
        waitForCreated(client, "testQuery_9", KEY + i);
      }
    }

    validateCQ(client, "testQuery_9", noTest, 25, 25, noTest);

    // Close.
    closeClient(client);
    closeServer(server);

  }

  /**
   * Tests execution of queries with NULL in where clause like where ID = NULL etc.
   */
  @Test
  public void testForSupportedRegionAttributes() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client = VM.getVM(2);

    // Create server with Global scope.
    SerializableRunnable createServer = new CacheSerializableRunnable("Create Cache Server") {
      public void run2() throws CacheException {
        logger.info("### Create Cache Server. ###");

        // Create region with Global scope
        AttributesFactory factory1 = new AttributesFactory();
        factory1.setScope(Scope.GLOBAL);
        factory1.setDataPolicy(DataPolicy.REPLICATE);
        createRegion(regions[0], factory1.createRegionAttributes());

        // Create region with non Global, distributed_ack scope
        AttributesFactory factory2 = new AttributesFactory();
        factory2.setScope(Scope.DISTRIBUTED_NO_ACK);
        factory2.setDataPolicy(DataPolicy.REPLICATE);
        createRegion(regions[1], factory2.createRegionAttributes());

        Wait.pause(2000);

        try {
          startBridgeServer(port, true);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }
        Wait.pause(2000);

      }
    };

    server1.invoke(createServer);
    server2.invoke(createServer);

    final int port1 = server1.invoke(() -> CqQueryDUnitTest.getCacheServerPort());

    final String host0 = NetworkUtils.getServerHostName();

    final int thePort2 = server2.invoke(() -> CqQueryDUnitTest.getCacheServerPort());

    // Create client.
    createClient(client, new int[] {port1, thePort2}, host0, "-1");

    // Create CQ on region with GLOBAL SCOPE.
    createCQ(client, "testForSupportedRegionAttributes_0", cqs[0]);
    executeCQ(client, "testForSupportedRegionAttributes_0", false, null);

    int size = 5;

    createValues(server1, regions[0], size);

    for (int i = 1; i <= size; i++) {
      waitForCreated(client, "testForSupportedRegionAttributes_0", KEY + i);
    }

    // Create CQ on region with non GLOBAL, DISTRIBUTED_ACK SCOPE.
    createCQ(client, "testForSupportedRegionAttributes_1", cqs[2]);

    String errMsg =
        "The replicated region " + " specified in CQ creation does not have scope supported by CQ."
            + " The CQ supported scopes are DISTRIBUTED_ACK and GLOBAL.";
    final String expectedErr = "Cq not registered on primary";
    client.invoke(new CacheSerializableRunnable("Set expect") {
      public void run2() {
        getCache().getLogger()
            .info("<ExpectedException action=add>" + expectedErr + "</ExpectedException>");
      }
    });

    try {
      executeCQ(client, "testForSupportedRegionAttributes_1", false, "CqException");
      fail("The test should have failed with exception, " + errMsg);
    } catch (Exception ex) {
      // Expected.
    } finally {
      client.invoke(new CacheSerializableRunnable("Remove expect") {
        public void run2() {
          getCache().getLogger()
              .info("<ExpectedException action=remove>" + expectedErr + "</ExpectedException>");
        }
      });
    }

    // Close.
    closeClient(client);
    closeServer(server1);
    closeServer(server2);

  }

  @Test
  public void testCQWhereCondOnShort() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    /* CQ Test with initial Values. */
    int size = 5;
    createValuesWithShort(server, regions[0], size);
    Wait.pause(500);

    // Create CQs.
    createCQ(client, "testCQResultSet_0", shortTypeCQs[0]);

    // Check resultSet Size.
    executeCQ(client, "testCQResultSet_0", true, 5, null);

    closeClient(client);
    closeServer(server);
  }

  @Test
  public void testCQEquals() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    /* CQ Test with initial Values. */
    int size = 10;
    // create values
    createValuesAsPrimitives(server, regions[0], size);
    Wait.pause(500);

    // Create CQs.
    createCQ(client, "equalsQuery1", "select * from /root/regionA p where p.equals('seeded')");

    // Check resultSet Size.
    executeCQ(client, "equalsQuery1", true, 2, null);

    // Create CQs.
    createCQ(client, "equalsQuery2", "select * from /root/regionA p where p='seeded'");

    // Check resultSet Size.
    executeCQ(client, "equalsQuery2", true, 2, null);

    // Create CQs.
    createCQ(client, "equalsStatusQuery1",
        "select * from /root/regionA p where p.status.equals('inactive')");

    // Check resultSet Size.
    executeCQ(client, "equalsStatusQuery1", true, 1, null);

    // Create CQs.
    createCQ(client, "equalsStatusQuery2",
        "select * from /root/regionA p where p.status='inactive'");

    // Check resultSet Size.
    executeCQ(client, "equalsStatusQuery2", true, 1, null);

    closeClient(client);
    closeServer(server);
  }

  @Test
  public void testCQEqualsWithIndex() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    /* CQ Test with initial Values. */
    int size = 10;
    // create values
    createIndex(server, "index1", "p.status", "/root/regionA p");
    createValuesAsPrimitives(server, regions[0], size);
    Wait.pause(500);

    // Create CQs.
    createCQ(client, "equalsQuery1", "select * from /root/regionA p where p.equals('seeded')");

    // Check resultSet Size.
    executeCQ(client, "equalsQuery1", true, 2, null);

    // Create CQs.
    createCQ(client, "equalsQuery2", "select * from /root/regionA p where p='seeded'");

    // Check resultSet Size.
    executeCQ(client, "equalsQuery2", true, 2, null);

    // Create CQs.
    createCQ(client, "equalsStatusQuery1",
        "select * from /root/regionA p where p.status.equals('inactive')");

    // Check resultSet Size.
    executeCQ(client, "equalsStatusQuery1", true, 1, null);

    // Create CQs.
    createCQ(client, "equalsStatusQuery2",
        "select * from /root/regionA p where p.status='inactive'");

    // Check resultSet Size.
    executeCQ(client, "equalsStatusQuery2", true, 1, null);

    closeClient(client);
    closeServer(server);
  }


  // Tests that cqs get an onCqDisconnect and onCqConnect
  @Test
  public void testCQAllServersCrash() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client = VM.getVM(2);

    createServer(server1);

    final int port1 = server1.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    createClient(client, new int[] {port1, ports[0]}, host0, "-1");

    // Create CQs.
    createCQ(client, "testCQAllServersLeave_" + 11, cqs[11], true);
    executeCQ(client, "testCQAllServersLeave_" + 11, false, null);

    Wait.pause(5 * 1000);
    waitForCqsConnected(client, "testCQAllServersLeave_11", 1);

    // CREATE.
    createValues(server1, regions[0], 10);
    waitForCreated(client, "testCQAllServersLeave_11", KEY + 10);

    createServer(server2, ports[0]);
    server2.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    Wait.pause(8 * 1000);

    // Close server1.
    crashServer(server1);

    Wait.pause(3 * 1000);

    crashServer(server2);

    Wait.pause(3 * 1000);
    waitForCqsDisconnected(client, "testCQAllServersLeave_11", 1);

    // Close.
    closeClient(client);
    closeCrashServer(server1);
    closeCrashServer(server2);
  }

  // Tests that we receive both an onCqConnected and a onCqDisconnected message
  @Test
  public void testCQAllServersLeave() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client = VM.getVM(2);

    createServer(server1);
    final int port1 = server1.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    createClient(client, new int[] {port1, ports[0]}, host0, "-1");

    Wait.pause(5 * 1000);
    // Create CQs.
    createCQ(client, "testCQAllServersLeave_" + 11, cqs[11], true);
    executeCQ(client, "testCQAllServersLeave_" + 11, false, null);

    Wait.pause(5 * 1000);
    waitForCqsConnected(client, "testCQAllServersLeave_11", 1);
    // CREATE.
    createValues(server1, regions[0], 10);
    waitForCreated(client, "testCQAllServersLeave_11", KEY + 10);

    createServer(server2, ports[0]);
    final int thePort2 = server2.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    Wait.pause(10 * 1000);

    // Close server1 and pause so server has chance to close
    closeServer(server1);
    Wait.pause(10 * 1000);
    waitForCqsDisconnected(client, "testCQAllServersLeave_11", 0);

    // Close server 2 and pause so server has a chance to close
    closeServer(server2);
    Wait.pause(10 * 1000);
    waitForCqsDisconnected(client, "testCQAllServersLeave_11", 1);

    // Close.
    closeClient(client);
  }

  // Test CQStatus listeners, onCqDisconnect should trigger when All servers leave
  // and onCqConnect should trigger when a cq is first connected and when the pool
  // goes from no primary queue to having a primary
  @Test
  public void testCQAllServersLeaveAndRejoin() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client = VM.getVM(2);

    createServer(server1);
    final int port1 = server1.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    createClient(client, new int[] {port1, ports[0]}, host0, "-1");

    // Create CQs.
    createCQ(client, "testCQAllServersLeave_" + 11, cqs[11], true);
    executeCQ(client, "testCQAllServersLeave_" + 11, false, null);
    Wait.pause(5 * 1000);
    // listener should have had onCqConnected invoked
    waitForCqsConnected(client, "testCQAllServersLeave_11", 1);

    // create entries
    createValues(server1, regions[0], 10);
    waitForCreated(client, "testCQAllServersLeave_11", KEY + 10);

    // start server 2
    createServer(server2, ports[0]);
    server2.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    Wait.pause(8 * 1000);

    // Close server1.
    closeServer(server1);
    // Give the server time to shut down
    Wait.pause(10 * 1000);
    // We should not yet get a disconnect because we still have server2
    waitForCqsDisconnected(client, "testCQAllServersLeave_11", 0);

    // Close the server2
    closeServer(server2);
    Wait.pause(10 * 1000);
    waitForCqsDisconnected(client, "testCQAllServersLeave_11", 1);

    // reconnect server1. Our total connects for this test run are now 2
    restartBridgeServer(server1, port1);
    Wait.pause(10 * 1000);
    waitForCqsConnected(client, "testCQAllServersLeave_11", 2);

    // Disconnect again and now our total disconnects should be 2
    Wait.pause(10 * 1000);
    closeServer(server1);
    waitForCqsDisconnected(client, "testCQAllServersLeave_11", 2);

    // Close.
    closeClient(client);
  }


  /*
   * Tests that the cqs do not get notified if primary leaves and a new primary is elected
   */
  @Test
  public void testCQPrimaryLeaves() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client = VM.getVM(2);

    createServer(server1);
    final int port1 = server1.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    createClient(client, new int[] {port1, ports[0]}, host0, "-1");

    // Create CQs.
    createCQ(client, "testCQAllServersLeave_" + 11, cqs[11], true);
    executeCQ(client, "testCQAllServersLeave_" + 11, false, null);

    Wait.pause(5 * 1000);
    waitForCqsConnected(client, "testCQAllServersLeave_11", 1);
    // CREATE.
    createValues(server1, regions[0], 10);
    waitForCreated(client, "testCQAllServersLeave_11", KEY + 10);

    createServer(server2, ports[0]);
    final int thePort2 = server2.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    Wait.pause(8 * 1000);

    // Close server1 and give time for server1 to actually shutdown
    closeServer(server1);
    Wait.pause(10 * 1000);
    waitForCqsDisconnected(client, "testCQAllServersLeave_11", 0);

    // Close server2 and give time for server2 to shutdown before checking disconnected count
    closeServer(server2);
    Wait.pause(10 * 1000);
    waitForCqsDisconnected(client, "testCQAllServersLeave_11", 1);

    // Close.
    closeClient(client);
    closeServer(server1);
  }

  // Tests when two pools each are configured to a different server
  // Each cq uses a different pool and the servers are shutdown.
  // The listeners for each cq should receive a connect and disconnect
  // when their respective servers are shutdown
  @Test
  public void testCQAllServersLeaveMultiplePool() throws Exception {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client = VM.getVM(2);

    createServer(server1);

    final int port1 = server1.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    createServer(server2, ports[0]);
    final int thePort2 = server2.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    Wait.pause(8 * 1000);

    // Create client
    createClientWith2Pools(client, new int[] {port1}, new int[] {thePort2}, host0, "-1");

    // Create CQs.
    createCQ(client, "testCQAllServersLeave_" + 11, cqs[11], true);
    executeCQ(client, "testCQAllServersLeave_" + 11, false, null);

    createCQ(client, "testCQAllServersLeave_" + 12, cqs[12], true);
    executeCQ(client, "testCQAllServersLeave_" + 12, false, null);

    Wait.pause(5 * 1000);
    waitForCqsConnected(client, "testCQAllServersLeave_11", 1);
    waitForCqsConnected(client, "testCQAllServersLeave_12", 1);
    // CREATE.
    createValues(server2, regions[0], 10);
    createValues(server2, regions[1], 10);
    waitForCreated(client, "testCQAllServersLeave_11", KEY + 10);

    // Close server1 pause is unnecessary here since each cq has it's own pool
    // and each pool is configured only to 1 server.
    closeServer(server1);
    waitForCqsDisconnected(client, "testCQAllServersLeave_11", 1);

    createValues(server2, regions[1], 20);
    waitForCreated(client, "testCQAllServersLeave_12", KEY + 19);

    // Close server 2 pause unnecessary here since each cq has it's own pool
    closeServer(server2);
    waitForCqsDisconnected(client, "testCQAllServersLeave_12", 1);

    // Close.
    closeClient(client);
  }


  @Test
  public void testCqCloseAndExecuteWithInitialResults() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    /* CQ Test with initial Values. */
    int size = 5;
    createValuesWithShort(server, regions[0], size);
    Wait.pause(500);

    // Create CQs.
    executeAndCloseAndExecuteIRMultipleTimes(client, shortTypeCQs[0]);

    closeClient(client);

    closeServer(server);
  }

  @Test
  public void testCQEventsWithNotEqualsUndefined() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    createServer(server);

    final int thePort = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    createClient(client, thePort, host0);

    // Create CQs.
    createCQ(client, "testCQEventsWithUndefined_0",
        "SELECT ALL * FROM /root/" + regions[0] + " p where p.position2.secId <> 'ABC'");

    executeCQ(client, "testCQEventsWithUndefined_0", false, null);

    // Init values at server.
    int size = 10;
    createValues(server, regions[0], size);

    waitForCreated(client, "testCQEventsWithUndefined_0", KEY + size);

    // validate Create events.
    validateCQ(client, "testCQEventsWithUndefined_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ 0, /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ 0,
        /* queryDeletes: */ 0, /* totalEvents: */ size);

    // Update values.
    createValues(server, regions[0], 5);
    createValues(server, regions[0], 10);

    waitForUpdated(client, "testCQEventsWithUndefined_0", KEY + size);

    // validate Update events.
    validateCQ(client, "testCQEventsWithUndefined_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ 15, /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ 15,
        /* queryDeletes: */ 0, /* totalEvents: */ size + 15);

    // Validate delete events.
    deleteValues(server, regions[0], 5);
    waitForDestroyed(client, "testCQEventsWithUndefined_0", KEY + 5);

    validateCQ(client, "testCQEventsWithUndefined_0", /* resultSize: */ noTest, /* creates: */ size,
        /* updates: */ 15, /* deletes; */5, /* queryInserts: */ size, /* queryUpdates: */ 15,
        /* queryDeletes: */ 5, /* totalEvents: */ size + 15 + 5);

    // Insert invalid Events.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = -1; i >= -5; i--) {
          region1.put(KEY + i, KEY + i);
        }
      }
    });

    Wait.pause(1000);
    // cqs should get any creates and inserts even for invalid
    // since this is a NOT EQUALS query which adds Undefined to
    // results
    validateCQ(client, "testCQEventsWithUndefined_0", /* resultSize: */ noTest,
        /* creates: */ size + 5, /* updates: */ 15, /* deletes; */5, /* queryInserts: */ size + 5,
        /* queryUpdates: */ 15, /* queryDeletes: */ 5, /* totalEvents: */ size + 15 + 5 + 5);

    // Close.
    closeClient(client);
    closeServer(server);
  }

  // HELPER METHODS....

  /* For debug purpose - Compares entries in the region */
  private void validateServerClientRegionEntries(VM server, VM client, final String regionName) {

    server.invoke(new CacheSerializableRunnable("Server Region Entries") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        logger.info("### Entries in Server :" + region.keySet().size());
      }
    });

    client.invoke(new CacheSerializableRunnable("Client Region Entries") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        logger.info("### Entries in Client :" + region.keySet().size());
      }
    });
  }

  /*
   * Used only by tests that start and stop a server only to need to start the cache server again
   */
  private void restartBridgeServer(VM server, final int port) {
    server.invoke(new CacheSerializableRunnable("Start cache server") {
      public void run2() {
        try {
          restartBridgeServers(getCache());
        } catch (IOException e) {
          throw new CacheException(e) {};
        }
      }
    });
  }


  /**
   * Starts a cache server on the given port to serve up the given region.
   *
   * @since GemFire 4.0
   */
  public void startBridgeServer(int port) throws IOException {
    startBridgeServer(port, CacheServer.DEFAULT_NOTIFY_BY_SUBSCRIPTION);
  }

  /**
   * Starts a cache server on the given port, using the given deserializeValues and
   * notifyBySubscription to serve up the given region.
   *
   * @since GemFire 4.0
   */
  public void startBridgeServer(int port, boolean notifyBySubscription) throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /**
   * Stops the cache server that serves up the given cache.
   *
   * @since GemFire 4.0
   */
  private void stopBridgeServer(Cache cache) {
    CacheServer bridge = cache.getCacheServers().iterator().next();
    bridge.stop();
    assertThat(bridge.isRunning()).isFalse();
  }

  private void stopBridgeServers(Cache cache) {
    CacheServer bridge;
    for (CacheServer cacheServer : cache.getCacheServers()) {
      bridge = cacheServer;
      bridge.stop();
      assertThat(bridge.isRunning()).isFalse();
    }
  }

  private void restartBridgeServers(Cache cache) throws IOException {
    CacheServer bridge;
    for (CacheServer cacheServer : cache.getCacheServers()) {
      bridge = cacheServer;
      bridge.start();
      assertThat(bridge.isRunning()).isTrue();
    }
  }

  private InternalDistributedSystem createLonerDS() {
    disconnectFromDS();
    Properties lonerProps = new Properties();
    lonerProps.setProperty(MCAST_PORT, "0");
    lonerProps.setProperty(LOCATORS, "");
    InternalDistributedSystem ds = getSystem(lonerProps);
    assertThat(ds.getDistributionManager().getOtherDistributionManagerIds().size()).isEqualTo(0);
    return ds;
  }

  /**
   * Returns region attributes for a <code>LOCAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    return factory.createRegionAttributes();
  }


}
