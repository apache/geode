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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqServiceStatistics;
import org.apache.geode.cache.query.CqStatistics;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.cq.internal.CqQueryImpl;
import org.apache.geode.cache.query.cq.internal.CqServiceImpl;
import org.apache.geode.cache.query.cq.internal.CqServiceVsdStats;
import org.apache.geode.cache.query.internal.CqQueryVsdStats;
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This class tests the ContinuousQuery mechanism in GemFire. This includes the test with different
 * data activities.
 */
@Category({ClientSubscriptionTest.class})
public class CqStatsUsingPoolDUnitTest extends JUnit4CacheTestCase {

  // TODO: delete this use of CqQueryUsingPoolDUnitTest
  private final CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest();

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "false");
    return result;
  }

  @Override
  public final void postSetUp() throws Exception {
    // avoid IllegalStateException from HandShake by connecting all vms to
    // system before creating pool
    getSystem();
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      @Override
      public void run() {
        getSystem();
      }
    });
    postSetUpCqStatsUsingPoolDUnitTest();
  }

  protected void postSetUpCqStatsUsingPoolDUnitTest() throws Exception {}

  private void validateCQStats(VM vm, final String cqName, final int creates, final int updates,
      final int deletes, final int totalEvents, final int cqListenerInvocations) {
    vm.invoke(new CacheSerializableRunnable("Validate CQs") {
      @Override
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Validating CQ Stats. ### " + cqName);
        // Get CQ Service.
        QueryService qService = null;
        try {
          qService = getCache().getQueryService();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to get query service.");
        }

        CqService cqService = null;
        try {
          cqService = ((DefaultQueryService) qService).getCqService();
        } catch (CqException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          fail("Failed to get CqService, CQ : " + cqName);
        }
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        if (cqs.size() == 0) {
          fail("Failed to get CqQuery for CQ : " + cqName);
        }
        CqQueryImpl cQuery = (CqQueryImpl) cqs.iterator().next();

        CqStatistics cqStats = cQuery.getStatistics();
        CqQueryVsdStats cqVsdStats = cQuery.getVsdStats();
        if (cqStats == null || cqVsdStats == null) {
          fail("Failed to get CqQuery Stats for CQ : " + cqName);
        }

        getCache().getLogger()
            .info("#### CQ stats for " + cQuery.getName() + ": " + " Events Total: "
                + cqStats.numEvents() + " Events Created: " + cqStats.numInserts()
                + " Events Updated: " + cqStats.numUpdates() + " Events Deleted: "
                + cqStats.numDeletes() + " CQ Listener invocations: "
                + cqVsdStats.getNumCqListenerInvocations() + " Initial results time (nano sec): "
                + cqVsdStats.getCqInitialResultsTime());


        // Check for totalEvents count.
        if (totalEvents != CqQueryUsingPoolDUnitTest.noTest) {
          // Result size validation.
          assertEquals("Total Event Count mismatch", totalEvents, cqStats.numEvents());
        }

        // Check for create count.
        if (creates != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("Create Event mismatch", creates, cqStats.numInserts());
        }

        // Check for update count.
        if (updates != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("Update Event mismatch", updates, cqStats.numUpdates());
        }

        // Check for delete count.
        if (deletes != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("Delete Event mismatch", deletes, cqStats.numDeletes());
        }

        // Check for CQ listener invocations.
        if (cqListenerInvocations != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("CQ Listener invocations mismatch", cqListenerInvocations,
              cqVsdStats.getNumCqListenerInvocations());
        }
        //// Check for initial results time.
        // if (initialResultsTime != CqQueryUsingPoolDUnitTest.noTest &&
        //// cqVsdStats.getCqInitialResultsTime() <= 0) {
        // assertIndexDetailsEquals("Initial results time mismatch", initialResultsTime,
        //// cqVsdStats.getCqInitialResultsTime());
        // }
      }
    });
  }

  private void validateCQServiceStats(VM vm, final int created, final int activated,
      final int stopped, final int closed, final int cqsOnClient, final int cqsOnRegion,
      final int clientsWithCqs) {
    vm.invoke(new CacheSerializableRunnable("Validate CQ Service Stats") {
      @Override
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Validating CQ Service Stats. ### ");
        // Get CQ Service.
        QueryService qService = null;
        try {
          qService = getCache().getQueryService();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }
        CqServiceStatistics cqServiceStats = null;
        cqServiceStats = qService.getCqStatistics();
        CqServiceVsdStats cqServiceVsdStats = null;
        try {
          cqServiceVsdStats =
              ((CqServiceImpl) ((DefaultQueryService) qService).getCqService()).stats();
        } catch (CqException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        if (cqServiceStats == null) {
          fail("Failed to get CQ Service Stats");
        }

        getCache().getLogger()
            .info("#### CQ Service stats: " + " CQs created: " + cqServiceStats.numCqsCreated()
                + " CQs active: " + cqServiceStats.numCqsActive() + " CQs stopped: "
                + cqServiceStats.numCqsStopped() + " CQs closed: " + cqServiceStats.numCqsClosed()
                + " CQs on Client: " + cqServiceStats.numCqsOnClient()
                + " CQs on region " + SEPARATOR + "root" + SEPARATOR + "regionA : "
                + cqServiceVsdStats.numCqsOnRegion(getCache(),
                    SEPARATOR + "root" + SEPARATOR + "regionA")
                + " Clients with CQs: " + cqServiceVsdStats.getNumClientsWithCqs());


        // Check for created count.
        if (created != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("Number of CQs created mismatch", created, cqServiceStats.numCqsCreated());
        }

        // Check for activated count.
        if (activated != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("Number of CQs activated mismatch", activated,
              cqServiceStats.numCqsActive());
        }

        // Check for stopped count.
        if (stopped != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("Number of CQs stopped mismatch", stopped, cqServiceStats.numCqsStopped());
        }

        // Check for closed count.
        if (closed != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("Number of CQs closed mismatch", closed, cqServiceStats.numCqsClosed());
        }

        // Check for CQs on client count.
        if (cqsOnClient != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("Number of CQs on client mismatch", cqsOnClient,
              cqServiceStats.numCqsOnClient());
        }

        // Check for CQs on region.
        if (cqsOnRegion != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals(
              "Number of CQs on region " + SEPARATOR + "root" + SEPARATOR + "regionA mismatch",
              cqsOnRegion,
              cqServiceVsdStats.numCqsOnRegion(getCache(),
                  SEPARATOR + "root" + SEPARATOR + "regionA"));
        }

        // Check for clients with CQs count.
        if (clientsWithCqs != CqQueryUsingPoolDUnitTest.noTest) {
          assertEquals("Clints with CQs mismatch", clientsWithCqs,
              cqServiceVsdStats.getNumClientsWithCqs());
        }
      }
    });
  }

  private static final int PAUSE = 8 * 1000; // 5 * 1000

  /**
   * Test for CQ and CQ Service Statistics
   *
   */
  @Test
  public void testCQStatistics() throws Exception {

    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    /* Init Server and Client */
    cqDUnitTest.createServer(server);
    final int port = server.invoke(CqQueryUsingPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testCQStatistics";
    cqDUnitTest.createPool(client, poolName, host0, port);

    // cqDUnitTest.createClient(client, port, host0);

    /* Create CQs. */
    cqDUnitTest.createCQ(client, poolName, "testCQStatistics_0", cqDUnitTest.cqs[0]);

    /* Init values at server. */
    int size = 100;
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    cqDUnitTest.executeCQ(client, "testCQStatistics_0", true, null);

    // Wait for CQ to be executed.
    cqDUnitTest.waitForCqState(client, "testCQStatistics_0", CqStateImpl.RUNNING);

    // Test CQ stats
    validateCQStats(client, "testCQStatistics_0", 0, 0, 0, 0, 0);

    // The stat would have not updated yet.
    // Commenting out the following check; the check for resultset initialization
    // is anyway done in the next validation.
    // validateCQStats(server, "testCQStatistics_0", 0, 0, 0, 0, CqQueryUsingPoolDUnitTest.noTest,
    // 1);

    /* Init values at server. */
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 200);
    // Wait for client to Synch.
    cqDUnitTest.waitForCreated(client, "testCQStatistics_0", CqQueryUsingPoolDUnitTest.KEY + 200);
    Wait.pause(PAUSE);
    size = 200;

    // validate CQs.
    cqDUnitTest.validateCQ(client, "testCQStatistics_0",
        /* resultSize: */ CqQueryUsingPoolDUnitTest.noTest, /* creates: */ 100, /* updates: */ 100,
        /* deletes; */ 0, /* queryInserts: */ 100, /* queryUpdates: */ 100, /* queryDeletes: */ 0,
        /* totalEvents: */ 200);

    // Test CQ stats
    validateCQStats(client, "testCQStatistics_0", 100, 100, 0, 200, 200);
    // We don't have serverside CQ name
    validateCQStats(server, "testCQStatistics_0", 100, 100, 0, 200,
        CqQueryUsingPoolDUnitTest.noTest);

    /* Delete values at server. */
    cqDUnitTest.deleteValues(server, cqDUnitTest.regions[0], 100);
    // Wait for client to Synch.
    cqDUnitTest.waitForDestroyed(client, "testCQStatistics_0", CqQueryUsingPoolDUnitTest.KEY + 100);
    size = 10;
    Wait.pause(PAUSE);

    cqDUnitTest.validateCQ(client, "testCQStatistics_0",
        /* resultSize: */ CqQueryUsingPoolDUnitTest.noTest, /* creates: */100, /* updates: */ 100,
        /* deletes; */ 100, /* queryInserts: */ 100, /* queryUpdates: */ 100,
        /* queryDeletes: */ 100, /* totalEvents: */ 300);

    // Test CQ stats
    validateCQStats(client, "testCQStatistics_0", 100, 100, 100, 300, 300);
    // We don't have serverside CQ name
    validateCQStats(server, "testCQStatistics_0", 100, 100, 100, 300,
        CqQueryUsingPoolDUnitTest.noTest);

    // Test CQ Close
    cqDUnitTest.closeCQ(client, "testCQStatistics_0");
    Wait.pause(PAUSE);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  /**
   * Test for CQ Service Statistics
   *
   */
  @Test
  public void testCQServiceStatistics() throws Exception {

    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);


    /* Init Server and Client */
    cqDUnitTest.createServer(server);
    final int port = server.invoke(CqQueryUsingPoolDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName1 = "testCQServiceStatistics1";
    String poolName2 = "testCQServiceStatistics2";

    cqDUnitTest.createPool(client1, poolName1, host0, port);
    cqDUnitTest.createPool(client2, poolName2, host0, port);

    // cqDUnitTest.createClient(client1, port, host0);
    // cqDUnitTest.createClient(client2, port, host0);

    /* Create CQs. */
    String cqName = "testCQServiceStatistics_0";
    String cqName10 = "testCQServiceStatistics_10";
    cqDUnitTest.createCQ(client1, poolName1, cqName, cqDUnitTest.cqs[0]);
    cqDUnitTest.createCQ(client2, poolName2, cqName10, cqDUnitTest.cqs[2]);
    Wait.pause(PAUSE);
    // Test CQ Service stats
    getCache().getLogger().info("Validating CQ Service stats on clients: #1");
    validateCQServiceStats(client1, 1, 0, 1, 0, 1, 1, CqQueryUsingPoolDUnitTest.noTest);
    validateCQServiceStats(server, 0, 0, 0, 0, CqQueryUsingPoolDUnitTest.noTest, 0, 0);

    cqDUnitTest.executeCQ(client1, cqName, false, null);
    cqDUnitTest.executeCQ(client2, cqName10, false, null);
    Wait.pause(PAUSE);

    getCache().getLogger().info("Validating CQ Service stats on clients: #2");
    validateCQServiceStats(client1, 1, 1, 0, 0, 1, 1, CqQueryUsingPoolDUnitTest.noTest);
    validateCQServiceStats(client2, 1, 1, 0, 0, 1, CqQueryUsingPoolDUnitTest.noTest,
        CqQueryUsingPoolDUnitTest.noTest);

    getCache().getLogger().info("Validating CQ Service stats on server: #1");
    validateCQServiceStats(server, 2, 2, 0, 0, CqQueryUsingPoolDUnitTest.noTest, 1, 2);

    /* Init values at server. */
    int size = 10;
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);
    // Wait for client to Synch.
    cqDUnitTest.waitForCreated(client1, "testCQServiceStatistics_0",
        CqQueryUsingPoolDUnitTest.KEY + size);

    // validate CQs.
    cqDUnitTest.validateCQ(client1, cqName, /* resultSize: */ CqQueryUsingPoolDUnitTest.noTest,
        /* creates: */ size, /* updates: */ 0, /* deletes; */ 0, /* queryInserts: */ size,
        /* queryUpdates: */ 0, /* queryDeletes: */ 0, /* totalEvents: */ size);
    Wait.pause(PAUSE);

    // Test CQ Service stats
    getCache().getLogger().info("Validating CQ Service stats on clients: #3");
    validateCQServiceStats(client1, 1, 1, 0, 0, 1, 1, CqQueryUsingPoolDUnitTest.noTest);
    validateCQServiceStats(client2, 1, 1, 0, 0, 1, CqQueryUsingPoolDUnitTest.noTest,
        CqQueryUsingPoolDUnitTest.noTest);

    getCache().getLogger().info("Validating CQ Service stats on server: #1");
    validateCQServiceStats(server, 2, 2, 0, 0, CqQueryUsingPoolDUnitTest.noTest, 1, 2);


    // Create CQs with no name, execute, and close.
    cqDUnitTest.createAndExecCQNoName(client1, poolName1, cqDUnitTest.cqs[0]);
    Wait.pause(PAUSE);

    // Test CQ Service stats
    getCache().getLogger().info("Validating CQ Service stats on client: #4");
    validateCQServiceStats(client1, 21, 1, 0, 20, 1, 1, CqQueryUsingPoolDUnitTest.noTest);

    getCache().getLogger().info("Validating CQ Service stats on server: #2");
    validateCQServiceStats(server, 22, 2, 0, 20, CqQueryUsingPoolDUnitTest.noTest, 1, 2);

    // Test CQ Close
    cqDUnitTest.closeCQ(client1, cqName);
    Wait.pause(PAUSE);

    // Test CQ Service stats
    getCache().getLogger().info("Validating CQ Service stats on client: #5");
    validateCQServiceStats(client1, 21, 0, 0, 21, 0, 0, CqQueryUsingPoolDUnitTest.noTest);

    getCache().getLogger().info("Validating CQ Service stats on server: #3");
    validateCQServiceStats(server, 22, 1, 0, 21, CqQueryUsingPoolDUnitTest.noTest, 0, 1);

    // Test stop CQ
    cqDUnitTest.stopCQ(client2, cqName10);
    Wait.pause(PAUSE);

    // Test CQ Service stats
    getCache().getLogger().info("Validating CQ Service stats on client: #6");
    validateCQServiceStats(client2, 1, 0, 1, 0, 1, CqQueryUsingPoolDUnitTest.noTest,
        CqQueryUsingPoolDUnitTest.noTest);
    getCache().getLogger().info("Validating CQ Service stats on server: #4");
    validateCQServiceStats(server, 22, 0, 1, 21, CqQueryUsingPoolDUnitTest.noTest,
        CqQueryUsingPoolDUnitTest.noTest, 1);

    // Test CQ Close
    cqDUnitTest.closeCQ(client2, cqName10);
    Wait.pause(PAUSE);

    // Test CQ Service stats
    getCache().getLogger().info("Validating CQ Service stats on client: #7");
    validateCQServiceStats(client1, 21, 0, 0, 21, 0, 0, CqQueryUsingPoolDUnitTest.noTest);
    validateCQServiceStats(client2, 1, 0, 0, 1, 0, 0, CqQueryUsingPoolDUnitTest.noTest);
    getCache().getLogger().info("Validating CQ Service stats on server: #5");
    validateCQServiceStats(server, 22, 0, 0, 22, CqQueryUsingPoolDUnitTest.noTest, 0, 0);

    // Close.
    cqDUnitTest.closeClient(client1);
    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeServer(server);
  }

}
