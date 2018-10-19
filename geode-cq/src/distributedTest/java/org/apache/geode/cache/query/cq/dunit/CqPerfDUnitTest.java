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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.CqServiceImpl;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.cache.query.internal.cq.ServerCQImpl;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.AvailablePortHelper;
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
 * This class tests the ContinuousQuery mechanism in GemFire. This includes the test with different
 * data activities.
 */
@SuppressWarnings("SpellCheckingInspection")
@Category({ClientSubscriptionTest.class})
public class CqPerfDUnitTest extends JUnit4CacheTestCase {
  private final Logger logger = LogService.getLogger();
  @SuppressWarnings("CanBeFinal")
  private CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest();

  public CqPerfDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() {
    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating connection pools
    getSystem();
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
  }

  /**
   * Tests the cq performance.
   */
  @Ignore("perf")
  @Test
  public void testCQPerf() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    cqDUnitTest.createServer(server);

    final int port = server.invoke(CqQueryDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    cqDUnitTest.createClient(client, port, host0);
    final String cqName = "testCQPerf_0";

    client.invoke(() -> {
      logger.info("### Create CQ. ###" + cqName);
      // Get CQ Service.
      QueryService cqService =
          getCache().getQueryService();

      // Create CQ Attributes.
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListener[] cqListeners = {new CqTimeTestListener(LogWriterUtils.getLogWriter())};
      ((CqTimeTestListener) cqListeners[0]).cqName = cqName;

      cqf.initCqListeners(cqListeners);
      CqAttributes cqa = cqf.create();

      // Create and Execute CQ.
      CqQuery cq1 = cqService.newCq(cqName, cqDUnitTest.cqs[0], cqa);
      assertThat(cq1.getState().isStopped()).describedAs("newCq() state mismatch").isTrue();
      cq1.execute();
    });

    final int size = 50;

    // Create values.
    cqDUnitTest.createValuesWithTime(client, cqDUnitTest.regions[0], size);

    Wait.pause(5000);

    // Update values
    cqDUnitTest.createValuesWithTime(client, cqDUnitTest.regions[0], size);

    client.invoke(new CacheSerializableRunnable("Validate CQs") {
      public void run2() throws CacheException {
        logger.info("### Validating CQ. ### " + cqName);
        // Get CQ Service.
        QueryService cqService = getCache().getQueryService();

        CqQuery cQuery = cqService.getCq(cqName);
        assertThat(cQuery).isNotNull();

      }
    });

    Wait.pause(10 * 60 * 1000);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);

  }

  /**
   * Test for maintaining keys for update optimization.
   */
  @Test
  public void testKeyMaintenance() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    cqDUnitTest.createServer(server);
    final int port = server.invoke(CqQueryDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    cqDUnitTest.createClient(client, port, host0);

    // Cq1
    cqDUnitTest.createCQ(client, "testKeyMaintenance_0", cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client, "testKeyMaintenance_0", false, null);

    // Cq2
    cqDUnitTest.createCQ(client, "testKeyMaintenance_1", cqDUnitTest.cqs[10]);
    cqDUnitTest.executeCQ(client, "testKeyMaintenance_1", false, null);

    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 1);
    cqDUnitTest.waitForCreated(client, "testKeyMaintenance_0", CqQueryDUnitTest.KEY + 1);

    // Entry is made into the CQs cache hashSet.
    // testKeyMaintenance_0 with 1 entry and testKeyMaintenance_1 with 0
    server.invoke(() -> {
      CqService cqService = ((DefaultQueryService) getCache().getQueryService()).getCqService();

      Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
      for (InternalCqQuery cq : cqs) {
        ServerCQImpl cqQuery = (ServerCQImpl) cq;
        String serverCqName = cqQuery.getServerCqName();
        if (serverCqName.startsWith("testKeyMaintenance_0")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_0 is wrong.")
              .isEqualTo(1);
        } else if (serverCqName.startsWith("testKeyMaintenance_1")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_1 is wrong.")
              .isEqualTo(0);
        }
      }
    });

    // Update 1.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 10);
    cqDUnitTest.waitForCreated(client, "testKeyMaintenance_0", CqQueryDUnitTest.KEY + 10);

    // Entry/check is made into the CQs cache hashSet.
    // testKeyMaintenance_0 with 1 entry and testKeyMaintenance_1 with 1
    server.invoke(() -> {
      CqService cqService = ((DefaultQueryService) getCache().getQueryService()).getCqService();

      Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
      for (InternalCqQuery cq : cqs) {
        ServerCQImpl cqQuery = (ServerCQImpl) cq;

        String serverCqName = cqQuery.getServerCqName();
        if (serverCqName.startsWith("testKeyMaintenance_0")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_0 is wrong.")
              .isEqualTo(10);
        } else if (serverCqName.startsWith("testKeyMaintenance_1")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_1 is wrong.")
              .isEqualTo(5);
        }
      }
    });

    // Update.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 12);
    cqDUnitTest.waitForCreated(client, "testKeyMaintenance_0", CqQueryDUnitTest.KEY + 12);

    // Entry/check is made into the CQs cache hashSet.
    // testKeyMaintenance_0 with 1 entry and testKeyMaintenance_1 with 1
    server.invoke(() -> {
      CqService cqService = ((DefaultQueryService) getCache().getQueryService()).getCqService();

      Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
      for (InternalCqQuery cq : cqs) {
        ServerCQImpl cqQuery = (ServerCQImpl) cq;
        String serverCqName = cqQuery.getServerCqName();
        if (serverCqName.startsWith("testKeyMaintenance_0")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_0 is wrong.")
              .isEqualTo(12);
        } else if (serverCqName.startsWith("testKeyMaintenance_1")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_1 is wrong.")
              .isEqualTo(6);
        }
      }
    });

    // Delete.
    cqDUnitTest.deleteValues(server, cqDUnitTest.regions[0], 6);
    cqDUnitTest.waitForDestroyed(client, "testKeyMaintenance_0", CqQueryDUnitTest.KEY + 6);

    // Entry/check is made into the CQs cache hashSet.
    // testKeyMaintenance_0 with 1 entry and testKeyMaintenance_1 with 1
    server.invoke(() -> {
      CqService cqService = ((DefaultQueryService) getCache().getQueryService()).getCqService();

      Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
      for (InternalCqQuery cq : cqs) {
        ServerCQImpl cqQuery = (ServerCQImpl) cq;

        String serverCqName = cqQuery.getServerCqName();
        if (serverCqName.startsWith("testKeyMaintenance_0")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_0 is wrong.")
              .isEqualTo(6);
        } else if (serverCqName.startsWith("testKeyMaintenance_1")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_1 is wrong.")
              .isEqualTo(3);
        }
      }
    });

    // Stop CQ.
    // This should still needs to process the events so that Results are up-to-date.
    cqDUnitTest.stopCQ(client, "testKeyMaintenance_1");
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 12);

    server.invoke(() -> {
      CqService cqService = ((DefaultQueryService) getCache().getQueryService()).getCqService();

      Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
      for (InternalCqQuery cq : cqs) {
        ServerCQImpl cqQuery = (ServerCQImpl) cq;
        String serverCqName = cqQuery.getServerCqName();
        if (serverCqName.startsWith("testKeyMaintenance_0")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_0 is wrong.")
              .isEqualTo(12);
        } else if (serverCqName.startsWith("testKeyMaintenance_1")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_1 is wrong.")
              .isEqualTo(6);
        }
      }
    });

    // re-start the CQ.
    cqDUnitTest.executeCQ(client, "testKeyMaintenance_1", false, null);

    // This will remove the caching for this CQ.
    cqDUnitTest.closeCQ(client, "testKeyMaintenance_1");
    server.invoke(() -> {
      CqService cqService = ((DefaultQueryService) getCache().getQueryService()).getCqService();

      Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
      for (InternalCqQuery cq : cqs) {
        ServerCQImpl cqQuery = (ServerCQImpl) cq;

        String serverCqName = cqQuery.getServerCqName();
        assertThat(serverCqName.startsWith("testKeyMaintenance_1")).isFalse();

        if (serverCqName.startsWith("testKeyMaintenance_0")) {
          assertThat(cqQuery.getCqResultKeysSize())
              .describedAs("The number of keys cached for cq testKeyMaintenance_0 is wrong.")
              .isEqualTo(12);
        }
      }
    });

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  /**
   * Test for common CQs. To test the changes relating to, executing CQ only once for all similar
   * CQs.
   */
  @Test
  public void testMatchingCqs() {

    VM server = VM.getVM(0);
    VM client = VM.getVM(1);

    cqDUnitTest.createServer(server);
    final int port = server.invoke(CqQueryDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    cqDUnitTest.createClient(client, port, host0);

    // Create and Execute same kind of CQs.
    for (int i = 0; i < 4; i++) {
      cqDUnitTest.createCQ(client, "testMatchingCqs_" + i, cqDUnitTest.cqs[0]);
      cqDUnitTest.executeCQ(client, "testMatchingCqs_" + i, false, null);
    }

    validateMatchingCqs(server, 1, cqDUnitTest.cqs[0], 4);

    int size = 1;

    // Create.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);
    cqDUnitTest.waitForCreated(client, "testMatchingCqs_0", CqQueryDUnitTest.KEY + size);
    cqDUnitTest.waitForCreated(client, "testMatchingCqs_3", CqQueryDUnitTest.KEY + size);

    // Close one of the CQ.
    cqDUnitTest.closeCQ(client, "testMatchingCqs_0");
    validateMatchingCqs(server, 1, cqDUnitTest.cqs[0], 3);

    // Update.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);
    cqDUnitTest.waitForUpdated(client, "testMatchingCqs_3", CqQueryDUnitTest.KEY + size);

    // Stop one of the CQ.
    cqDUnitTest.stopCQ(client, "testMatchingCqs_1");

    validateMatchingCqs(server, 1, cqDUnitTest.cqs[0], 2);

    // Update - 2.
    cqDUnitTest.clearCQListenerEvents(client, "testMatchingCqs_3");
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);
    cqDUnitTest.waitForUpdated(client, "testMatchingCqs_3", CqQueryDUnitTest.KEY + size);

    // stopped CQ should not receive 2nd/previous updates.
    cqDUnitTest.validateCQ(client, "testMatchingCqs_1", /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size, /* updates: once */ size, /* deletes; */ 0, /* queryInserts: */ size,
        /* queryUpdates: */ size, /* queryDeletes: */ 0, /* totalEvents: */ size * 2);

    // Execute the stopped CQ.
    cqDUnitTest.executeCQ(client, "testMatchingCqs_1", false, null);

    // Update - 3.
    cqDUnitTest.clearCQListenerEvents(client, "testMatchingCqs_3");
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);
    cqDUnitTest.waitForUpdated(client, "testMatchingCqs_3", CqQueryDUnitTest.KEY + size);

    cqDUnitTest.validateCQ(client, "testMatchingCqs_1", /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size, /* updates: 2 */ size * 2, /* deletes; */ 0, /* queryInserts: */ size,
        /* queryUpdates: */ size * 2, /* queryDeletes: */ 0, /* totalEvents: */ size * 3);

    // Create different kind of CQs.
    cqDUnitTest.createCQ(client, "testMatchingCqs_4", cqDUnitTest.cqs[1]);
    cqDUnitTest.executeCQ(client, "testMatchingCqs_4", false, null);

    cqDUnitTest.createCQ(client, "testMatchingCqs_5", cqDUnitTest.cqs[1]);
    cqDUnitTest.executeCQ(client, "testMatchingCqs_5", false, null);

    cqDUnitTest.createCQ(client, "testMatchingCqs_6", cqDUnitTest.cqs[2]);
    cqDUnitTest.executeCQ(client, "testMatchingCqs_6", false, null);

    validateMatchingCqs(server, 3, cqDUnitTest.cqs[1], 2);

    cqDUnitTest.closeCQ(client, "testMatchingCqs_6");
    validateMatchingCqs(server, 2, cqDUnitTest.cqs[1], 2);

    cqDUnitTest.closeCQ(client, "testMatchingCqs_5");
    cqDUnitTest.closeCQ(client, "testMatchingCqs_4");
    cqDUnitTest.closeCQ(client, "testMatchingCqs_3");
    cqDUnitTest.closeCQ(client, "testMatchingCqs_2");
    cqDUnitTest.closeCQ(client, "testMatchingCqs_1");

    validateMatchingCqs(server, 0, null, 0);

    // update 4
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }


  /**
   * Test for common CQs. To test the changes relating to, executing CQ only once for all similar
   * CQs.
   */
  @Test
  public void testMatchingCQWithMultipleClients() {

    VM server = VM.getVM(0);
    VM client1 = VM.getVM(1);
    VM client2 = VM.getVM(2);
    VM client3 = VM.getVM(3);

    VM clients[] = new VM[] {client1, client2, client3};

    cqDUnitTest.createServer(server);
    final int port = server.invoke(CqQueryDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    for (int clientIndex = 0; clientIndex < 3; clientIndex++) {
      cqDUnitTest.createClient(clients[clientIndex], port, host0);
      // Create and Execute same kind of CQs.
      for (int i = 0; i < 4; i++) {
        cqDUnitTest.createCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_" + i,
            cqDUnitTest.cqs[0]);
        cqDUnitTest.executeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_" + i, false,
            null);
      }
    }

    validateMatchingCqs(server, 1, cqDUnitTest.cqs[0], clients.length * 4);

    int size = 1;

    // Create.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);
    for (int clientIndex = 0; clientIndex < 3; clientIndex++) {
      cqDUnitTest.waitForCreated(clients[clientIndex], "testMatchingCQWithMultipleClients_0",
          CqQueryDUnitTest.KEY + size);
      cqDUnitTest.waitForCreated(clients[clientIndex], "testMatchingCQWithMultipleClients_3",
          CqQueryDUnitTest.KEY + size);
    }

    // Close one of the CQ.
    cqDUnitTest.closeCQ(client1, "testMatchingCQWithMultipleClients_0");
    validateMatchingCqs(server, 1, cqDUnitTest.cqs[0], (clients.length * 4) - 1);

    // Update.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    for (int clientIndex = 0; clientIndex < 3; clientIndex++) {
      cqDUnitTest.waitForUpdated(clients[clientIndex], "testMatchingCQWithMultipleClients_3",
          CqQueryDUnitTest.KEY + size);
    }

    // Stop one of the CQ.
    cqDUnitTest.stopCQ(client2, "testMatchingCQWithMultipleClients_1");

    validateMatchingCqs(server, 1, cqDUnitTest.cqs[0], (clients.length * 4) - 2);

    // Update - 2.
    for (int clientIndex = 0; clientIndex < 3; clientIndex++) {
      cqDUnitTest.clearCQListenerEvents(clients[clientIndex],
          "testMatchingCQWithMultipleClients_3");
    }

    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    for (int clientIndex = 0; clientIndex < 3; clientIndex++) {
      cqDUnitTest.waitForUpdated(clients[clientIndex], "testMatchingCQWithMultipleClients_3",
          CqQueryDUnitTest.KEY + size);
    }

    // stopped CQ should not receive 2nd/previous updates.
    cqDUnitTest.validateCQ(client2, "testMatchingCQWithMultipleClients_1",
        /* resultSize: */ CqQueryDUnitTest.noTest, /* creates: */ size, /* updates: once */ size,
        /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ size, /* queryDeletes: */ 0,
        /* totalEvents: */ size * 2);

    // Execute the stopped CQ.
    cqDUnitTest.executeCQ(client2, "testMatchingCQWithMultipleClients_1", false, null);

    // Update - 3.
    for (int clientIndex = 0; clientIndex < 3; clientIndex++) {
      cqDUnitTest.clearCQListenerEvents(clients[clientIndex],
          "testMatchingCQWithMultipleClients_3");
    }

    validateMatchingCqs(server, 1, cqDUnitTest.cqs[0], (clients.length * 4) - 1);

    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    for (int clientIndex = 0; clientIndex < 3; clientIndex++) {
      cqDUnitTest.waitForUpdated(clients[clientIndex], "testMatchingCQWithMultipleClients_3",
          CqQueryDUnitTest.KEY + size);
    }

    cqDUnitTest.validateCQ(client2, "testMatchingCQWithMultipleClients_1",
        /* resultSize: */ CqQueryDUnitTest.noTest, /* creates: */ size, /* updates: 2 */ size * 2,
        /* deletes; */ 0, /* queryInserts: */ size, /* queryUpdates: */ size * 2,
        /* queryDeletes: */ 0, /* totalEvents: */ size * 3);

    // Create different kind of CQs.
    for (int clientIndex = 0; clientIndex < 3; clientIndex++) {
      cqDUnitTest.createCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_4",
          cqDUnitTest.cqs[1]);
      cqDUnitTest.executeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_4", false,
          null);

      cqDUnitTest.createCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_5",
          cqDUnitTest.cqs[1]);
      cqDUnitTest.executeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_5", false,
          null);

      cqDUnitTest.createCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_6",
          cqDUnitTest.cqs[2]);
      cqDUnitTest.executeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_6", false,
          null);
    }

    validateMatchingCqs(server, 3, cqDUnitTest.cqs[1], 2 * clients.length);

    for (int clientIndex = 0; clientIndex < 3; clientIndex++) {
      cqDUnitTest.closeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_6");
    }

    validateMatchingCqs(server, 2, cqDUnitTest.cqs[1], 2 * clients.length);

    // update 4
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    // Close.
    cqDUnitTest.closeClient(client3);
    validateMatchingCqs(server, 2, cqDUnitTest.cqs[1], 2 * (clients.length - 1));

    for (int clientIndex = 0; clientIndex < 2; clientIndex++) {
      cqDUnitTest.closeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_5");
      cqDUnitTest.closeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_4");
      cqDUnitTest.closeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_3");
      cqDUnitTest.closeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_2");
      cqDUnitTest.closeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_1");
      if (clientIndex != 0) {
        cqDUnitTest.closeCQ(clients[clientIndex], "testMatchingCQWithMultipleClients_0");
      }
    }

    validateMatchingCqs(server, 0, null, 0);

    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeClient(client1);

    cqDUnitTest.closeServer(server);
  }

  @Test
  public void testMatchingCQsWithMultipleServers() {

    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client1 = VM.getVM(2);
    VM client2 = VM.getVM(3);

    cqDUnitTest.createServer(server1);

    VM clients[] = new VM[] {client1, client2};

    final int port1 = server1.invoke(CqQueryDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    // Create client.

    // Create client with redundancyLevel -1

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    cqDUnitTest.createClient(client1, new int[] {port1, ports[0]}, host0, "-1");
    cqDUnitTest.createClient(client2, new int[] {port1, ports[0]}, host0, "-1");

    int numCQs = 3;

    for (int i = 0; i < numCQs; i++) {
      cqDUnitTest.createCQ(client1, "testMatchingCQsWithMultipleServers_" + i, cqDUnitTest.cqs[i]);
      cqDUnitTest.executeCQ(client1, "testMatchingCQsWithMultipleServers_" + i, false, null);

      cqDUnitTest.createCQ(client2, "testMatchingCQsWithMultipleServers_" + i, cqDUnitTest.cqs[i]);
      cqDUnitTest.executeCQ(client2, "testMatchingCQsWithMultipleServers_" + i, false, null);
    }

    validateMatchingCqs(server1, numCQs, cqDUnitTest.cqs[0], clients.length);
    validateMatchingCqs(server1, numCQs, cqDUnitTest.cqs[1], clients.length);

    Wait.pause(1000);

    // CREATE.
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[0], 10);
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[1], 10);

    for (int i = 1; i <= 10; i++) {
      cqDUnitTest.waitForCreated(client1, "testMatchingCQsWithMultipleServers_0",
          CqQueryDUnitTest.KEY + i);
    }

    cqDUnitTest.createServer(server2, ports[0]);

    final int port2 = server2.invoke(CqQueryDUnitTest::getCacheServerPort);
    System.out
        .println("### Port on which server1 running : " + port1 + " Server2 running : " + port2);

    // UPDATE - 1.
    for (int k = 0; k < numCQs; k++) {
      cqDUnitTest.clearCQListenerEvents(client1, "testMatchingCQsWithMultipleServers_" + k);
      cqDUnitTest.clearCQListenerEvents(client2, "testMatchingCQsWithMultipleServers_" + k);
    }

    cqDUnitTest.createValues(server1, cqDUnitTest.regions[0], 10);
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[1], 10);

    // Wait for updates on regions[0]
    for (int i = 1; i <= 10; i++) {
      cqDUnitTest.waitForUpdated(client1, "testMatchingCQsWithMultipleServers_0",
          CqQueryDUnitTest.KEY + i);
      cqDUnitTest.waitForUpdated(client2, "testMatchingCQsWithMultipleServers_0",
          CqQueryDUnitTest.KEY + i);
    }

    // Wait for updates on regions[1] - Waiting for last key is good enough.
    cqDUnitTest.waitForUpdated(client1, "testMatchingCQsWithMultipleServers_2",
        CqQueryDUnitTest.KEY + 4);
    cqDUnitTest.waitForUpdated(client2, "testMatchingCQsWithMultipleServers_2",
        CqQueryDUnitTest.KEY + 4);

    int[] resultsCnt = new int[] {10, 1, 2};

    for (int i = 0; i < numCQs; i++) {
      cqDUnitTest.validateCQ(client1, "testMatchingCQsWithMultipleServers_" + i,
          CqQueryDUnitTest.noTest, resultsCnt[i], resultsCnt[i], CqQueryDUnitTest.noTest);

      cqDUnitTest.validateCQ(client2, "testMatchingCQsWithMultipleServers_" + i,
          CqQueryDUnitTest.noTest, resultsCnt[i], resultsCnt[i], CqQueryDUnitTest.noTest);
    }

    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeServer(server2);
  }


  @Test
  public void testFailOverMatchingCQsWithMultipleServers() {

    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client1 = VM.getVM(2);
    VM client2 = VM.getVM(3);

    logger.info("Ready to create server 1");
    cqDUnitTest.createServer(server1);
    logger.info("Ready to create server 1");

    VM clients[] = new VM[] {client1, client2};

    final int port1 = server1.invoke(CqQueryDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    // Create client.

    // Create client with redundancyLevel -1

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    cqDUnitTest.createClient(client1, new int[] {port1, ports[0]}, host0, "-1");
    cqDUnitTest.createClient(client2, new int[] {port1, ports[0]}, host0, "-1");

    int numCQs = 3;

    for (int i = 0; i < numCQs; i++) {
      cqDUnitTest.createCQ(client1, "testMatchingCQsWithMultipleServers_" + i, cqDUnitTest.cqs[i]);
      cqDUnitTest.executeCQ(client1, "testMatchingCQsWithMultipleServers_" + i, false, null);

      cqDUnitTest.createCQ(client2, "testMatchingCQsWithMultipleServers_" + i, cqDUnitTest.cqs[i]);
      cqDUnitTest.executeCQ(client2, "testMatchingCQsWithMultipleServers_" + i, false, null);
    }

    validateMatchingCqs(server1, numCQs, cqDUnitTest.cqs[0], clients.length);
    validateMatchingCqs(server1, numCQs, cqDUnitTest.cqs[1], clients.length);

    cqDUnitTest.createServer(server2, ports[0]);

    // Close server1.
    cqDUnitTest.closeServer(server1);

    validateMatchingCqs(server2, numCQs, cqDUnitTest.cqs[0], clients.length);

    // Close.
    cqDUnitTest.closeClient(client1);

    validateMatchingCqs(server2, numCQs, cqDUnitTest.cqs[0], (clients.length - 1));

    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeServer(server2);
  }


  /**
   * Test for CQ Fail over.
   */
  @Test
  public void testMatchingCQsOnDataNodeWithMultipleServers() {

    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client1 = VM.getVM(2);
    VM client2 = VM.getVM(3);

    cqDUnitTest.createServerOnly(server1, 0);
    cqDUnitTest.createServerOnly(server2, 0);
    cqDUnitTest.createPartitionRegion(server1, cqDUnitTest.regions);
    cqDUnitTest.createPartitionRegion(server2, cqDUnitTest.regions);

    VM clients[] = new VM[] {client1, client2};

    final int port1 = server1.invoke(CqQueryDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    cqDUnitTest.createLocalRegion(client1, new int[] {port1, ports[0]}, host0, "-1",
        cqDUnitTest.regions);
    cqDUnitTest.createLocalRegion(client2, new int[] {port1, ports[0]}, host0, "-1",
        cqDUnitTest.regions);

    int numCQs = cqDUnitTest.prCqs.length;

    for (int i = 0; i < numCQs; i++) {
      cqDUnitTest.createCQ(client1, "testMatchingCQsWithMultipleServers_" + i,
          cqDUnitTest.prCqs[i]);
      cqDUnitTest.executeCQ(client1, "testMatchingCQsWithMultipleServers_" + i, false, null);

      cqDUnitTest.createCQ(client2, "testMatchingCQsWithMultipleServers_" + i,
          cqDUnitTest.prCqs[i]);
      cqDUnitTest.executeCQ(client2, "testMatchingCQsWithMultipleServers_" + i, false, null);
    }

    validateMatchingCqs(server1, numCQs, cqDUnitTest.prCqs[0], clients.length);
    validateMatchingCqs(server1, numCQs, cqDUnitTest.prCqs[1], clients.length);

    validateMatchingCqs(server2, numCQs, cqDUnitTest.prCqs[0], clients.length);
    validateMatchingCqs(server2, numCQs, cqDUnitTest.prCqs[1], clients.length);

    // Close.
    cqDUnitTest.closeClient(client1);
    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeServer(server1);
    cqDUnitTest.closeServer(server2);
  }

  /**
   * Performance test for Matching CQ optimization changes.
   */
  @Ignore("perf")
  @Test
  public void testPerformanceForMatchingCQs() {

    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client1 = VM.getVM(2);
    VM client2 = VM.getVM(3);

    cqDUnitTest.createServer(server1);
    cqDUnitTest.createServer(server2);

    // VM clients[] = new VM[]{client1, client2};

    final int port1 = server1.invoke(CqQueryDUnitTest::getCacheServerPort);
    final int port2 = server2.invoke(CqQueryDUnitTest::getCacheServerPort);
    final String host0 = NetworkUtils.getServerHostName();

    // Create client.
    // final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    // Client1 connects to server1.
    cqDUnitTest.createClient(client1, new int[] {port1}, host0, "-1");

    // Client2 connects to server2.
    cqDUnitTest.createClient(client2, new int[] {port2}, host0, "-1");

    // Client1 registers matching CQs on server1.
    String[] matchingCqs = this.generateCqQueries(false);
    for (int i = 0; i < matchingCqs.length; i++) {
      cqDUnitTest.createCQ(client1, "testPerformanceForMatchingCQs_" + i, matchingCqs[i]);
      cqDUnitTest.executeCQ(client1, "testPerformanceForMatchingCQs_" + i, false, null);
    }

    // Client2 registers non-matching CQs on server2.
    matchingCqs = this.generateCqQueries(true);
    for (int i = 0; i < matchingCqs.length; i++) {
      cqDUnitTest.createCQ(client2, "testPerformanceForMatchingCQs_" + i, matchingCqs[i]);
      cqDUnitTest.executeCQ(client2, "testPerformanceForMatchingCQs_" + i, false, null);
    }

    Wait.pause(1000);

    // CREATE.
    int size = 1000;
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[0], size);
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[1], size);

    // Update couple of times;
    for (int j = 0; j < 5; j++) {
      cqDUnitTest.createValues(server1, cqDUnitTest.regions[0], size - 1);
      cqDUnitTest.createValues(server2, cqDUnitTest.regions[1], size - 1);
    }

    for (int j = 0; j < 4; j++) {
      cqDUnitTest.createValues(server2, cqDUnitTest.regions[0], size - 1);
      cqDUnitTest.createValues(server1, cqDUnitTest.regions[1], size - 1);
    }

    // Update the last key.
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[0], size);
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[1], size);

    for (int k = 1; k <= size; k++) {
      cqDUnitTest.waitForUpdated(client1, "testPerformanceForMatchingCQs_0",
          CqQueryDUnitTest.KEY + k);
    }

    Wait.pause(1000);
    printCqQueryExecutionTime(server1);
    printCqQueryExecutionTime(server2);

    // Close.
    cqDUnitTest.closeClient(client1);
    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeServer(server2);
    cqDUnitTest.closeServer(server1);

  }

  private void validateMatchingCqs(VM server, final int mapSize, final String query,
      final int numCqSize) {
    server.invoke(() -> {
      CqServiceImpl cqService =
          (CqServiceImpl) ((DefaultQueryService) getCache().getQueryService()).getCqService();

      Map matchedCqMap = cqService.getMatchingCqMap();
      await()
          .until(matchedCqMap::size, equalTo(mapSize));

      if (query != null) {
        assertThat(matchedCqMap.containsKey(query)).isTrue();

        Collection cqs = (Collection) matchedCqMap.get(query);
        await()
            .until(cqs::size, equalTo(numCqSize));
      }
    });
  }

  private void printCqQueryExecutionTime(VM server) {
    server.invoke(() -> {
      CqServiceImpl cqService =
          (CqServiceImpl) ((DefaultQueryService) getCache().getQueryService()).getCqService();

      long timeTaken = cqService.getCqServiceVsdStats().getCqQueryExecutionTime();
      logger.info("Total Time taken to Execute CQ Query :" + timeTaken);
    });
  }

  private String[] generateCqQueries(boolean uniqueQueries) {
    List<String> initQueries = new ArrayList<>();
    // From Portfolio object.
    String[] names = {"aaa", "bbb", "ccc", "ddd"};
    int nameIndex;

    // Construct few unique Queries.
    for (int i = 0; i < 3; i++) {
      for (int cnt = 0; cnt < 5; cnt++) {
        String query = cqDUnitTest.cqs[i];
        if (cnt > 0) {
          nameIndex = (cnt % names.length);
          query += " or p.names[" + nameIndex + "] = '" + names[nameIndex] + cnt + "'";
        }
        initQueries.add(query);
      }
    }

    int numMatchedQueries = 10;
    List<String> cqQueries = new ArrayList<>();
    for (String query : initQueries) {
      for (int cnt = 0; cnt < numMatchedQueries; cnt++) {
        if (uniqueQueries) {
          // Append blank string, so that query string is different but the
          // Query constraint remains same.
          query += " ";
        }
        cqQueries.add(query);
      }
    }
    String[] queries = new String[cqQueries.size()];
    cqQueries.toArray(queries);
    return queries;
  }
}
