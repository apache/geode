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
package org.apache.geode.internal.cache.tier.sockets;


import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ConnectionStats;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class DurableClientCQDUnitTest extends DurableClientTestBase {

  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server
   */
  @Test
  public void testCloseCqAndDrainEvents() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start a server
    server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    durableClientId = getName() + "_client";
    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    this.server1VM.invoke(new CacheSerializableRunnable("Close cq for durable client") {
      @Override
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
        } catch (CqException e) {
          fail("failed", e);
        }
      }
    });

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);

    // Re-register durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // verify cq events for all 3 cqs
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server This
   * draining should not affect events that still have register interest
   */
  @Test
  public void testCloseAllCqsAndDrainEvents() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    final String durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);
    // register durable cqs
    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    closeCQsforDurableClient(durableClientId);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);

    // Reregister durable cqs
    registerInterest(durableClientVM, regionName, true, InterestResultPolicy.NONE);
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);
    checkListenerEvents(10, 1, -1, this.durableClientVM);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server This
   * draining should remove all events due to no interest registered Continues to publish afterwards
   * to verify that stats are correct
   */
  @Test
  public void testCloseAllCqsAndDrainEventsNoInterestRegistered() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    closeCQsforDurableClient(durableClientId);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);
    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);

    // the flush entry message may remain in the queue due
    // verify the queue stats are as close/correct as possible
    this.checkHAQueueSize(server1VM, durableClientId, 0, 1);

    // continue to publish and make sure we get the events
    publishEntries(regionName, 10);
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 10/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 10/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 10/* secondsToWait */);

    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);

    // the flush entry message may remain in the queue due
    // verify the queue stats are as close/correct as possible
    this.checkHAQueueSize(server1VM, durableClientId, 0, 1);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  private void closeCQsforDurableClient(String durableClientId) {
    this.server1VM.invoke(new CacheSerializableRunnable("Close cq for durable client") {
      @Override
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
          ccnInstance.closeClientCq(durableClientId, "GreaterThan5");
          ccnInstance.closeClientCq(durableClientId, "LessThan5");
        } catch (CqException e) {
          fail("failed", e);
        }
      }
    });
  }

  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server Two
   * durable clients, one will have a cq be closed, the other should be unaffected
   */
  @Test
  public void testCloseCqAndDrainEvents2Client() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    final String durableClientId = getName() + "_client";
    final String durableClientId2 = getName() + "_client2";
    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    startDurableClient(durableClientVM, durableClientId2, server1Port, regionName);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify 2nd durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify 2nd durable client") {
      @Override
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(2);
      }
    });

    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    this.server1VM.invoke(new CacheSerializableRunnable("Close cq for durable client 1") {
      @Override
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
        } catch (CqException e) {
          fail("failed", e);
        }
      }
    });

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // verify cq events for all 3 cqs, where ALL should have 0 entries
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
        /* numEventsToWaitFor */ 5/* secondsToWait */);

    this.disconnectDurableClient(false);

    // Restart the 2nd durable client
    startDurableClient(durableClientVM, durableClientId2, server1Port, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // verify cq events for all 3 cqs, where ALL should have 10 entries
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests situation where a client is trying to reconnect while a cq is being drained. The client
   * should be rejected until no cqs are currently being drained
   */
  @Test
  public void testRejectClientWhenDrainingCq() {
    try {
      IgnoredException.addIgnoredException(
          "CacheClientNotifier: Connection refused due to cq queue being drained from admin command, please wait...");
      IgnoredException.addIgnoredException(
          "Could not initialize a primary queue on startup. No queue servers available.");

      String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
      String allQuery = "select * from /" + regionName + " p where p.ID > -1";
      String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

      // Start server 1
      server1Port = this.server1VM.invoke(
          () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

      final String durableClientId = getName() + "_client";
      this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

      startDurableClient(durableClientVM, durableClientId, server1Port, regionName);

      // register durable cqs
      createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
      createCq(durableClientVM, "All", allQuery, true);
      createCq(durableClientVM, "LessThan5", lessThan5Query, true);
      // send client ready
      sendClientReady(durableClientVM);

      verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
          server1VM);

      // Stop the durable client
      this.disconnectDurableClient(true);

      // Start normal publisher client
      startClient(publisherClientVM, server1Port, regionName);

      // Publish some entries
      publishEntries(regionName, 10);

      this.server1VM.invoke(new CacheSerializableRunnable("Set test hook") {
        @Override
        public void run2() throws CacheException {
          // Set the Test Hook!
          // This test hook will pause during the drain process
          CacheClientProxy.testHook = new RejectClientReconnectTestHook();
        }
      });

      this.server1VM.invokeAsync(new CacheSerializableRunnable("Close cq for durable client") {
        @Override
        public void run2() throws CacheException {

          final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();

          try {
            ccnInstance.closeClientCq(durableClientId, "All");
          } catch (CqException e) {
            fail("failed", e);
          }
        }
      });

      // Restart the durable client
      startDurableClient(durableClientVM, durableClientId, server1Port, regionName);

      this.server1VM.invoke(new CacheSerializableRunnable("verify was rejected at least once") {
        @Override
        public void run2() throws CacheException {
          await()
              .until(() -> CacheClientProxy.testHook != null
                  && (((RejectClientReconnectTestHook) CacheClientProxy.testHook)
                      .wasClientRejected()));
          assertTrue(
              ((RejectClientReconnectTestHook) CacheClientProxy.testHook).wasClientRejected());
        }
      });

      checkPrimaryUpdater(durableClientVM);

      // After rejection, the client will retry and eventually connect
      // Verify durable client on server2
      verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
          server1VM);

      createCq(durableClientVM, "GreaterThan5",
          "select * from /" + regionName + " p where p.ID > 5", true);
      createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
      createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
          true);
      // send client ready
      sendClientReady(durableClientVM);

      checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
          /* numEventsToWaitFor */ 15/* secondsToWait */);
      checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
          /* numEventsToWaitFor */ 15/* secondsToWait */);
      checkCqListenerEvents(durableClientVM, "All", 0 /* numEventsExpected */,
          /* numEventsToWaitFor */ 5/* secondsToWait */);

      // Stop the durable client
      this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the publisher client
      this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the server
      this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    } finally {
      this.server1VM.invoke(new CacheSerializableRunnable("unset test hook") {
        @Override
        public void run2() throws CacheException {
          CacheClientProxy.testHook = null;
        }
      });
    }
  }


  /**
   * Tests scenario where close cq will throw an exception due to a client being reactivated
   */
  @Test
  public void testCqCloseExceptionDueToActivatingClient() throws Exception {
    try {
      String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
      String allQuery = "select * from /" + regionName + " p where p.ID > -1";
      String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

      // Start server 1
      server1Port = this.server1VM.invoke(
          () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

      durableClientId = getName() + "_client";

      startDurableClient(durableClientVM, durableClientId, server1Port, regionName);
      // register durable cqs
      createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
      createCq(durableClientVM, "All", allQuery, true);
      createCq(durableClientVM, "LessThan5", lessThan5Query, true);
      // send client ready
      sendClientReady(durableClientVM);

      // Verify durable client on server
      verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
          server1VM);

      // Stop the durable client
      this.disconnectDurableClient(true);

      // Start normal publisher client
      startClient(publisherClientVM, server1Port, regionName);

      // Publish some entries
      publishEntries(regionName, 10);

      AsyncInvocation async =
          this.server1VM.invokeAsync(new CacheSerializableRunnable("Close cq for durable client") {
            @Override
            public void run2() throws CacheException {

              // Set the Test Hook!
              // This test hook will pause during the drain process
              CacheClientProxy.testHook = new CqExceptionDueToActivatingClientTestHook();

              final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
              final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);
              ClientProxyMembershipID proxyId = clientProxy.getProxyID();

              try {
                ccnInstance.closeClientCq(durableClientId, "All");
                fail("Should have thrown an exception due to activating client");
              } catch (CqException e) {
                String expected =
                    String.format(
                        "CacheClientProxy: Could not drain cq %s due to client proxy id %s reconnecting.",
                        "All", proxyId.getDurableId());
                if (!e.getMessage().equals(expected)) {
                  fail("Not the expected exception, was expecting "
                      + expected
                      + " instead of exception: " + e.getMessage());
                }
              }
            }
          });

      // Restart the durable client
      startDurableClient(durableClientVM, durableClientId, server1Port, regionName);

      // Reregister durable cqs
      createCq(durableClientVM, "GreaterThan5",
          "select * from /" + regionName + " p where p.ID > 5", true);
      createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
      createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
          true);
      // send client ready
      sendClientReady(durableClientVM);

      async.join();
      assertFalse(async.getException() != null ? async.getException().toString() : "No error",
          async.exceptionOccurred());

      // verify cq listener events
      checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
          /* numEventsToWaitFor */ 15/* secondsToWait */);
      checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
          /* numEventsToWaitFor */ 15/* secondsToWait */);
      checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
          /* numEventsToWaitFor */ 15/* secondsToWait */);

      // Stop the durable client
      this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the publisher client
      this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the server
      this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    } finally {
      this.server1VM.invoke(new CacheSerializableRunnable("unset test hook") {
        @Override
        public void run2() throws CacheException {
          CacheClientProxy.testHook = null;
        }
      });
    }
  }

  /**
   * Tests situation where a client is trying to reconnect while a cq is being drained
   */
  @Test
  public void testCqCloseExceptionDueToActiveConnection() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start a server
    server1Port = this.server1VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    durableClientId = getName() + "_client";
    startDurableClient(durableClientVM, durableClientId, server1Port, regionName);
    sendClientReady(durableClientVM);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);

    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    // Attempt to close a cq even though the client is running
    this.server1VM.invoke(new CacheSerializableRunnable("Close cq for durable client") {
      @Override
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
        final CacheClientProxy clientProxy = ccnInstance.getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
          fail(
              "expected a cq exception.  We have an active client proxy, the close cq command should have failed");
        } catch (CqException e) {
          // expected exception;
          String expected =
              String.format(
                  "CacheClientProxy: Could not drain cq %s because client proxy id %s is connected.",
                  "All", proxyId.getDurableId());
          if (!e.getMessage().equals(expected)) {
            fail("Not the expected exception, was expecting "
                + expected + " instead of exception: "
                + e.getMessage(),
                e);
          }
        }
      }
    });

    // verify cq events for all 3 cqs
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15 * 4/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  @Test
  public void testGetAllDurableCqsFromServer() {
    // Start server 1
    server1Port = this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, Boolean.TRUE});

    // Start server 2
    final int server2Port = this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, Boolean.TRUE});

    // Start a durable client
    durableClientId = getName() + "_client";
    this.durableClientVM.invoke(() -> {
      CacheServerTestUtil
          .createCacheClient(getClientPool(getServerHostName(), server1Port, server2Port,
              true, 0),
              regionName, getClientDistributedSystemProperties(durableClientId, 60), Boolean.TRUE);

    });

    // Send clientReady message
    sendClientReady(this.durableClientVM);

    // Register durable CQ
    String cqName = getName() + "_cq";
    registerDurableCq(cqName);

    // Execute getAllDurableCqsFromServer on the client
    List<String> durableCqNames =
        this.durableClientVM.invoke(DurableClientCQDUnitTest::getAllDurableCqsFromServer);

    this.durableClientVM.invoke(() -> verifyDurableCqs(durableCqNames, cqName));

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  private static List<String> getAllDurableCqsFromServer() throws CqException {
    QueryService queryService = CacheServerTestUtil.getPool().getQueryService();
    return queryService.getAllDurableCqsFromServer();
  }

  private static void verifyDurableCqs(final List<String> durableCqNames,
      final String registeredCqName) {
    // Verify the number of durable CQ names is one, and it matches the registered name
    assertEquals(1, durableCqNames.size());
    String returnedCqName = durableCqNames.get(0);
    assertEquals(registeredCqName, returnedCqName);

    // Get client's primary server
    PoolImpl pool = CacheServerTestUtil.getPool();
    ServerLocation primaryServerLocation = pool.getPrimary();

    // Verify the primary server was used and no other server was used
    Map<ServerLocation, ConnectionStats> statistics = pool.getEndpointManager().getAllStats();
    for (Map.Entry<ServerLocation, ConnectionStats> entry : statistics.entrySet()) {
      int expectedGetDurableCqInvocations = entry.getKey().equals(primaryServerLocation) ? 1 : 0;
      assertEquals(expectedGetDurableCqInvocations, entry.getValue().getGetDurableCqs());
    }
  }

  /**
   * This test method is disabled because it is failing periodically and causing cruise control
   * failures See bug #47060 (test seems to be enabled now!)
   */
  @Test
  public void testReadyForEventsNotCalledImplicitlyWithCacheXML() throws InterruptedException {
    try {
      setPeriodicACKObserver(durableClientVM);
      final String cqName = "cqTest";
      // Start a server
      server1Port =
          this.server1VM.invoke(() -> CacheServerTestUtil.createCacheServerFromXml(
              DurableClientTestBase.class.getResource("durablecq-server-cache.xml")));

      // Start a durable client that is not kept alive on the server when it
      // stops normally
      final String durableClientId = getName() + "_client";

      // create client cache from xml
      this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClientFromXml(
          DurableClientTestBase.class.getResource("durablecq-client-cache.xml"), "client",
          durableClientId, VERY_LONG_DURABLE_TIMEOUT_SECONDS, Boolean.FALSE));

      // verify that readyForEvents has not yet been called on all the client's pools
      this.durableClientVM.invoke(new CacheSerializableRunnable("check readyForEvents not called") {
        @Override
        public void run2() throws CacheException {
          for (Pool p : PoolManager.getAll().values()) {
            assertFalse(((PoolImpl) p).getReadyForEventsCalled());
          }
        }
      });

      // Send clientReady message
      sendClientReady(durableClientVM);
      registerDurableCq(cqName);

      // Verify durable client on server1
      verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1VM);

      // Start normal publisher client
      this.publisherClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
          getClientPool(getServerHostName(), server1Port, false),
          regionName));

      // Publish some entries
      final int numberOfEntries = 10;
      publishEntries(0, numberOfEntries);

      // Verify the durable client received the updates
      checkCqListenerEvents(this.durableClientVM, cqName, numberOfEntries,
          VERY_LONG_DURABLE_TIMEOUT_SECONDS);

      Thread.sleep(10000);

      // Stop the durable client
      this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache(Boolean.TRUE));

      // Verify the durable client still exists on the server
      verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId,
          server1VM);

      // Publish some more entries
      publishEntries(10, numberOfEntries);

      this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Re-start the durable client
      this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClientFromXml(
          DurableClientTestBase.class.getResource("durablecq-client-cache.xml"), "client",
          durableClientId, VERY_LONG_DURABLE_TIMEOUT_SECONDS, Boolean.FALSE));

      // Durable client registers durable cq on server
      registerDurableCq(cqName);

      // Send clientReady message
      sendClientReady(this.durableClientVM);

      // Verify durable client on server
      verifyDurableClientPresent(VERY_LONG_DURABLE_TIMEOUT_SECONDS, durableClientId, server1VM);

      // Verify the durable client received the updates held for it on the server
      checkCqListenerEvents(this.durableClientVM, cqName, 10, VERY_LONG_DURABLE_TIMEOUT_SECONDS);

      // Stop the durable client
      this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

      // Stop the server
      this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    } finally {
      unsetPeriodicACKObserver(durableClientVM);
    }
  }

  private void setPeriodicACKObserver(VM vm) {
    CacheSerializableRunnable cacheSerializableRunnable =
        new CacheSerializableRunnable("Set ClientServerObserver") {
          @Override
          public void run2() throws CacheException {
            PoolImpl.BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG = true;
            ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
              @Override
              public void beforeSendingClientAck() {
                // logger.info("beforeSendingClientAck invoked");

              }
            });

          }
        };
    vm.invoke(cacheSerializableRunnable);
  }

  private void unsetPeriodicACKObserver(VM vm) {
    CacheSerializableRunnable cacheSerializableRunnable =
        new CacheSerializableRunnable("Unset ClientServerObserver") {
          @Override
          public void run2() throws CacheException {
            PoolImpl.BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG = false;
          }
        };
    vm.invoke(cacheSerializableRunnable);
  }

  void registerDurableCq(final String cqName) {
    // Durable client registers durable cq on server
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register Cq") {
      @Override
      public void run2() throws CacheException {
        // Get the region
        Region<Object, Object> region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Create CQ Attributes.
        CqAttributesFactory cqAf = new CqAttributesFactory();

        // Initialize and set CqListener.
        CqListener[] cqListeners = {new CacheServerTestUtil.ControlCqListener()};
        cqAf.initCqListeners(cqListeners);
        CqAttributes cqa = cqAf.create();

        // Create cq's
        // Get the query service for the Pool
        QueryService queryService = CacheServerTestUtil.getPool().getQueryService();

        try {
          CqQuery query = queryService.newCq(cqName, "Select * from /" + regionName, cqa, true);
          query.execute();
        } catch (CqExistsException | CqException e) {
          fail("Failed due to ", e);
        } catch (RegionNotFoundException e) {
          fail("Could not find specified region:" + regionName + ":", e);
        }
      }
    });
  }

}
