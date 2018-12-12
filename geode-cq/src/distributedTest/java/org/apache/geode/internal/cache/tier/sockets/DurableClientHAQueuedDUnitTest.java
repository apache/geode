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

import static org.assertj.core.api.Assertions.fail;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;


@Category({ClientSubscriptionTest.class})
public class DurableClientHAQueuedDUnitTest extends DurableClientTestBase {

  /**
   * Tests the ha queued events stat Connects a durable client, registers durable cqs and then shuts
   * down the durable client Publisher then does puts onto the server Events are queued up and the
   * stats are checked Durable client is then reconnected, events are dispatched and stats are
   * rechecked
   */
  @Test
  public void testHAQueueSizeStat() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    final String durableClientId = getName() + "_client";

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

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkHAQueueSize(server1VM, durableClientId, 10, 11);

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

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);
    checkHAQueueSize(server1VM, durableClientId, 0, 1);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests the ha queued events stat Connects a durable client, registers durable cqs and then shuts
   * down the durable client Publisher then does puts onto the server Events are queued up and the
   * stats are checked Test sleeps until durable client times out Stats should now be 0 Durable
   * client is then reconnected, no events should exist and stats are rechecked
   */
  @Test
  public void testHAQueueSizeStatExpired() {
    int timeoutInSeconds = 20;
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    final String durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, server1Port, regionName, timeoutInSeconds);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(timeoutInSeconds, durableClientId,
        server1VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkHAQueueSize(server1VM, durableClientId, 10, 11);

    // pause until timeout
    try {
      Thread.sleep((timeoutInSeconds + 2) * 1000);
    } catch (InterruptedException ie) {
      fail("interrupted", ie);
    }
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
    checkHAQueueSize(server1VM, durableClientId, 0, 1);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests the ha queued events stat Starts up two servers, shuts one down Connects a durable
   * client, registers durable cqs and then shuts down the durable client Publisher then does puts
   * onto the server Events are queued up Durable client is then reconnected but does not send ready
   * for events Secondary server is brought back up Stats are checked Durable client then
   * reregisters cqs and sends ready for events
   */
  @Test
  public void testHAQueueSizeStatForGII() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // shut down server 2
    closeCache(server2VM);

    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);
    checkNumDurableCqs(server1VM, durableClientId, 3);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // Re-start server2, at this point it will be the first time server2 has connected to client
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        server2Port));

    // Verify durable client on server2
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server2VM);

    // verify cqs and stats on server 2. These events are through gii, stats should be correct
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkHAQueueSize(server2VM, durableClientId, 10, 11);

    closeCache(server1VM);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // verify cq listeners received events
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);
    checkHAQueueSize(server2VM, durableClientId, 0, 1);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests the ha queued cq stat
   */
  @Test
  public void testHAQueuedCqStat() {
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

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

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

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    // This can cause events to linger in the queue due to a "later" ack and only cleared on
    // the next dispatch. We need to send one more message to dispatch, that calls remove one more
    // time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);

    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  @Test
  public void testHAQueuedCqStatOnSecondary() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server 2
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server2VM);

    // Verify durable client on server
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    // verify cq stats are correct on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

    // verify cq stats are correct
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Verify stats are 0 for both servers
    flushEntries(server1VM, durableClientVM, regionName);

    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Server 2 comes up, client connects and registers cqs, server 2 then disconnects events are put
   * into region client goes away server 2 comes back up and should get a gii check stats server 1
   * goes away client comes back and receives all events stats should still be correct
   */
  @Test
  public void testHAQueuedCqStatForGII() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on both servers
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server2VM);
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // verify durable cqs on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkNumDurableCqs(server2VM, durableClientId, 3);

    // shutdown server 2
    closeCache(server2VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    // Re-start server2, should get events through gii
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        server2Port));

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // verify cq stats are correct on server 2
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    closeCache(server1VM);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable clients
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Start both servers, but shut down secondary server before durable client has connected. Connect
   * durable client to primary, register cqs and then shutdown durable client Publish events,
   * reconnect durable client but do not send ready for events Restart secondary and check stats to
   * be sure cqs have correct stats due to GII Shutdown primary and fail over to secondary Durable
   * Client sends ready or events and receives events Recheck stats
   */
  @Test
  public void testHAQueuedCqStatForGII2() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // shut down server 2
    closeCache(server2VM);

    durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);
    checkNumDurableCqs(server1VM, durableClientId, 3);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // Re-start server2, at this point it will be the first time server2 has connected to client
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        server2Port));

    // Verify durable client on server2
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server2VM);

    // verify cqs and stats on server 2. These events are through gii, stats should be correct
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    closeCache(server1VM);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Server 2 comes up and goes down after client connects and registers cqs events are put into
   * region client goes away server 2 comes back up and should get a gii check stats client comes
   * back and receives all events stats should still be correct
   */
  @Test
  public void testHAQueuedCqStatForGIINoFailover() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on both servers
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server2VM);
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // verify durable cqs on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkNumDurableCqs(server2VM, durableClientId, 3);

    // shutdown server 2
    closeCache(server2VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    // Re-start server2, should get events through gii
    this.server2VM.invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE,
        server2Port));

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // verify cq stats are correct on server 2
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);

    // Stop the durable clients
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the servers
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * server 1 and 2 both get events server 1 goes down dc reconnects server 2 behaves accordingly
   */
  @Test
  public void testHAQueuedCqStatForFailover() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start server 2 using the same mcast port as server 1
    final int server2Port = this.server2VM
        .invoke(() -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil::disableShufflingOfEndpoints);

    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on both servers
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server2VM);
    verifyDurableClientPresent(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, durableClientId,
        server1VM);

    // verify durable cqs on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkNumDurableCqs(server2VM, durableClientId, 3);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    closeCache(server1VM);

    // verify cq stats are correct on server 2
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);

    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, server1Port, server2Port, regionName);

    // Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5",
        true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5",
        true);
    // send client ready
    sendClientReady(durableClientVM);

    // verify listeners on client
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);
    checkCqListenerEvents(durableClientVM, "All", 10 /* numEventsExpected */,
        /* numEventsToWaitFor */ 15/* secondsToWait */);

    // Verify stats are 0 for both servers
    flushEntries(server2VM, durableClientVM, regionName);

    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server2VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

  /**
   * Tests the ha queued cq stat
   */
  @Test
  public void testHAQueuedCqStatWithTimeOut() {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";

    int timeoutInSeconds = 20;
    // Start server 1
    server1Port = this.server1VM.invoke(
        () -> CacheServerTestUtil.createCacheServer(regionName, Boolean.TRUE));

    final String durableClientId = getName() + "_client";

    startDurableClient(durableClientVM, durableClientId, server1Port, regionName, timeoutInSeconds);
    // register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    // send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientPresent(timeoutInSeconds, durableClientId,
        server1VM);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, server1Port, regionName);

    // Publish some entries
    publishEntries(regionName, 10);

    // verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

    // Pause for timeout
    try {
      Thread.sleep((timeoutInSeconds + 5) * 1000);
    } catch (InterruptedException e) {
      fail("interrupted", e);
    }

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

    // Make sure all events are expired and are not sent
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

    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the publisher client
    this.publisherClientVM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);

    // Stop the server
    this.server1VM.invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }

}
