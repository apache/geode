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
package org.apache.geode.internal.cache.wan.misc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class WANSSLDUnitTest extends WANTestBase {

  public WANSSLDUnitTest() {
    super();
  }

  @Test
  public void testSenderSSLReceiverSSL() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiverWithSSL(nyPort));

    vm4.invoke(() -> WANTestBase.createCacheWithSSL(lnPort));

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("ln"));

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
  }

  @Test
  public void testSenderNoSSLReceiverSSL() {
    IgnoredException.addIgnoredException("Unexpected IOException");
    IgnoredException.addIgnoredException("SSL Error");
    IgnoredException.addIgnoredException("Unrecognized SSL message");

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiverWithSSL(nyPort));

    vm4.invoke(() -> WANTestBase.createCache(lnPort));

    String senderId = "ln";
    vm4.invoke(
        () -> WANTestBase.createSender(senderId, 2, false, 100, 10, false, false, null, true));

    String regionName = getTestMethodName() + "_RR";
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(regionName, null, isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender(senderId));

    // Verify the sender is started
    vm4.invoke(() -> verifySenderRunningState(senderId));

    // Verify the sender is not connected
    vm4.invoke(() -> verifySenderConnectedState(senderId, false));

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(regionName, "ln", isOffHeap()));

    // Do some puts in the sender
    int numPuts = 10;
    vm4.invoke(() -> WANTestBase.doPuts(regionName, numPuts));

    // Verify the sender is still started
    vm4.invoke(() -> verifySenderRunningState(senderId));

    // Verify the sender is still not connected
    vm4.invoke(() -> verifySenderConnectedState(senderId, false));

    // Verify the sender queue size
    vm4.invoke(() -> testQueueSize(senderId, numPuts));

    // Stop the receiver
    vm2.invoke(() -> closeCache());
    vm2.invoke(() -> closeSocketCreatorFactory());

    // Restart the receiver with SSL disabled
    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> createReplicatedRegion(regionName, null, isOffHeap()));
    vm2.invoke(() -> createReceiver());

    // Wait for the queue to drain
    vm4.invoke(() -> checkQueueSize(senderId, 0));

    // Verify region size on receiver
    vm2.invoke(() -> validateRegionSize(regionName, numPuts));
  }

  @Test
  public void testSenderSSLReceiverNoSSL() {
    IgnoredException.addIgnoredException("Acceptor received unknown");
    IgnoredException.addIgnoredException("failed accepting client");
    IgnoredException.addIgnoredException("Error in connecting to peer");
    IgnoredException.addIgnoredException("Remote host closed connection during handshake");
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    vm4.invoke(() -> WANTestBase.createCacheWithSSL(lnPort));

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("ln"));

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1));

    Boolean doesSizeMatch = (Boolean) vm2
        .invoke(() -> WANSSLDUnitTest.ValidateSSLRegionSize(getTestMethodName() + "_RR", 1));

    assertFalse(doesSizeMatch);
  }

  public static boolean ValidateSSLRegionSize(String regionName, final int regionSize) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    Wait.pause(2000);

    if (r.size() == regionSize) {
      return true;
    }
    return false;
  }

  private void closeSocketCreatorFactory() {
    SocketCreatorFactory.close();
  }
}
