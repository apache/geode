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
package org.apache.geode.management;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;

/**
 * Tests for WAN artifacts like Sender and Receiver. The purpose of this test is not to check WAN
 * functionality , but to verify ManagementServices running properly and reflecting WAN behaviour
 * and data properly
 *
 *
 */
@Category({WanTest.class})
public class WANManagementDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public WANManagementDUnitTest() throws Exception {
    super();
  }

  @Test
  public void testMBeanCallback() throws Exception {

    VM nyLocator = getManagedNodeList().get(0);
    VM nyReceiver = getManagedNodeList().get(1);
    VM puneSender = getManagedNodeList().get(2);
    VM managing = getManagingNode();
    VM puneLocator = Host.getLocator();

    int dsIdPort = puneLocator.invoke(() -> WANManagementDUnitTest.getLocatorPort());

    Integer nyPort = nyLocator.invoke(() -> WANTestBase.createFirstRemoteLocator(12, dsIdPort));

    puneSender.invoke(() -> WANTestBase.createCache(dsIdPort));
    managing.invoke(() -> WANTestBase.createManagementCache(dsIdPort));
    startManagingNode(managing);

    // keep a larger batch to minimize number of exception occurrences in the
    // log
    puneSender
        .invoke(() -> WANTestBase.createSender("pn", 12, true, 100, 300, false, false, null, true));
    managing
        .invoke(() -> WANTestBase.createSender("pn", 12, true, 100, 300, false, false, null, true));


    puneSender.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "pn",
        1, 100, false));
    managing.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "pn", 1,
        100, false));


    nyReceiver.invoke(() -> WANTestBase.createCache(nyPort));
    nyReceiver.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null,
        1, 100, false));
    nyReceiver.invoke(() -> WANTestBase.createReceiver());

    WANTestBase.startSenderInVMs("pn", puneSender, managing);

    // make sure all the senders are running before doing any puts
    puneSender.invoke(() -> WANTestBase.waitForSenderRunningState("pn"));
    managing.invoke(() -> WANTestBase.waitForSenderRunningState("pn"));

    WANTestBase.checkSenderMBean(puneSender, getTestMethodName() + "_PR", true);
    WANTestBase.checkSenderMBean(managing, getTestMethodName() + "_PR", true);

    WANTestBase.checkReceiverMBean(nyReceiver);

    WANTestBase.stopGatewaySender(puneSender);
    WANTestBase.startGatewaySender(puneSender);

    DistributedMember puneMember = puneSender.invoke(() -> WANTestBase.getMember());

    WANTestBase.checkProxySender(managing, puneMember);
    WANTestBase.checkSenderNavigationAPIS(managing, puneMember);

    nyReceiver.invoke(() -> WANTestBase.stopReceivers());

    WANTestBase.checkSenderMBean(puneSender, getTestMethodName() + "_PR", false);
    WANTestBase.checkSenderMBean(managing, getTestMethodName() + "_PR", false);
  }

  @Category({WanTest.class})
  @Test
  public void testReceiverMBean() throws Exception {

    VM nyLocator = getManagedNodeList().get(0);
    VM nyReceiver = getManagedNodeList().get(1);
    VM puneSender = getManagedNodeList().get(2);
    VM managing = getManagingNode();
    VM puneLocator = Host.getLocator();

    int dsIdPort = puneLocator.invoke(() -> WANManagementDUnitTest.getLocatorPort());

    Integer nyPort = nyLocator.invoke(() -> WANTestBase.createFirstRemoteLocator(12, dsIdPort));

    puneSender.invoke(() -> WANTestBase.createCache(dsIdPort));

    nyReceiver.invoke(() -> WANTestBase.createCache(nyPort));
    nyReceiver.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null,
        1, 100, false));
    nyReceiver.invoke(() -> WANTestBase.createReceiver());

    // keep a larger batch to minimize number of exception occurrences in the
    // log
    puneSender
        .invoke(() -> WANTestBase.createSender("pn", 12, true, 100, 300, false, false, null, true));

    puneSender.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "pn",
        1, 100, false));

    puneSender.invoke(() -> WANTestBase.startSender("pn"));

    // make sure all the senders are running before doing any puts
    puneSender.invoke(() -> WANTestBase.waitForSenderRunningState("pn"));

    managing.invoke(() -> WANTestBase.createManagementCache(nyPort));
    startManagingNode(managing);

    WANTestBase.checkSenderMBean(puneSender, getTestMethodName() + "_PR", true);
    WANTestBase.checkReceiverMBean(nyReceiver);

    DistributedMember nyMember = nyReceiver.invoke(() -> WANTestBase.getMember());

    WANTestBase.checkProxyReceiver(managing, nyMember);
    WANTestBase.checkReceiverNavigationAPIS(managing, nyMember);
  }


  @Test
  public void testAsyncEventQueue() throws Exception {

    VM nyLocator = getManagedNodeList().get(0);
    VM nyReceiver = getManagedNodeList().get(1);
    VM puneSender = getManagedNodeList().get(2);
    VM managing = getManagingNode();
    VM puneLocator = Host.getLocator();

    int dsIdPort = puneLocator.invoke(() -> WANManagementDUnitTest.getLocatorPort());

    Integer nyPort = nyLocator.invoke(() -> WANTestBase.createFirstRemoteLocator(12, dsIdPort));

    puneSender.invoke(() -> WANTestBase.createCache(dsIdPort));
    managing.invoke(() -> WANTestBase.createManagementCache(dsIdPort));
    startManagingNode(managing);

    puneSender.invoke(() -> WANTestBase.createAsyncEventQueue("pn", false, 100, 100, false, false,
        "puneSender", false));
    managing.invoke(() -> WANTestBase.createAsyncEventQueue("pn", false, 100, 100, false, false,
        "managing", false));

    puneSender.invoke(() -> WANTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "pn", false));
    managing.invoke(() -> WANTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "pn", false));

    WANTestBase.createCacheInVMs(nyPort, nyReceiver);
    nyReceiver.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null,
        1, 100, false));
    nyReceiver.invoke(() -> WANTestBase.createReceiver());

    WANTestBase.checkAsyncQueueMBean(puneSender, true);
    WANTestBase.checkAsyncQueueMBean(managing, true);

    DistributedMember puneMember = puneSender.invoke(() -> WANTestBase.getMember());

    WANTestBase.checkProxyAsyncQueue(managing, puneMember, true);

  }

  @Test
  public void testCreateDestroyAsyncEventQueue() throws Exception {
    VM memberVM = getManagedNodeList().get(2);
    VM managerVm = getManagingNode();
    VM locatorVm = Host.getLocator();

    int locatorPort = locatorVm.invoke(() -> WANManagementDUnitTest.getLocatorPort());

    memberVM.invoke(() -> WANTestBase.createCache(locatorPort));
    managerVm.invoke(() -> WANTestBase.createManagementCache(locatorPort));
    startManagingNode(managerVm);

    // Create AsyncEventQueue
    String aeqId = "pn";
    memberVM.invoke(
        () -> WANTestBase.createAsyncEventQueue(aeqId, false, 100, 100, false, false, null, false));
    managerVm.invoke(
        () -> WANTestBase.createAsyncEventQueue(aeqId, false, 100, 100, false, false, null, false));

    // Verify AsyncEventQueueMXBean exists
    WANTestBase.checkAsyncQueueMBean(memberVM, true);
    WANTestBase.checkAsyncQueueMBean(managerVm, true);
    DistributedMember member = memberVM.invoke(() -> WANTestBase.getMember());
    WANTestBase.checkProxyAsyncQueue(managerVm, member, true);

    // Destroy AsyncEventQueue
    memberVM.invoke(() -> WANTestBase.destroyAsyncEventQueue(aeqId));
    managerVm.invoke(() -> WANTestBase.destroyAsyncEventQueue(aeqId));

    // Verify AsyncEventQueueMXBean no longer exists
    WANTestBase.checkAsyncQueueMBean(memberVM, false);
    WANTestBase.checkAsyncQueueMBean(managerVm, false);
    WANTestBase.checkProxyAsyncQueue(managerVm, member, false);
  }

  @Test
  public void testDistributedRegionMBeanHasGatewaySenderIds() {
    VM locator = Host.getLocator();
    VM managing = getManagingNode();
    VM sender = getManagedNodeList().get(0);

    int dsIdPort = locator.invoke(() -> WANManagementDUnitTest.getLocatorPort());

    sender.invoke(() -> WANTestBase.createCache(dsIdPort));
    managing.invoke(() -> WANTestBase.createManagementCache(dsIdPort));
    startManagingNode(managing);

    sender
        .invoke(() -> WANTestBase.createSender("pn", 12, true, 100, 300, false, false, null, true));

    String regionName = getTestMethodName() + "_PR";
    sender.invoke(() -> WANTestBase.createPartitionedRegion(regionName, "pn", 0, 13, false));

    String regionPath = "/" + regionName;
    managing.invoke(() -> {
      ManagementService service = WANTestBase.getManagementService();
      await()
          .untilAsserted(() -> assertNotNull(service.getDistributedRegionMXBean(regionPath)));

      DistributedRegionMXBean bean = service.getDistributedRegionMXBean(regionPath);
      assertThat(bean.listRegionAttributes().getGatewaySenderIds()).containsExactly("pn");
    });
  }

  @Category({WanTest.class})
  @Test
  public void testMBeanCallbackInRemoteCluster() throws Exception {

    VM nyLocator = getManagedNodeList().get(0);
    VM nyReceiver = getManagedNodeList().get(1);
    VM puneSender = getManagedNodeList().get(2);
    VM managing = getManagingNode();
    VM puneLocator = Host.getLocator();

    int dsIdPort = puneLocator.invoke(() -> WANManagementDUnitTest.getLocatorPort());

    Integer nyPort = nyLocator.invoke(() -> WANTestBase.createFirstRemoteLocator(12, dsIdPort));

    puneSender.invoke(() -> WANTestBase.createCache(dsIdPort));
    managing.invoke(() -> WANTestBase.createManagementCache(dsIdPort));
    startManagingNode(managing);

    puneSender
        .invoke(() -> WANTestBase.createSender("pn", 12, true, 100, 300, false, false, null, true));
    managing
        .invoke(() -> WANTestBase.createSender("pn", 12, true, 100, 300, false, false, null, true));

    puneSender.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "pn",
        1, 100, false));
    managing.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "pn", 1,
        100, false));

    WANTestBase.createCacheInVMs(nyPort, nyReceiver);
    nyReceiver.invoke(() -> WANTestBase.createReceiver());
    nyReceiver.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null,
        1, 100, false));

    WANTestBase.startSenderInVMs("pn", puneSender, managing);

    // make sure all the senders are running before doing any puts
    puneSender.invoke(() -> WANTestBase.waitForSenderRunningState("pn"));
    managing.invoke(() -> WANTestBase.waitForSenderRunningState("pn"));

    WANTestBase.checkSenderMBean(puneSender, getTestMethodName() + "_PR", true);
    WANTestBase.checkSenderMBean(managing, getTestMethodName() + "_PR", true);

    WANTestBase.checkReceiverMBean(nyReceiver);

    WANTestBase.stopGatewaySender(puneSender);
    WANTestBase.startGatewaySender(puneSender);

    DistributedMember puneMember = puneSender.invoke(() -> WANTestBase.getMember());

    WANTestBase.checkRemoteClusterStatus(managing, puneMember);

  }

  private static int getLocatorPort() {
    return Locator.getLocators().get(0).getPort();
  }

}
