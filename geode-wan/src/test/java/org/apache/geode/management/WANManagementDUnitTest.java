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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

/**
 * Tests for WAN artifacts like Sender and Receiver. The purpose of this test is not to check WAN
 * functionality , but to verify ManagementServices running properly and reflecting WAN behaviour
 * and data properly
 *
 *
 */
@Category(DistributedTest.class)
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

    checkSenderMBean(puneSender, getTestMethodName() + "_PR", true);
    checkSenderMBean(managing, getTestMethodName() + "_PR", true);

    checkReceiverMBean(nyReceiver);

    stopGatewaySender(puneSender);
    startGatewaySender(puneSender);

    DistributedMember puneMember = puneSender.invoke(() -> WANManagementDUnitTest.getMember());

    checkProxySender(managing, puneMember);
    checkSenderNavigationAPIS(managing, puneMember);

    nyReceiver.invoke(() -> WANTestBase.stopReceivers());

    checkSenderMBean(puneSender, getTestMethodName() + "_PR", false);
    checkSenderMBean(managing, getTestMethodName() + "_PR", false);
  }

  @Category(FlakyTest.class) // GEODE-1603
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

    checkSenderMBean(puneSender, getTestMethodName() + "_PR", true);
    checkReceiverMBean(nyReceiver);

    DistributedMember nyMember = nyReceiver.invoke(() -> WANManagementDUnitTest.getMember());

    checkProxyReceiver(managing, nyMember);
    checkReceiverNavigationAPIS(managing, nyMember);
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

    checkAsyncQueueMBean(puneSender, true);
    checkAsyncQueueMBean(managing, true);

    DistributedMember puneMember = puneSender.invoke(() -> WANManagementDUnitTest.getMember());

    checkProxyAsyncQueue(managing, puneMember, true);

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
    checkAsyncQueueMBean(memberVM, true);
    checkAsyncQueueMBean(managerVm, true);
    DistributedMember member = memberVM.invoke(() -> WANManagementDUnitTest.getMember());
    checkProxyAsyncQueue(managerVm, member, true);

    // Destroy AsyncEventQueue
    memberVM.invoke(() -> WANTestBase.destroyAsyncEventQueue(aeqId));
    managerVm.invoke(() -> WANTestBase.destroyAsyncEventQueue(aeqId));

    // Verify AsyncEventQueueMXBean no longer exists
    checkAsyncQueueMBean(memberVM, false);
    checkAsyncQueueMBean(managerVm, false);
    checkProxyAsyncQueue(managerVm, member, false);
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
      Cache cache = GemFireCacheImpl.getInstance();
      ManagementService service = ManagementService.getManagementService(cache);

      Awaitility.await().atMost(5, TimeUnit.SECONDS)
          .until(() -> assertNotNull(service.getDistributedRegionMXBean(regionPath)));

      DistributedRegionMXBean bean = service.getDistributedRegionMXBean(regionPath);
      assertThat(bean.listRegionAttributes().getGatewaySenderIds()).containsExactly("pn");
    });
  }

  @SuppressWarnings("serial")
  protected void checkSenderNavigationAPIS(final VM vm, final DistributedMember senderMember) {
    SerializableRunnable checkNavigationAPIS =
        new SerializableRunnable("Check Sender Navigation APIs") {
          public void run() {
            Cache cache = GemFireCacheImpl.getInstance();
            ManagementService service = ManagementService.getManagementService(cache);
            DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
            ObjectName expectedName = service.getGatewaySenderMBeanName(senderMember, "pn");
            try {
              ObjectName actualName = bean.fetchGatewaySenderObjectName(senderMember.getId(), "pn");
              assertEquals(expectedName, actualName);
            } catch (Exception e) {
              fail("Sender Navigation Failed " + e);
            }

            assertEquals(2, bean.listGatewaySenderObjectNames().length);
            try {
              assertEquals(1, bean.listGatewaySenderObjectNames(senderMember.getId()).length);
            } catch (Exception e) {
              fail("Sender Navigation Failed " + e);
            }

          }
        };
    vm.invoke(checkNavigationAPIS);
  }

  @SuppressWarnings("serial")
  protected void checkReceiverNavigationAPIS(final VM vm, final DistributedMember receiverMember) {
    SerializableRunnable checkNavigationAPIS =
        new SerializableRunnable("Check Receiver Navigation APIs") {
          public void run() {
            Cache cache = GemFireCacheImpl.getInstance();
            ManagementService service = ManagementService.getManagementService(cache);
            DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
            ObjectName expectedName = service.getGatewayReceiverMBeanName(receiverMember);
            try {
              ObjectName actualName = bean.fetchGatewayReceiverObjectName(receiverMember.getId());
              assertEquals(expectedName, actualName);
            } catch (Exception e) {
              fail("Receiver Navigation Failed " + e);
            }

            assertEquals(1, bean.listGatewayReceiverObjectNames().length);

          }
        };
    vm.invoke(checkNavigationAPIS);
  }

  private static int getLocatorPort() {
    return Locator.getLocators().get(0).getPort();
  }

  private static DistributedMember getMember() {
    return GemFireCacheImpl.getInstance().getMyId();
  }

  /**
   * Checks Proxy GatewaySender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void checkProxySender(final VM vm, final DistributedMember senderMember) {
    SerializableRunnable checkProxySender = new SerializableRunnable("Check Proxy Sender") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService.getManagementService(cache);
        GatewaySenderMXBean bean = null;
        try {
          bean = MBeanUtil.getGatewaySenderMbeanProxy(senderMember, "pn");
        } catch (Exception e) {
          fail("Could not obtain Sender Proxy in desired time " + e);
        }
        assertNotNull(bean);
        final ObjectName senderMBeanName = service.getGatewaySenderMBeanName(senderMember, "pn");
        try {
          MBeanUtil.printBeanDetails(senderMBeanName);
        } catch (Exception e) {
          fail("Error while Printing Bean Details " + e);
        }

        if (service.isManager()) {
          DistributedSystemMXBean dsBean = service.getDistributedSystemMXBean();
          Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() -> {
            Map<String, Boolean> dsMap = dsBean.viewRemoteClusterStatus();
            dsMap.entrySet().stream()
                .forEach(entry -> assertTrue("Should be true " + entry.getKey(), entry.getValue()));
          });
        }

      }
    };
    vm.invoke(checkProxySender);
  }

  /**
   * Checks Proxy GatewayReceiver
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void checkProxyReceiver(final VM vm, final DistributedMember senderMember) {
    SerializableRunnable checkProxySender = new SerializableRunnable("Check Proxy Receiver") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService.getManagementService(cache);
        GatewayReceiverMXBean bean = null;
        try {
          bean = MBeanUtil.getGatewayReceiverMbeanProxy(senderMember);
        } catch (Exception e) {
          fail("Could not obtain Sender Proxy in desired time " + e);
        }
        assertNotNull(bean);
        final ObjectName receiverMBeanName = service.getGatewayReceiverMBeanName(senderMember);
        try {
          MBeanUtil.printBeanDetails(receiverMBeanName);
        } catch (Exception e) {
          fail("Error while Printing Bean Details " + e);
        }

      }
    };
    vm.invoke(checkProxySender);
  }


  /**
   * stops a gateway sender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void stopGatewaySender(final VM vm) {
    SerializableRunnable stopGatewaySender = new SerializableRunnable("Stop Gateway Sender") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService.getManagementService(cache);
        GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("pn");
        assertNotNull(bean);
        bean.stop();
        assertFalse(bean.isRunning());
      }
    };
    vm.invoke(stopGatewaySender);
  }

  /**
   * start a gateway sender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void startGatewaySender(final VM vm) {
    SerializableRunnable stopGatewaySender = new SerializableRunnable("Start Gateway Sender") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService.getManagementService(cache);
        GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("pn");
        assertNotNull(bean);
        bean.start();
        assertTrue(bean.isRunning());
      }
    };
    vm.invoke(stopGatewaySender);
  }


  /**
   * Checks whether a GatewayReceiverMBean is created or not
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void checkReceiverMBean(final VM vm) {
    SerializableRunnable checkMBean = new SerializableRunnable("Check Receiver MBean") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService.getManagementService(cache);
        GatewayReceiverMXBean bean = service.getLocalGatewayReceiverMXBean();
        assertNotNull(bean);
      }
    };
    vm.invoke(checkMBean);
  }

  /**
   * Checks whether a GatewayReceiverMBean is created or not
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void checkSenderMBean(final VM vm, final String regionPath, boolean connected) {
    SerializableRunnable checkMBean = new SerializableRunnable("Check Sender MBean") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService.getManagementService(cache);

        GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("pn");
        assertNotNull(bean);
        Awaitility.await().atMost(1, TimeUnit.MINUTES)
            .until(() -> assertEquals(connected, bean.isConnected()));

        ObjectName regionBeanName = service.getRegionMBeanName(
            cache.getDistributedSystem().getDistributedMember(), "/" + regionPath);
        RegionMXBean rBean = service.getMBeanInstance(regionBeanName, RegionMXBean.class);
        assertTrue(rBean.isGatewayEnabled());


      }
    };
    vm.invoke(checkMBean);
  }

  /**
   * Checks whether a Async Queue MBean is created or not
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void checkAsyncQueueMBean(final VM vm, final boolean shouldExist) {
    SerializableRunnable checkAsyncQueueMBean =
        new SerializableRunnable("Check Async Queue MBean") {
          public void run() {
            Cache cache = GemFireCacheImpl.getInstance();
            ManagementService service = ManagementService.getManagementService(cache);
            AsyncEventQueueMXBean bean = service.getLocalAsyncEventQueueMXBean("pn");
            if (shouldExist) {
              assertNotNull(bean);
            } else {
              assertNull(bean);
            }
          }
        };
    vm.invoke(checkAsyncQueueMBean);
  }

  /**
   * Checks Proxy Async Queue
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void checkProxyAsyncQueue(final VM vm, final DistributedMember senderMember,
      final boolean shouldExist) {
    SerializableRunnable checkProxyAsyncQueue =
        new SerializableRunnable("Check Proxy Async Queue") {
          public void run() {
            Cache cache = GemFireCacheImpl.getInstance();
            SystemManagementService service =
                (SystemManagementService) ManagementService.getManagementService(cache);
            final ObjectName queueMBeanName =
                service.getAsyncEventQueueMBeanName(senderMember, "pn");
            AsyncEventQueueMXBean bean = null;
            if (shouldExist) {
              // Verify the MBean proxy exists
              try {
                bean = MBeanUtil.getAsyncEventQueueMBeanProxy(senderMember, "pn");
              } catch (Exception e) {
                fail("Could not obtain Sender Proxy in desired time " + e);
              }
              assertNotNull(bean);

              try {
                MBeanUtil.printBeanDetails(queueMBeanName);
              } catch (Exception e) {
                fail("Error while Printing Bean Details " + e);
              }
            } else {
              // Verify the MBean proxy doesn't exist
              bean = service.getMBeanProxy(queueMBeanName, AsyncEventQueueMXBean.class);
              assertNull(bean);
            }
          }
        };
    vm.invoke(checkProxyAsyncQueue);
  }
}
