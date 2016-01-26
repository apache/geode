/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.pulse;

import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.GatewayReceiverMXBean;
import com.gemstone.gemfire.management.GatewaySenderMXBean;
import com.gemstone.gemfire.management.MBeanUtil;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagementTestBase;
import com.gemstone.gemfire.management.RegionMXBean;

import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This is for testing remote Cluster
 * 
 * @author ajayp
 * 
 */

public class TestRemoteClusterDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public static MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

  public TestRemoteClusterDUnitTest(String name) throws Exception {
    super(name);
  }

  public void tearDown2() throws Exception {
    super.tearDown2();

  }

  public void testMBeanCallback() throws Exception {

    VM nyLocator = getManagedNodeList().get(0);
    VM nyReceiver = getManagedNodeList().get(1);
    VM puneSender = getManagedNodeList().get(2);
    VM managing = getManagingNode();
    VM puneLocator = Host.getLocator();

    int punePort = (Integer) puneLocator.invoke(
        TestRemoteClusterDUnitTest.class, "getLocatorPort");

    Integer nyPort = (Integer) nyLocator.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 12, punePort });

    puneSender.invoke(WANTestBase.class, "createCache",
        new Object[] { punePort });
    managing.invoke(WANTestBase.class, "createManagementCache",
        new Object[] { punePort });
    startManagingNode(managing);

    puneSender.invoke(WANTestBase.class, "createSender", new Object[] { "pn",
        12, true, 100, 300, false, false, null, true });
    managing.invoke(WANTestBase.class, "createSender", new Object[] { "pn", 12,
        true, 100, 300, false, false, null, true });

    puneSender.invoke(WANTestBase.class, "createPartitionedRegion",
        new Object[] { testName + "_PR", "pn", 1, 100, false });
    managing.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "pn", 1, 100, false });

    nyReceiver.invoke(WANTestBase.class, "createReceiver",
        new Object[] { nyPort });
    nyReceiver.invoke(WANTestBase.class, "createPartitionedRegion",
        new Object[] { testName + "_PR", null, 1, 100, false });

    puneSender.invoke(WANTestBase.class, "startSender", new Object[] { "pn" });
    managing.invoke(WANTestBase.class, "startSender", new Object[] { "pn" });

    // make sure all the senders are running before doing any puts
    puneSender.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "pn" });
    managing.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "pn" });

    checkSenderMBean(puneSender, testName + "_PR");
    checkSenderMBean(managing, testName + "_PR");

    checkReceiverMBean(nyReceiver);

    stopGatewaySender(puneSender);
    startGatewaySender(puneSender);

    DistributedMember puneMember = (DistributedMember) puneSender.invoke(
        TestRemoteClusterDUnitTest.class, "getMember");

    checkRemoteClusterStatus(managing, puneMember);

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
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  protected void checkRemoteClusterStatus(final VM vm,
      final DistributedMember senderMember) {
    SerializableRunnable checkProxySender = new SerializableRunnable(
        "DS Map Size") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
          final WaitCriterion waitCriteria2 = new WaitCriterion() {
            @Override
            public boolean done() {
              Cache cache = GemFireCacheImpl.getInstance();
              final ManagementService service = ManagementService
                  .getManagementService(cache);
              final DistributedSystemMXBean dsBean = service
                  .getDistributedSystemMXBean();
              if (dsBean != null) {
                return true;
              }
              return false;
            }
            @Override
            public String description() {
              return "wait for getDistributedSystemMXBean to complete and get results";
            }
          };
          waitForCriterion(waitCriteria2, 2 * 60 * 1000, 5000, true);
          ManagementService service = ManagementService
              .getManagementService(cache);
          final DistributedSystemMXBean dsBean = service
              .getDistributedSystemMXBean();
          assertNotNull(dsBean);
          Map<String, Boolean> dsMap = dsBean.viewRemoteClusterStatus();
          getLogWriter().info(
              "Ds Map is: " + dsMap.size());
          assertNotNull(dsMap);
          assertEquals(true, dsMap.size() > 0 ? true : false);
        }      
    };
    vm.invoke(checkProxySender);
  }


  /**
   * stops a gateway sender
   * 
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  protected void stopGatewaySender(final VM vm) {
    SerializableRunnable stopGatewaySender = new SerializableRunnable(
        "Stop Gateway Sender") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService
            .getManagementService(cache);
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
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  protected void startGatewaySender(final VM vm) {
    SerializableRunnable stopGatewaySender = new SerializableRunnable(
        "Start Gateway Sender") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService
            .getManagementService(cache);
        GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("pn");
        assertNotNull(bean);
        bean.start();
        assertTrue(bean.isRunning());
      }
    };
    vm.invoke(stopGatewaySender);
  }

  /**
   * Checks whether a GatewayreceiverMBean is created or not
   * 
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  protected void checkReceiverMBean(final VM vm) {
    SerializableRunnable checkMBean = new SerializableRunnable(
        "Check Receiver MBean") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService
            .getManagementService(cache);
        GatewayReceiverMXBean bean = service.getLocalGatewayReceiverMXBean();
        assertNotNull(bean);
      }
    };
    vm.invoke(checkMBean);
  }

  /**
   * Checks whether a GatewayreceiverMBean is created or not
   * 
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  protected void checkSenderMBean(final VM vm, final String regionPath) {
    SerializableRunnable checkMBean = new SerializableRunnable(
        "Check Sender MBean") {
      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService
            .getManagementService(cache);

        GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("pn");
        assertNotNull(bean);
        assertTrue(bean.isConnected());

        ObjectName regionBeanName = service.getRegionMBeanName(cache
            .getDistributedSystem().getDistributedMember(), "/" + regionPath);
        RegionMXBean rBean = service.getMBeanInstance(regionBeanName,
            RegionMXBean.class);
        assertTrue(rBean.isGatewayEnabled());

      }
    };
    vm.invoke(checkMBean);
  }
}