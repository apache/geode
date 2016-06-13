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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

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
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * This is for testing remote Cluster
 * 
 * 
 */

@Category(DistributedTest.class)
public class TestRemoteClusterDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  public static MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

  public TestRemoteClusterDUnitTest() throws Exception {
    super();
  }

  @Test
  public void testMBeanCallback() throws Exception {

    VM nyLocator = getManagedNodeList().get(0);
    VM nyReceiver = getManagedNodeList().get(1);
    VM puneSender = getManagedNodeList().get(2);
    VM managing = getManagingNode();
    VM puneLocator = Host.getLocator();

    int punePort = (Integer) puneLocator.invoke(() -> TestRemoteClusterDUnitTest.getLocatorPort());

    Integer nyPort = (Integer) nyLocator.invoke(() -> WANTestBase.createFirstRemoteLocator( 12, punePort ));

    puneSender.invoke(() -> WANTestBase.createCache( punePort ));
    managing.invoke(() -> WANTestBase.createManagementCache( punePort ));
    startManagingNode(managing);

    puneSender.invoke(() -> WANTestBase.createSender( "pn",
        12, true, 100, 300, false, false, null, true ));
    managing.invoke(() -> WANTestBase.createSender( "pn", 12,
        true, 100, 300, false, false, null, true ));

    puneSender.invoke(() -> WANTestBase.createPartitionedRegion( getTestMethodName() + "_PR", "pn", 1, 100, false ));
    managing.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "pn", 1, 100, false ));

    WANTestBase.createCacheInVMs(nyPort, nyReceiver);
    nyReceiver.invoke(() -> WANTestBase.createReceiver());
    nyReceiver.invoke(() -> WANTestBase.createPartitionedRegion( getTestMethodName() + "_PR", null, 1, 100, false ));

    WANTestBase.startSenderInVMs("pn", puneSender, managing);

    // make sure all the senders are running before doing any puts
    puneSender.invoke(() -> WANTestBase.waitForSenderRunningState( "pn" ));
    managing.invoke(() -> WANTestBase.waitForSenderRunningState( "pn" ));

    checkSenderMBean(puneSender, getTestMethodName() + "_PR");
    checkSenderMBean(managing, getTestMethodName() + "_PR");

    checkReceiverMBean(nyReceiver);

    stopGatewaySender(puneSender);
    startGatewaySender(puneSender);

    DistributedMember puneMember = (DistributedMember) puneSender.invoke(() -> TestRemoteClusterDUnitTest.getMember());

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
          Wait.waitForCriterion(waitCriteria2, 2 * 60 * 1000, 5000, true);
          ManagementService service = ManagementService
              .getManagementService(cache);
          final DistributedSystemMXBean dsBean = service
              .getDistributedSystemMXBean();
          assertNotNull(dsBean);
          Map<String, Boolean> dsMap = dsBean.viewRemoteClusterStatus();
          LogWriterUtils.getLogWriter().info(
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