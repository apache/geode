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
package org.apache.geode.internal.cache.wan;


import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Set;

import javax.management.ObjectName;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class GatewayReceiverMBeanDUnitTest extends ManagementTestBase {

  @Test
  public void testMBeanAndProxiesForGatewayReceiverAreCreated() throws Exception {
    initManagement(true);


    // Verify MBean is created in each managed node
    for (VM vm : getManagedNodeList()) {
      vm.invoke(() -> {
        GatewayReceiver receiver = getCache().createGatewayReceiverFactory().create();
      });
      vm.invoke(() -> verifyMBean());
    }

    // Verify MBean proxies are created in the managing node
    getManagingNode().invoke(() -> verifyMBeanProxies(getCache()));
  }

  @Test
  public void testMBeanAndProxiesForGatewayReceiverAreRemovedOnDestroy() throws Exception {
    initManagement(true);

    // Verify MBean is created in each managed node
    for (VM vm : getManagedNodeList()) {
      vm.invoke(() -> {
        GatewayReceiver receiver = getCache().createGatewayReceiverFactory().create();
        receiver.start();
        receiver.stop();
        receiver.destroy();

      });
      vm.invoke(() -> verifyMBeanDoesNotExist());
    }

    // Verify MBean proxies are created in the managing node
    getManagingNode().invoke(() -> verifyMBeanProxiesDoesNotExist(getCache()));
  }

  private void verifyMBean() {
    assertNotNull(getMBean());
  }

  private void verifyMBeanDoesNotExist() {
    assertNull(getMBean());
  }

  private GatewayReceiverMXBean getMBean() {
    ObjectName objectName =
        MBeanJMXAdapter.getGatewayReceiverMBeanName(getSystem().getDistributedMember());
    return getManagementService().getMBeanInstance(objectName, GatewayReceiverMXBean.class);
  }

  private static void verifyMBeanProxies(final InternalCache cache) {
    Set<? extends DistributedMember> members =
        cache.getDistributionManager().getOtherNormalDistributionManagerIds();
    for (DistributedMember member : members) {
      await()
          .untilAsserted(() -> assertNotNull(getMBeanProxy(member)));
    }
  }

  private static void verifyMBeanProxiesDoesNotExist(final InternalCache cache) {
    Set<? extends DistributedMember> members =
        cache.getDistributionManager().getOtherNormalDistributionManagerIds();
    for (DistributedMember member : members) {
      assertNull(getMBeanProxy(member));
    }
  }

  private static GatewayReceiverMXBean getMBeanProxy(DistributedMember member) {
    SystemManagementService service = (SystemManagementService) getManagementService();
    ObjectName objectName = MBeanJMXAdapter.getGatewayReceiverMBeanName(member);
    return service.getMBeanProxy(objectName, GatewayReceiverMXBean.class);
  }


}
