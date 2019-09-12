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
package org.apache.geode.management.bean.stats;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;

import javax.management.ObjectName;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagementTestRule;
import org.apache.geode.management.Manager;
import org.apache.geode.management.Member;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.JMXTest;

@Category({JMXTest.class})
@SuppressWarnings({"unused", "serial"})
public class DistributedSystemStatsDUnitTest implements Serializable {

  @Manager
  private VM managerVM;

  @Member
  private VM[] members;

  @Rule
  public ManagementTestRule managementTestRule = ManagementTestRule.builder().start(true).build();

  @Test
  public void testDistributedSystemStats() throws Exception {
    this.managerVM.invoke("verifyMBeans", () -> {
      DistributedSystemMXBean distributedSystemMXBean = awaitDistributedSystemMXBean();

      // next block awaits all memberMXBeanName to refresh (getLastUpdateTime)
      SystemManagementService service = this.managementTestRule.getSystemManagementService();
      List<DistributedMember> otherMemberSet = this.managementTestRule.getOtherNormalMembers();
      assertEquals(3, otherMemberSet.size());
      for (DistributedMember member : otherMemberSet) {
        MemberMXBean memberMXBean = awaitMemberMXBeanProxy(member);
        ObjectName memberMXBeanName = service.getMemberMBeanName(member);
        long lastRefreshTime = service.getLastUpdateTime(memberMXBeanName);

        await()
            .untilAsserted(
                () -> assertTrue(service.getLastUpdateTime(memberMXBeanName) > lastRefreshTime));
      }

      // TODO: add assertions for distributedSystemMXBean stats?
    });
  }

  private MemberMXBean awaitMemberMXBeanProxy(final DistributedMember member) {
    SystemManagementService service = this.managementTestRule.getSystemManagementService();
    ObjectName objectName = service.getMemberMBeanName(member);

    String alias = "Awaiting MemberMXBean proxy for " + member;
    await(alias)
        .untilAsserted(
            () -> assertThat(service.getMBeanProxy(objectName, MemberMXBean.class)).isNotNull());

    return service.getMBeanProxy(objectName, MemberMXBean.class);
  }

  private DistributedSystemMXBean awaitDistributedSystemMXBean() {
    ManagementService service = this.managementTestRule.getManagementService();

    await().untilAsserted(() -> assertThat(service.getDistributedSystemMXBean()).isNotNull());

    return service.getDistributedSystemMXBean();
  }
}
