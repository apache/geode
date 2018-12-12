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

import java.io.Serializable;

import javax.management.ObjectName;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;

/**
 * Distributed tests for registering of custom mbean using composite types.
 * <p>
 *
 * See User API {@link ManagementService#registerMBean(Object, ObjectName)}.
 */

@SuppressWarnings({"serial", "unused"})
public class CompositeTypeTestDUnitTest implements Serializable {

  @Manager
  private VM managerVM;

  @Member
  private VM memberVM;

  @Rule
  public ManagementTestRule managementTestRule = ManagementTestRule.builder().start(true).build();

  @Test
  public void testCompositeTypeGetters() throws Exception {
    registerMBeanWithCompositeTypeGetters(this.memberVM);

    String memberName = MBeanJMXAdapter.makeCompliantName(getMemberId(this.memberVM));
    verifyMBeanWithCompositeTypeGetters(this.managerVM, memberName);
  }

  private void registerMBeanWithCompositeTypeGetters(final VM memberVM) {
    memberVM.invoke("registerMBeanWithCompositeTypeGetters", () -> {
      SystemManagementService service = this.managementTestRule.getSystemManagementService();

      ObjectName objectName = new ObjectName("GemFire:service=custom,type=composite");
      CompositeTestMXBean compositeTestMXBean = new CompositeTestMBean();

      objectName = service.registerMBean(compositeTestMXBean, objectName);
      service.federate(objectName, CompositeTestMXBean.class, false);
    });
  }

  private void verifyMBeanWithCompositeTypeGetters(final VM managerVM, final String memberId) {
    managerVM.invoke("verifyMBeanWithCompositeTypeGetters", () -> {
      SystemManagementService service = this.managementTestRule.getSystemManagementService();
      ObjectName objectName =
          new ObjectName("GemFire:service=custom,type=composite,member=" + memberId);

      GeodeAwaitility.await()
          .until(() -> service.getMBeanInstance(objectName, CompositeTestMXBean.class) != null);

      CompositeTestMXBean compositeTestMXBean =
          service.getMBeanInstance(objectName, CompositeTestMXBean.class);
      assertThat(compositeTestMXBean).isNotNull();

      CompositeStats listCompositeStatsData = compositeTestMXBean.listCompositeStats();
      assertThat(listCompositeStatsData).isNotNull();

      CompositeStats getCompositeStatsData = compositeTestMXBean.getCompositeStats();
      assertThat(getCompositeStatsData).isNotNull();

      CompositeStats[] getCompositeArrayData = compositeTestMXBean.getCompositeArray();
      assertThat(getCompositeArrayData).isNotNull().isNotEmpty();

      Integer[] getIntegerArrayData = compositeTestMXBean.getIntegerArray();
      assertThat(getIntegerArrayData).isNotNull().isNotEmpty();
    });
  }

  private String getMemberId(final VM memberVM) {
    return memberVM.invoke("getMemberId",
        () -> this.managementTestRule.getDistributedMember().getId());
  }

}
