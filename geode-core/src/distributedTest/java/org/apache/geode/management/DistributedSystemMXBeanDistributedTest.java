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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getMemberMBeanName;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.dunit.internal.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedErrorCollector;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ManagementTest;

/**
 * Distributed tests for {@link DistributedSystemMXBean} notifications.
 *
 * <pre>
 * a) Concurrently modify proxy list by removing member and accessing the distributed system MBean
 * b) Aggregate Operations like shutDownAll
 * c) Member level operations like fetchJVMMetrics()
 * d ) Statistics
 * </pre>
 */
@Category(ManagementTest.class)
@SuppressWarnings("serial")
public class DistributedSystemMXBeanDistributedTest implements Serializable {

  private static final String MANAGER_NAME = "managerVM";
  private static final String MEMBER_NAME = "memberVM-";

  private static InternalCache cache;
  private static InternalDistributedMember distributedMember;
  private static SystemManagementService managementService;
  private static DistributedSystemMXBean distributedSystemMXBean;

  private VM managerVM;
  private VM memberVM1;
  private VM memberVM2;
  private VM memberVM3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

  @Before
  public void setUp() throws Exception {
    managerVM = getVM(0);
    memberVM1 = getVM(1);
    memberVM2 = getVM(2);
    memberVM3 = getVM(3);

    managerVM.invoke(this::createManager);

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> createMember(memberVM.getId()));
    }
  }

  @After
  public void tearDown() throws Exception {
    for (VM vm : toArray(managerVM, memberVM1, memberVM2, memberVM3)) {
      vm.invoke(() -> {
        if (cache != null) {
          cache.close();
        }
        cache = null;
        distributedMember = null;
        managementService = null;
        distributedSystemMXBean = null;
      });
    }
  }

  @Test
  public void getMemberCount() {
    // 1 manager, 3 members, 1 dunit locator
    managerVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(distributedSystemMXBean.getMemberCount()).isEqualTo(5);
      });
    });
  }

  @Test
  public void showJVMMetrics() {
    managerVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(distributedSystemMXBean.getMemberCount()).isEqualTo(5);
      });

      for (DistributedMember member : getOtherNormalMembers()) {
        assertThat(distributedSystemMXBean.showJVMMetrics(member.getName())).isNotNull();
      }
    });
  }

  @Test
  public void showOSMetrics() {
    managerVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(distributedSystemMXBean.getMemberCount()).isEqualTo(5);
      });

      Set<InternalDistributedMember> otherMembers = getOtherNormalMembers();
      for (DistributedMember member : otherMembers) {
        assertThat(distributedSystemMXBean.showOSMetrics(member.getName())).isNotNull();
      }
    });
  }

  @Test
  public void shutDownAllMembers() {
    managerVM.invoke(() -> {
      distributedSystemMXBean.shutDownAllMembers();

      await().untilAsserted(() -> {
        assertThat(getOtherNormalMembers()).hasSize(0);
      });
    });
  }

  @Test
  public void listMemberObjectNames() {
    managerVM.invoke(() -> {
      await().untilAsserted(() -> {
        assertThat(distributedSystemMXBean.listMemberObjectNames()).hasSize(4);
      });
    });
  }

  @Test
  public void fetchMemberObjectName() {
    managerVM.invoke(() -> {
      String memberName = distributedMember.getName();

      await().untilAsserted(() -> {
        assertThat(distributedSystemMXBean.fetchMemberObjectName(memberName)).isNotNull();
      });

      ObjectName memberMXBeanName = distributedSystemMXBean.fetchMemberObjectName(memberName);
      assertThat(memberMXBeanName).isEqualTo(getMemberMBeanName(memberName));
    });
  }

  private void createManager() {
    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, MANAGER_NAME);
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "true");
    config.setProperty(JMX_MANAGER_PORT, "0");
    config.setProperty(HTTP_SERVICE_PORT, "0");

    cache = (InternalCache) new CacheFactory(config).create();
    distributedMember = cache.getDistributionManager().getId();
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);

    distributedSystemMXBean = managementService.getDistributedSystemMXBean();
  }

  private void createMember(int vmId) {
    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, MEMBER_NAME + vmId);
    config.setProperty(JMX_MANAGER, "false");

    cache = (InternalCache) new CacheFactory(config).create();
    distributedMember = cache.getDistributionManager().getId();
    managementService = (SystemManagementService) ManagementService.getManagementService(cache);
  }

  private Set<InternalDistributedMember> getOtherNormalMembers() {
    return cache.getDistributionManager().getOtherNormalDistributionManagerIds();
  }
}
