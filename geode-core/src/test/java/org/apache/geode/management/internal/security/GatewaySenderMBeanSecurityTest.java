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
package org.apache.geode.management.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.beans.GatewaySenderMBean;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.management.ObjectName;

@Category({IntegrationTest.class, SecurityTest.class})
public class GatewaySenderMBeanSecurityTest {
  private static GatewaySenderMBean mock = mock(GatewaySenderMBean.class);
  private static ObjectName mockBeanName = null;
  private static ManagementService service = null;

  private GatewaySenderMXBean bean;

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager()
          .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/cacheServer.json")
          .startAutomatically();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @BeforeClass
  public static void beforeClass() throws Exception {
    // the server does not have a GAtewaySenderMXBean registered initially, has to register a mock
    // one.
    service = ManagementService.getManagementService(server.getCache());
    mockBeanName = ObjectName.getInstance("GemFire", "key", "value");
    service.registerMBean(mock, mockBeanName);
  }

  @AfterClass
  public static void afterClass() {
    service.unregisterMBean(mockBeanName);
  }

  @Before
  public void before() throws Exception {
    bean = connectionRule.getProxyMBean(GatewaySenderMXBean.class);
  }

  @Test
  @ConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testAllAccess() throws Exception {
    bean.getAlertThreshold();
    bean.getAverageDistributionTimePerBatch();
    bean.getBatchSize();
    bean.getMaximumQueueMemory();
    bean.getOrderPolicy();
    bean.isBatchConflationEnabled();
    bean.isManualStart();
    bean.pause();
    bean.rebalance();
    bean.resume();
    bean.start();
    bean.stop();
  }

  @Test
  @ConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> bean.getAlertThreshold())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getAverageDistributionTimePerBatch())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getBatchSize())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getMaximumQueueMemory())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getOrderPolicy())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.isBatchConflationEnabled())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.isManualStart())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.pause()).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.rebalance())
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.resume()).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.start()).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.stop()).hasMessageContaining(TestCommand.dataManage.toString());
  }

}
