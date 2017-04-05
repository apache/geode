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

import org.apache.geode.management.MemberMXBean;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class, SecurityTest.class})
public class MemberMBeanSecurityJUnitTest {
  private MemberMXBean bean;

  @Rule // do not use a ClassRule since some test will do a shutdownMember
  public LocalServerStarterRule server = new ServerStarterBuilder().withJMXManager()
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/cacheServer.json")
      .buildInThisVM();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server.getJmxPort());

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(MemberMXBean.class);
  }

  @Test
  @ConnectionConfiguration(user = "super-user", password = "1234567")
  public void testAllAccess() throws Exception {
    bean.shutDownMember();
    bean.compactAllDiskStores();
    bean.createManager();
    bean.fetchJvmThreads();
    bean.getName();
    bean.getDiskStores();
    bean.hasGatewayReceiver();
    bean.isCacheServer();
    bean.isServer();
    bean.listConnectedGatewayReceivers();
    // bean.processCommand("create region --name=Region_A");
    bean.showJVMMetrics();
    bean.status();
  }

  @Test
  @ConnectionConfiguration(user = "cluster-admin", password = "1234567")
  public void testClusterAdmin() throws Exception {
    assertThatThrownBy(() -> bean.compactAllDiskStores())
        .hasMessageContaining(TestCommand.dataManage.toString());
    bean.shutDownMember();
    bean.createManager();
    bean.fetchJvmThreads();
    bean.getName();
    bean.getDiskStores();
    bean.hasGatewayReceiver();
    bean.isCacheServer();
    bean.isServer();
    bean.listConnectedGatewayReceivers();
    bean.showJVMMetrics();
    bean.status();
  }

  @Test
  @ConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testDataAdmin() throws Exception {
    bean.compactAllDiskStores();
    assertThatThrownBy(() -> bean.shutDownMember())
        .hasMessageContaining(TestCommand.clusterManage.toString());
    assertThatThrownBy(() -> bean.createManager())
        .hasMessageContaining(TestCommand.clusterManage.toString());
    bean.showJVMMetrics();
    bean.status();
  }

  @Test
  @ConnectionConfiguration(user = "data-user", password = "1234567")
  public void testDataUser() throws Exception {
    assertThatThrownBy(() -> bean.shutDownMember())
        .hasMessageContaining(TestCommand.clusterManage.toString());
    assertThatThrownBy(() -> bean.createManager())
        .hasMessageContaining(TestCommand.clusterManage.toString());
    assertThatThrownBy(() -> bean.compactAllDiskStores())
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.fetchJvmThreads())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getName())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getDiskStores())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.hasGatewayReceiver())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.isCacheServer())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.isServer())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.listConnectedGatewayReceivers())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.processCommand("create region --name=Region_A"))
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.showJVMMetrics())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.status())
        .hasMessageContaining(TestCommand.clusterRead.toString());
  }
}
