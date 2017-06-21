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

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({IntegrationTest.class, SecurityTest.class})
public class CacheServerMBeanAuthorizationJUnitTest {
  private CacheServerMXBean bean;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/cacheServer.json")
      .withAutoStart();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(CacheServerMXBean.class);
  }

  @Test
  @ConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testDataAdmin() throws Exception {
    bean.removeIndex("foo");
    assertThatThrownBy(() -> bean.executeContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.dataRead.toString());
    bean.fetchLoadProbe();
    bean.getActiveCQCount();
    assertThatThrownBy(() -> bean.stopContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.clusterManageQuery.toString());
    assertThatThrownBy(() -> bean.closeAllContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.clusterManageQuery.toString());
    bean.isRunning();
    bean.showClientQueueDetails("foo");
  }

  @Test
  @ConnectionConfiguration(user = "cluster-admin", password = "1234567")
  public void testClusterAdmin() throws Exception {
    assertThatThrownBy(() -> bean.removeIndex("foo"))
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.executeContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.dataRead.toString());
    bean.fetchLoadProbe();
  }


  @Test
  @ConnectionConfiguration(user = "data-user", password = "1234567")
  public void testDataUser() throws Exception {
    assertThatThrownBy(() -> bean.removeIndex("foo"))
        .hasMessageContaining(TestCommand.dataManage.toString());
    bean.executeContinuousQuery("bar");
    assertThatThrownBy(() -> bean.fetchLoadProbe())
        .hasMessageContaining(TestCommand.clusterRead.toString());
  }

  @Test
  @ConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() throws Exception {
    SoftAssertions softly = new SoftAssertions();

    softly.assertThatThrownBy(() -> bean.removeIndex("foo"))
        .hasMessageContaining(TestCommand.dataManage.toString());
    softly.assertThatThrownBy(() -> bean.executeContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.dataRead.toString());
    softly.assertThatThrownBy(() -> bean.fetchLoadProbe())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    softly.assertThatThrownBy(() -> bean.getActiveCQCount())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    softly.assertThatThrownBy(() -> bean.stopContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.clusterManageQuery.toString());
    softly.assertThatThrownBy(() -> bean.closeAllContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.clusterManageQuery.toString());
    softly.assertThatThrownBy(() -> bean.isRunning())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    softly.assertThatThrownBy(() -> bean.showClientQueueDetails("bar"))
        .hasMessageContaining(TestCommand.clusterRead.toString());

    softly.assertAll();
  }
}
