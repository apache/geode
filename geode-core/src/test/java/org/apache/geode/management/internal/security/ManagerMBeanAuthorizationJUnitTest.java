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
import static org.mockito.Mockito.mock;

import java.lang.management.ManagementFactory;

import javax.management.ObjectName;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.ManagerMXBean;
import org.apache.geode.management.internal.beans.ManagerMBean;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({IntegrationTest.class, SecurityTest.class})
public class ManagerMBeanAuthorizationJUnitTest {
  private ManagerMXBean managerMXBean;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/cacheServer.json")
      .withAutoStart();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @BeforeClass
  public static void beforeClassSetup() throws Exception {
    // Create a mock ManagerMBean that we will use to call against.
    ObjectName managerMBeanName = ObjectName.getInstance("GemFire", "mock", "Manager");
    ManagerMXBean bean = mock(ManagerMBean.class);
    ManagementFactory.getPlatformMBeanServer().registerMBean(bean, managerMBeanName);
  }

  @Before
  public void setUp() throws Exception {
    managerMXBean = connectionRule.getProxyMBean(ManagerMXBean.class, "GemFire:mock=Manager");
  }

  @Test
  @ConnectionConfiguration(user = "cluster-admin", password = "1234567")
  public void testAllAccess() throws Exception {
    managerMXBean.setPulseURL("foo");
    managerMXBean.start();
    managerMXBean.stop();
    managerMXBean.isRunning();
  }

  @Test
  @ConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testSomeAccess() throws Exception {
    SoftAssertions softly = new SoftAssertions();

    softly.assertThatThrownBy(() -> managerMXBean.start())
        .hasMessageContaining(TestCommand.clusterManage.toString());
    softly.assertThatThrownBy(() -> managerMXBean.getPulseURL())
        .hasMessageContaining(TestCommand.clusterWrite.toString());

    softly.assertAll();
    managerMXBean.isRunning();
  }
}
