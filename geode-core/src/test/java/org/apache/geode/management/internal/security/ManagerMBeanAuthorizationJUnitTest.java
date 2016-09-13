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
package org.apache.geode.management.internal.security;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.management.ManagementFactory;

import javax.management.ObjectName;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePort;
import org.apache.geode.management.ManagerMXBean;
import org.apache.geode.management.internal.beans.ManagerMBean;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ IntegrationTest.class, SecurityTest.class })
public class ManagerMBeanAuthorizationJUnitTest {

  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private ManagerMXBean managerMXBean;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "org/apache/geode/management/internal/security/cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

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
  @JMXConnectionConfiguration(user = "cluster-admin", password = "1234567")
  public void testAllAccess() throws Exception {
    managerMXBean.setPulseURL("foo");
    managerMXBean.start();
    managerMXBean.stop();
    managerMXBean.isRunning();
  }

  @Test
  @JMXConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testSomeAccess() throws Exception {
    assertThatThrownBy(() -> managerMXBean.start()).hasMessageContaining(TestCommand.clusterManage.toString());
    assertThatThrownBy(() -> managerMXBean.getPulseURL()).hasMessageContaining(TestCommand.clusterWrite.toString());
    managerMXBean.isRunning();
  }
}
