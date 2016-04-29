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
package com.gemstone.gemfire.management.internal.security;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.management.ManagementFactory;
import javax.management.ObjectName;

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.ManagerMXBean;
import com.gemstone.gemfire.management.internal.beans.ManagerMBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ManagerMBeanAuthorizationJUnitTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private ManagerMXBean managerMXBean;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "cacheServer.json");

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
