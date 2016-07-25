/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import javax.management.ObjectName;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.GatewayReceiverMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ IntegrationTest.class, SecurityTest.class })
public class GatewayReceiverMBeanSecurityTest {

  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private static GatewayReceiverMXBean mock = mock(GatewayReceiverMXBean.class);
  private static ObjectName mockBeanName = null;
  private static ManagementService service = null;

  private GatewayReceiverMXBean bean;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "com/gemstone/gemfire/management/internal/security/cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @BeforeClass
  public static void beforeClass() throws Exception{
    // the server does not have a GAtewayReceiverMXBean registered initially, has to register a mock one.
    service = ManagementService.getManagementService(serverRule.getCache());
    mockBeanName = ObjectName.getInstance("GemFire", "key", "value");
    service.registerMBean(mock, mockBeanName);
  }

  @AfterClass
  public static void afterClass(){
    service.unregisterMBean(mockBeanName);
  }

  @Before
  public void before() throws Exception {
    bean = connectionRule.getProxyMBean(GatewayReceiverMXBean.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testAllAccess() throws Exception {
    bean.getAverageBatchProcessingTime();
    bean.getBindAddress();
    bean.getTotalConnectionsTimedOut();
    bean.isRunning();
    bean.start();
    bean.stop();
  }

  @Test
  @JMXConnectionConfiguration(user = "data-user", password = "1234567")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> bean.getTotalConnectionsTimedOut()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.start()).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.stop()).hasMessageContaining(TestCommand.dataManage.toString());
  }

}
