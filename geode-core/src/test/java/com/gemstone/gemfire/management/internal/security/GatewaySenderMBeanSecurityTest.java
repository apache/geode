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
import com.gemstone.gemfire.management.GatewaySenderMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.beans.GatewaySenderMBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ IntegrationTest.class, SecurityTest.class })
public class GatewaySenderMBeanSecurityTest {

  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private static GatewaySenderMBean mock = mock(GatewaySenderMBean.class);
  private static ObjectName mockBeanName = null;
  private static ManagementService service = null;

  private GatewaySenderMXBean bean;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "com/gemstone/gemfire/management/internal/security/cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @BeforeClass
  public static void beforeClass() throws Exception{
    // the server does not have a GAtewaySenderMXBean registered initially, has to register a mock one.
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
    bean = connectionRule.getProxyMBean(GatewaySenderMXBean.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "data-admin", password = "1234567")
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
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> bean.getAlertThreshold()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getAverageDistributionTimePerBatch()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getBatchSize()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getMaximumQueueMemory()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getOrderPolicy()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.isBatchConflationEnabled()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.isManualStart()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.pause()).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.rebalance()).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.resume()).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.start()).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.stop()).hasMessageContaining(TestCommand.dataManage.toString());
  }

}
