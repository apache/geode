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

import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePort;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({IntegrationTest.class, SecurityTest.class, FlakyTest.class}) // GEODE-1953
public class CacheServerMBeanAuthorizationJUnitTest {

  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private CacheServerMXBean bean;

  @ClassRule
  public static CacheServerStartupRule serverRule =
      CacheServerStartupRule.withDefaultSecurityJson(jmxManagerPort);

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

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
    bean.stopContinuousQuery("bar");
    bean.closeAllContinuousQuery("bar");
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
    assertThatThrownBy(() -> bean.removeIndex("foo"))
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.executeContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.dataRead.toString());
    assertThatThrownBy(() -> bean.fetchLoadProbe())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getActiveCQCount())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.stopContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.closeAllContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.isRunning())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.showClientQueueDetails("bar"))
        .hasMessageContaining(TestCommand.clusterRead.toString());
  }
}
