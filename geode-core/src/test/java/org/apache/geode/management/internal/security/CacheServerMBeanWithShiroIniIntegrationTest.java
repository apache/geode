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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class, SecurityTest.class})
public class CacheServerMBeanWithShiroIniIntegrationTest {
  private CacheServerMXBean bean;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_SHIRO_INIT, "shiro.ini").withJMXManager().withAutoStart();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(CacheServerMXBean.class);
  }

  @Test
  @ConnectionConfiguration(user = "root", password = "secret")
  public void testAllAccess() throws Exception {
    bean.removeIndex("foo");
    bean.executeContinuousQuery("bar");
    bean.fetchLoadProbe();
    bean.getActiveCQCount();
    bean.stopContinuousQuery("bar");
    bean.closeAllContinuousQuery("bar");
    bean.isRunning();
    bean.showClientQueueDetails("foo");
  }

  @Test
  @ConnectionConfiguration(user = "guest", password = "guest")
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

  @Test
  @ConnectionConfiguration(user = "regionAReader", password = "password")
  public void testRegionAccess() throws Exception {
    assertThatThrownBy(() -> bean.removeIndex("foo"))
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.fetchLoadProbe())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getActiveCQCount())
        .hasMessageContaining(TestCommand.clusterRead.toString());

    assertThatThrownBy(() -> bean.executeContinuousQuery("bar"))
        .hasMessageContaining(TestCommand.dataRead.toString());
  }

  @Test
  @ConnectionConfiguration(user = "dataReader", password = "12345")
  public void testDataRead() throws Exception {
    assertThatThrownBy(() -> bean.removeIndex("foo"))
        .hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.fetchLoadProbe())
        .hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getActiveCQCount())
        .hasMessageContaining(TestCommand.clusterRead.toString());

    bean.executeContinuousQuery("bar");
  }
}
