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

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class CacheServerMBeanShiroJUnitTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private CacheServerMXBean bean;

  @ClassRule
  public static ShiroCacheStartRule serverRule = new ShiroCacheStartRule(jmxManagerPort, "shiro.ini");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(CacheServerMXBean.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "root", password = "secret")
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
  @JMXConnectionConfiguration(user = "guest", password = "guest")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> bean.removeIndex("foo")).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.executeContinuousQuery("bar")).hasMessageContaining(TestCommand.dataRead.toString());
    assertThatThrownBy(() -> bean.fetchLoadProbe()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getActiveCQCount()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.stopContinuousQuery("bar")).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.closeAllContinuousQuery("bar")).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.isRunning()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.showClientQueueDetails("bar")).hasMessageContaining(TestCommand.clusterRead.toString());
  }

  @Test
  @JMXConnectionConfiguration(user = "regionAReader", password = "password")
  public void testRegionAccess() throws Exception{
    assertThatThrownBy(() -> bean.removeIndex("foo")).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.fetchLoadProbe()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getActiveCQCount()).hasMessageContaining(TestCommand.clusterRead.toString());

    assertThatThrownBy(() -> bean.executeContinuousQuery("bar")).hasMessageContaining(TestCommand.dataRead.toString());
  }

  @Test
  @JMXConnectionConfiguration(user = "dataReader", password = "12345")
  public void ztestDataRead() throws Exception{
    assertThatThrownBy(() -> bean.removeIndex("foo")).hasMessageContaining(TestCommand.dataManage.toString());
    assertThatThrownBy(() -> bean.fetchLoadProbe()).hasMessageContaining(TestCommand.clusterRead.toString());
    assertThatThrownBy(() -> bean.getActiveCQCount()).hasMessageContaining(TestCommand.clusterRead.toString());

    bean.executeContinuousQuery("bar");
  }
}
