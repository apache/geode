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

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category(IntegrationTest.class)
public class MemberMBeanSecurityJUnitTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private MemberMXBean bean;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(MemberMXBean.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "superuser", password = "1234567")
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
    bean.processCommand("create region --name=Region_A");
    bean.showJVMMetrics();
    bean.status();
  }

  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> bean.shutDownMember()).isInstanceOf(SecurityException.class).hasMessageContaining("MEMBER:SHUTDOWN");
    assertThatThrownBy(() -> bean.createManager()).hasMessageContaining("MANAGER:CREATE");
    assertThatThrownBy(() -> bean.fetchJvmThreads()).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> bean.getName()).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> bean.getDiskStores()).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> bean.hasGatewayReceiver()).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> bean.isCacheServer()).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> bean.isServer()).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> bean.listConnectedGatewayReceivers()).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> bean.processCommand("create region --name=Region_A")).hasMessageContaining("REGION:CREATE");
    assertThatThrownBy(() -> bean.showJVMMetrics()).hasMessageContaining("JMX:GET");
    assertThatThrownBy(() -> bean.status()).hasMessageContaining("JMX:GET");
  }
}
