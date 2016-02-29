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
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
  @JMXConnectionConfiguration(user = "super-user", password = "1234567")
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
    //bean.processCommand("create region --name=Region_A");
    bean.showJVMMetrics();
    bean.status();
  }

  @Test
  @JMXConnectionConfiguration(user = "cluster-admin", password = "1234567")
  public void testClusterAdmin() throws Exception {
    assertThatThrownBy(() -> bean.compactAllDiskStores()).hasMessageContaining("DATA:MANAGE");
    bean.shutDownMember();
    bean.createManager();
    bean.fetchJvmThreads();
    bean.getName();
    bean.getDiskStores();
    bean.hasGatewayReceiver();
    bean.isCacheServer();
    bean.isServer();
    bean.listConnectedGatewayReceivers();
    bean.showJVMMetrics();
    bean.status();
  }

  @Test
  @JMXConnectionConfiguration(user = "data-admin", password = "1234567")
  public void testDataAdmin() throws Exception {
    bean.compactAllDiskStores();
    assertThatThrownBy(() -> bean.shutDownMember()).hasMessageContaining("CLUSTER:MANAGE");
    assertThatThrownBy(() -> bean.createManager()).hasMessageContaining("CLUSTER:MANAGE");
    bean.showJVMMetrics();
    bean.status();
  }

  @Test
  @JMXConnectionConfiguration(user = "data-user", password = "1234567")
  public void testDataUser() throws Exception {
    assertThatThrownBy(() -> bean.shutDownMember()).hasMessageContaining("CLUSTER:MANAGE");
    assertThatThrownBy(() -> bean.createManager()).hasMessageContaining("CLUSTER:MANAGE");
    assertThatThrownBy(() -> bean.compactAllDiskStores()).hasMessageContaining("DATA:MANAGE");
    assertThatThrownBy(() -> bean.fetchJvmThreads()).hasMessageContaining("CLUSTER:READ");
    assertThatThrownBy(() -> bean.getName()).hasMessageContaining("CLUSTER:READ");
    assertThatThrownBy(() -> bean.getDiskStores()).hasMessageContaining("CLUSTER:READ");
    assertThatThrownBy(() -> bean.hasGatewayReceiver()).hasMessageContaining("CLUSTER:READ");
    assertThatThrownBy(() -> bean.isCacheServer()).hasMessageContaining("CLUSTER:READ");
    assertThatThrownBy(() -> bean.isServer()).hasMessageContaining("CLUSTER:READ");
    assertThatThrownBy(() -> bean.listConnectedGatewayReceivers()).hasMessageContaining("CLUSTER:READ");
    //assertThatThrownBy(() -> bean.processCommand("create region --name=Region_A")).hasMessageContaining("DATA:MANAGE");
    assertThatThrownBy(() -> bean.showJVMMetrics()).hasMessageContaining("CLUSTER:READ");
    assertThatThrownBy(() -> bean.status()).hasMessageContaining("CLUSTER:READ");
  }
}
