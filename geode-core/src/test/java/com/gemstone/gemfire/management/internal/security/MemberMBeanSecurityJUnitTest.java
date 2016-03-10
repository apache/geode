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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.management.MBeanServerConnection;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MemberMBeanSecurityJUnitTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private MemberMXBean bean;
  private MBeanServerConnection con;

  @ClassRule
  public static JsonAuthorizationMBeanServerStartRule serverRule = new JsonAuthorizationMBeanServerStartRule(jmxManagerPort, "cacheServer.json");

  @Rule
  public MBeanServerConnectionRule<MemberMXBean> connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(MemberMXBean.class);
    con = connectionRule.getMBeanServerConnection();
  }

  @Test
  @JMXConnectionConfiguration(user = "superuser", password = "1234567")
  public void testAllAccess() throws Exception {
    bean.shutDownMember();  //SHUTDOWN
    bean.compactAllDiskStores(); //COMPACT_DISKSTORE
    bean.createManager(); //CREATE_MANAGER
    bean.fetchJvmThreads(); //LIST_DS
    bean.getName(); //LIST_DS
    bean.getDiskStores(); //LIST_DS
    bean.hasGatewayReceiver(); //LIST_DS
    bean.isCacheServer(); //LIST_DS
    bean.isServer(); //LIST_DS
    bean.listConnectedGatewayReceivers(); //LIST_DS
    bean.processCommand("create region --name=Region_A"); //CREATE_REGION
    bean.showJVMMetrics(); //LIST_DS
    bean.status(); //LIST_DS
  }

  @Test
  @JMXConnectionConfiguration(user = "user", password = "1234567")
  public void testSomeAccess() throws Exception {

  }

  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> bean.shutDownMember()).isInstanceOf(SecurityException.class);
  }

}
