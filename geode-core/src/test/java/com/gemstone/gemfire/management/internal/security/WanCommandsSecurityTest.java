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
public class WanCommandsSecurityTest {
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
  @JMXConnectionConfiguration(user = "adminUser", password = "1234567")
  public void testAdminUser() throws Exception {
    bean.processCommand("create gateway-sender --id=sender1 --remote-distributed-system-id=2");
    bean.processCommand("start gateway-sender --id=sender1");
    bean.processCommand("pause gateway-sender --id=sender1");
    bean.processCommand("resume gateway-sender --id=sender1");
    bean.processCommand("stop gateway-sender --id=sender1");
    bean.processCommand("list gateways");
    bean.processCommand("create gateway-receiver");
    bean.processCommand("start gateway-receiver");
    bean.processCommand("stop gateway-receiver");
    bean.processCommand("status gateway-receiver");
  }

  // dataUser has all the permissions granted, but not to region2 (only to region1)
  @Test
  @JMXConnectionConfiguration(user = "dataUser", password = "1234567")
  public void testNoAccess(){
    assertThatThrownBy(() -> bean.processCommand("create gateway-sender --id=sender1 --remote-distributed-system-id=2"))
        .isInstanceOf(SecurityException.class)
        .hasMessageStartingWith("Access Denied: Not authorized for GATEWAY_SENDER:CREATE");

    assertThatThrownBy(() -> bean.processCommand("start gateway-sender --id=sender1"))
        .isInstanceOf(SecurityException.class)
        .hasMessageStartingWith("Access Denied: Not authorized for GATEWAY_SENDER:START");

    assertThatThrownBy(() -> bean.processCommand("pause gateway-sender --id=sender1"))
        .isInstanceOf(SecurityException.class)
        .hasMessageStartingWith("Access Denied: Not authorized for GATEWAY_SENDER:PAUSE");

    assertThatThrownBy(() -> bean.processCommand("resume gateway-sender --id=sender1"))
        .isInstanceOf(SecurityException.class)
        .hasMessageStartingWith("Access Denied: Not authorized for GATEWAY_SENDER:RESUME");

    assertThatThrownBy(() -> bean.processCommand("stop gateway-sender --id=sender1"))
        .isInstanceOf(SecurityException.class)
        .hasMessageStartingWith("Access Denied: Not authorized for GATEWAY_SENDER:STOP");

    bean.processCommand("list gateways");

    assertThatThrownBy(() -> bean.processCommand("create gateway-receiver"))
        .isInstanceOf(SecurityException.class)
        .hasMessageStartingWith("Access Denied: Not authorized for GATEWAY_RECEIVER:CREATE");

    assertThatThrownBy(() -> bean.processCommand("start gateway-receiver"))
        .isInstanceOf(SecurityException.class)
        .hasMessageStartingWith("Access Denied: Not authorized for GATEWAY_RECEIVER:START");

    assertThatThrownBy(() -> bean.processCommand("stop gateway-receiver"))
        .isInstanceOf(SecurityException.class)
        .hasMessageStartingWith("Access Denied: Not authorized for GATEWAY_RECEIVER:STOP");

    assertThatThrownBy(() -> bean.processCommand("status gateway-receiver"))
        .isInstanceOf(SecurityException.class)
        .hasMessageStartingWith("Access Denied: Not authorized for GATEWAY_RECEIVER:STATUS");
  }

}
