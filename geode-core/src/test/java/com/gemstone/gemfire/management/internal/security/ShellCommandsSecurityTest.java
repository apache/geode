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
public class ShellCommandsSecurityTest {
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

  @JMXConnectionConfiguration(user = "superuser", password = "1234567")
  @Test
  public void testAllAccess(){
    bean.processCommand("connect");
    bean.processCommand("debug --state=on");
    bean.processCommand("describe connection");
    bean.processCommand("echo --string=\"Hello World!\"");
    bean.processCommand("encrypt password --password=value");
    bean.processCommand("version");
    bean.processCommand("sleep");
    bean.processCommand("sh ls");
    bean.processCommand("disconnect");
  }

  // stranger has no permission granted
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  @Test
  public void testNoAccess(){
    assertThatThrownBy(() -> bean.processCommand("connect")).isInstanceOf(SecurityException.class).hasMessageContaining("DISTRIBUTED_SYSTEM:ALL");
    assertThatThrownBy(() -> bean.processCommand("debug --state=on")).isInstanceOf(SecurityException.class).hasMessageContaining("DISTRIBUTED_SYSTEM:ALL");
    assertThatThrownBy(() -> bean.processCommand("describe connection")).isInstanceOf(SecurityException.class).hasMessageContaining("DISTRIBUTED_SYSTEM:ALL");
    assertThatThrownBy(() -> bean.processCommand("disconnect")).isInstanceOf(SecurityException.class).hasMessageContaining("DISTRIBUTED_SYSTEM:ALL");
    assertThatThrownBy(() -> bean.processCommand("echo --string=\"Hello World!\"")).isInstanceOf(SecurityException.class).hasMessageContaining("DISTRIBUTED_SYSTEM:ALL");
    assertThatThrownBy(() -> bean.processCommand("encrypt password --password=value")).isInstanceOf(SecurityException.class).hasMessageContaining("DISTRIBUTED_SYSTEM:ALL");
    assertThatThrownBy(() -> bean.processCommand("version")).isInstanceOf(SecurityException.class).hasMessageContaining("DISTRIBUTED_SYSTEM:ALL");
    assertThatThrownBy(() -> bean.processCommand("sleep")).isInstanceOf(SecurityException.class).hasMessageContaining("DISTRIBUTED_SYSTEM:ALL");
    assertThatThrownBy(() -> bean.processCommand("sh ls")).isInstanceOf(SecurityException.class).hasMessageContaining("DISTRIBUTED_SYSTEM:ALL");
  }

}
