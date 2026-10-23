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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * Exercises MBean creation, registration and invocation through the JMX connector for users with
 * different resource permissions, drawn from {@code cacheServer.json}.
 */
@Category({SecurityTest.class})
public class MBeanServerWrapperJUnitTest {

  private static final String DISTRIBUTED_SYSTEM_MBEAN =
      "GemFire:service=System,type=Distributed";

  private static final ObjectName OTHER_DOMAIN_BEAN = objectName("other:type=timer");

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/cacheServer.json")
      .withAutoStart();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  private static ObjectName objectName(String name) {
    try {
      return new ObjectName(name);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void registerBeanForInvocation() throws Exception {
    MBeanServer platform = MBeanJMXAdapter.mbeanServer;
    if (!platform.isRegistered(OTHER_DOMAIN_BEAN)) {
      platform.createMBean("javax.management.timer.Timer", OTHER_DOMAIN_BEAN);
    }
  }

  @Test
  @ConnectionConfiguration(user = "stranger", password = "1234567")
  public void memberCountReadRequiresClusterRead() throws Exception {
    MBeanServerConnection con = connectionRule.getMBeanServerConnection();
    assertThatThrownBy(
        () -> con.getAttribute(new ObjectName(DISTRIBUTED_SYSTEM_MBEAN), "MemberCount"))
            .hasMessageContaining("CLUSTER:READ");
  }

  @Test
  @ConnectionConfiguration(user = "cluster-reader", password = "1234567")
  public void clusterReaderCanReadMemberCount() throws Exception {
    MBeanServerConnection con = connectionRule.getMBeanServerConnection();
    Object memberCount = con.getAttribute(new ObjectName(DISTRIBUTED_SYSTEM_MBEAN), "MemberCount");
    assertThat(((Number) memberCount).intValue()).isGreaterThanOrEqualTo(1);
  }

  @Test
  @ConnectionConfiguration(user = "stranger", password = "1234567")
  public void createMBeanInOtherDomainRequiresClusterManage() throws Exception {
    MBeanServerConnection con = connectionRule.getMBeanServerConnection();
    ObjectName name = objectName("other:type=created");
    assertThatThrownBy(() -> con.createMBean("javax.management.timer.Timer", name))
        .satisfiesAnyOf(
            t -> assertThat(t).isInstanceOf(SecurityException.class),
            t -> assertThat(t).hasMessageContaining("not authorized"),
            t -> assertThat(t).hasMessageContaining("Access Denied"));
  }

  @Test
  @ConnectionConfiguration(user = "stranger", password = "1234567")
  public void invokeInOtherDomainRequiresClusterManage() throws Exception {
    MBeanServerConnection con = connectionRule.getMBeanServerConnection();
    assertThatThrownBy(
        () -> con.invoke(OTHER_DOMAIN_BEAN, "removeAllNotifications", new Object[] {},
            new String[] {}))
                .satisfiesAnyOf(
                    t -> assertThat(t).isInstanceOf(SecurityException.class),
                    t -> assertThat(t).hasMessageContaining("not authorized"),
                    t -> assertThat(t).hasMessageContaining("Access Denied"));
  }
}
