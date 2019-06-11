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

import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class JmxMultipleConnectionsTest {
  private MemberMXBean bean;

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/cacheServer.json")
      .withAutoStart();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Cache cache = server.getCache();
    cache.createRegionFactory().create("region1");
  }

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMXBean(MemberMXBean.class);
  }

  @Test
  @ConnectionConfiguration(user = "region1-user", password = "1234567")
  public void testMultipleJMXConnection() throws Exception {

    assertThat(bean.processCommand("locate entry --key=k1 --region=region1")).isNotNull();

    JMXServiceURL url =
        new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:" + server.getJmxPort() + "/jmxrmi");
    Map<String, String[]> env = new HashMap<>();
    String user = "region1-user";
    String password = "1234567";
    env.put(JMXConnector.CREDENTIALS, new String[] {user, password});

    // create another JMX connection with the same credentials
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, env);

    assertThat(bean.processCommand("locate entry --key=k1 --region=region1")).isNotNull();

    // close one jmx connection
    jmxConnector.close();

    assertThat(bean.processCommand("locate entry --key=k1 --region=region1")).isNotNull();
  }
}
