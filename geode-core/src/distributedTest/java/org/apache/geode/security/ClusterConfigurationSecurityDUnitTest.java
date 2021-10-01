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

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class ClusterConfigurationSecurityDUnitTest {

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public ServerStarterRule serverStarter = new ServerStarterRule();


  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties properties = new Properties();
    properties.put(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    locator = lsRule.startLocatorVM(0, properties);
  }

  @Test
  public void startServerWithNoCredentialWouldFail() throws Exception {
    assertThatThrownBy(() -> serverStarter.startServer(new Properties(), locator.getPort()))
        .isInstanceOf(AuthenticationRequiredException.class)
        .hasMessageContaining("Failed to find credentials");
  }

  @Test
  public void startServerWithInvalidCredentialWouldfail() throws Exception {
    Properties properties = new Properties();
    properties.put("security-username", "test");
    properties.put("security-password", "invalidPassword");
    assertThatThrownBy(() -> serverStarter.startServer(properties, locator.getPort()))
        .isInstanceOf(GemFireSecurityException.class)
        .hasMessageContaining("Security check failed. invalid username/password");
  }

  @Test
  public void startServerWithInsufficientCredential() throws Exception {
    Properties properties = new Properties();
    properties.put("security-username", "test");
    properties.put("security-password", "test");
    assertThatThrownBy(() -> serverStarter.startServer(properties, locator.getPort()))
        .isInstanceOf(GemFireSecurityException.class)
        .hasMessageContaining("Security check failed. test not authorized for CLUSTER:MANAGE");
  }

  @Test
  public void startServerWithValidCredential() throws Exception {
    Properties properties = new Properties();
    properties.put("security-username", "clusterManage");
    properties.put("security-password", "clusterManage");
    serverStarter.startServer(properties, locator.getPort());
  }
}
