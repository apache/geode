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

import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({DistributedTest.class, SecurityTest.class})
public class StartServerAuthorizationTest {

  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();
  private static Locator locator = null;

  @Rule
  public ServerStarterRule serverStarter = new ServerStarterRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    locator = lsRule.startLocatorVM(0, props);
  }

  @Test
  public void testStartServerWithInvalidCredential() throws Exception {
    Properties props = new Properties();
    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "wrongPswd");

    assertThatThrownBy(() -> serverStarter.startServer(props, locator.getPort()))
        .isInstanceOf(GemFireSecurityException.class).hasMessageContaining(
            "Security check failed. Authentication error. Please check your credentials");
  }

  @Test
  public void testStartServerWithInsufficientPrevilage() throws Exception {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "user");

    assertThatThrownBy(() -> serverStarter.startServer(props, locator.getPort()))
        .isInstanceOf(GemFireSecurityException.class)
        .hasMessageContaining("user not authorized for CLUSTER:MANAGE");
  }

  @Test
  public void testStartServerWithSufficientPrevilage() throws Exception {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");

    lsRule.startServerVM(1, props);
  }

}
