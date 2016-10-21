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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.templates.SimpleSecurityManager;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.ServerStarter;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class StartServerAuthorizationTest extends JUnit4DistributedTestCase {

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    lsRule.getLocatorVM(0, props);
  }

  @Test
  public void testStartServerWithInvalidCredential() throws Exception {
    Properties props = new Properties();
    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "wrongPswd");

    VM server = lsRule.getNodeVM(1);
    server.invoke(() -> {
      ServerStarter serverStarter = new ServerStarter(props);
      assertThatThrownBy(() -> serverStarter.startServer(lsRule.getPort(0)))
          .isInstanceOf(GemFireSecurityException.class).hasMessageContaining(
              "Security check failed. Authentication error. Please check your credentials");
    });
  }

  @Test
  public void testStartServerWithInsufficientPrevilage() throws Exception {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "user");
    props.setProperty("security-password", "user");

    VM server = lsRule.getNodeVM(1);
    server.invoke(() -> {
      ServerStarter serverStarter = new ServerStarter(props);
      assertThatThrownBy(() -> serverStarter.startServer(lsRule.getPort(0)))
          .isInstanceOf(GemFireSecurityException.class)
          .hasMessageContaining("user not authorized for CLUSTER:MANAGE");
    });


  }

  @Test
  public void testStartServerWithSufficientPrevilage() throws Exception {
    Properties props = new Properties();

    // the following are needed for peer-to-peer authentication
    props.setProperty("security-username", "cluster");
    props.setProperty("security-password", "cluster");

    lsRule.getServerVM(1, props);
  }

}
