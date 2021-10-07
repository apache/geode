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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.templates.DummyAuthenticator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class PeerSecurityWithEmbeddedLocatorDUnitTest {

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();


  @Test
  public void testPeerSecurityManager() throws Exception {
    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    Properties server0Props = new Properties();
    server0Props.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    server0Props.setProperty("start-locator", "localhost[" + locatorPort + "]");
    lsRule.startServerVM(0, server0Props);


    Properties server1Props = new Properties();
    server1Props.setProperty("security-username", "cluster");
    server1Props.setProperty("security-password", "cluster");
    lsRule.startServerVM(1, server1Props, locatorPort);

    Properties server2Props = new Properties();
    server2Props.setProperty("security-username", "user");
    server2Props.setProperty("security-password", "wrongPwd");

    VM server2 = getHost(0).getVM(2);
    server2.invoke(() -> {
      ServerStarterRule serverStarter = new ServerStarterRule();
      ClusterStartupRule.memberStarter = serverStarter;
      assertThatThrownBy(() -> serverStarter.startServer(server2Props, locatorPort))
          .isInstanceOf(GemFireSecurityException.class)
          .hasMessageContaining("Security check failed. invalid username/password");
    });
  }



  @Test
  public void testPeerAuthenticator() throws Exception {
    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    Properties server0Props = new Properties();
    server0Props.setProperty(SECURITY_PEER_AUTHENTICATOR, DummyAuthenticator.class.getName());
    server0Props.setProperty("start-locator", "localhost[" + locatorPort + "]");
    lsRule.startServerVM(0, server0Props);


    Properties server1Props = new Properties();
    server1Props.setProperty("security-username", "user");
    server1Props.setProperty("security-password", "user");
    lsRule.startServerVM(1, server1Props, locatorPort);

    Properties server2Props = new Properties();
    server2Props.setProperty("security-username", "bogus");
    server2Props.setProperty("security-password", "user");

    VM server2 = getHost(0).getVM(2);
    server2.invoke(() -> {
      ServerStarterRule serverStarter = new ServerStarterRule();
      ClusterStartupRule.memberStarter = serverStarter;
      assertThatThrownBy(() -> serverStarter.startServer(server2Props, locatorPort))
          .isInstanceOf(GemFireSecurityException.class).hasMessageContaining("Invalid user name");
    });
  }

}
