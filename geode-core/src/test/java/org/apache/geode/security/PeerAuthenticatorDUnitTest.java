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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.templates.DummyAuthenticator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.ServerStarter;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class PeerAuthenticatorDUnitTest extends JUnit4DistributedTestCase {
  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(SECURITY_PEER_AUTHENTICATOR, DummyAuthenticator.class.getName());
    lsRule.getLocatorVM(0, props);
  }

  @Test
  public void testPeerAuthenticator() throws Exception {

    int locatorPort = lsRule.getPort(0);
    Properties server1Props = new Properties();
    server1Props.setProperty("security-username", "user");
    server1Props.setProperty("security-password", "user");
    lsRule.getServerVM(1, server1Props, locatorPort);


    Properties server2Props = new Properties();
    server2Props.setProperty("security-username", "bogus");
    server2Props.setProperty("security-password", "user");
    VM server2 = lsRule.getNodeVM(2);

    server2.invoke(() -> {
      ServerStarter serverStarter = new ServerStarter(server2Props);
      assertThatThrownBy(() -> serverStarter.startServer(locatorPort))
          .isInstanceOf(GemFireSecurityException.class).hasMessageContaining("Invalid user name");
    });
  }

}
