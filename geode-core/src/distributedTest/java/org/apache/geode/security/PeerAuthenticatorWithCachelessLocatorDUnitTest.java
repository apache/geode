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

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.templates.DummyAuthenticator;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class PeerAuthenticatorWithCachelessLocatorDUnitTest extends JUnit4DistributedTestCase {
  protected VM locator = null;
  protected VM server = null;
  protected VM server1 = null;

  @Before
  public void before() throws Exception {
    final Host host = Host.getHost(0);
    locator = host.getVM(0);
    server = host.getVM(1);
    server1 = host.getVM(2);
  }

  @Test
  public void testPeerAuthenticator() throws Exception {
    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    locator.invoke(() -> {
      Properties props = new Properties();
      props.setProperty(SECURITY_PEER_AUTHENTICATOR, DummyAuthenticator.class.getName());
      props.setProperty(MCAST_PORT, "0");
      props.put(JMX_MANAGER, "true");
      props.put(JMX_MANAGER_START, "true");
      props.put(JMX_MANAGER_PORT, "0");
      props.setProperty("start-locator", "localhost[" + locatorPort + "]");
      DistributedSystem.connect(props);
    });

    // set up server with security
    String locators = "localhost[" + locatorPort + "]";
    server.invoke(() -> {
      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, locators);

      // the following are needed for peer-to-peer authentication
      props.setProperty("security-username", "user");
      props.setProperty("security-password", "user");
      // this should execute without exception
      InternalDistributedSystem ds = getSystem(props);
    });

    server1.invoke(() -> {
      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, locators);

      // the following are needed for peer-to-peer authentication
      props.setProperty("security-username", "bogus");
      props.setProperty("security-password", "user");

      assertThatThrownBy(() -> getSystem(props)).isInstanceOf(GemFireSecurityException.class);
    });
  }

}
