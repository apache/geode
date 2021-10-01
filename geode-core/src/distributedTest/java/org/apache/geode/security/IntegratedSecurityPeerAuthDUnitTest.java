/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * this test specifically uses a peer-auth-init that doesn't produce credentials that has
 * security-username or security-password. It produces a property that its SecurityManager
 * knows how to authenticate/authorize. It's just to show that we can pass in properties
 * other than security-username and security-password. But this will be doable only in peer/client
 * case. For gfsh/rest, we still expected the credentials to be wrapped as expected.
 */
@Category({SecurityTest.class})
public class IntegratedSecurityPeerAuthDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties properties = new Properties();
    properties.put(SECURITY_MANAGER, MySecurityManager.class.getName());
    locator = cluster.startLocatorVM(0, properties);
  }

  @Test
  public void startServer1WithPeerAuthInit_success() throws IOException {
    Properties props = new Properties();
    props.setProperty(SECURITY_PEER_AUTH_INIT, MyAuthInit.class.getName());
    props.setProperty("security-name", "server-1");
    cluster.startServerVM(1, props, locator.getPort());
  }

  @Test
  public void startServer2_not_authorized() {
    Properties props = new Properties();
    props.setProperty(SECURITY_PEER_AUTH_INIT, MyAuthInit.class.getName());
    props.setProperty("security-name", "server-2");
    int locatorPort = locator.getPort();
    cluster.getVM(2).invoke(() -> {
      ServerStarterRule server = new ServerStarterRule();
      server.withProperties(props).withConnectionToLocator(locatorPort).withAutoStart();
      assertThatThrownBy(() -> server.before()).isInstanceOf(GemFireSecurityException.class)
          .hasMessageContaining("server-2 not authorized for CLUSTER:MANAGE");
    });
  }

  @Test
  public void startServer3_not_authenticated() {
    Properties props = new Properties();
    props.setProperty(SECURITY_PEER_AUTH_INIT, MyAuthInit.class.getName());
    props.setProperty("security-name", "server-3");
    int locatorPort = locator.getPort();
    cluster.getVM(3).invoke(() -> {
      ServerStarterRule server = new ServerStarterRule();
      server.withProperties(props).withConnectionToLocator(locatorPort).withAutoStart();
      assertThatThrownBy(() -> server.before()).isInstanceOf(GemFireSecurityException.class)
          .hasMessageContaining("server-3 is not authenticated");
    });
  }

  public static class MyAuthInit implements AuthInitialize {
    @Override
    public Properties getCredentials(Properties securityProps, DistributedMember server,
        boolean isPeer) throws AuthenticationFailedException {
      Properties properties = new Properties();
      properties.setProperty("name", securityProps.getProperty("security-name"));
      return properties;
    }
  }

  public static class MySecurityManager implements SecurityManager {
    @Override
    public Object authenticate(Properties credentials) throws AuthenticationFailedException {
      // server1 and server2 are authenticated, server3 is not
      String name = credentials.getProperty("name");
      if ("server-3".equals(name)) {
        throw new AuthenticationFailedException("server-3 is not authenticated");
      }
      return name;
    }

    @Override
    public boolean authorize(Object principal, ResourcePermission permission) {
      // server-1 and server-2 are authenticated, but only server-1 is authorized
      return ("server-1".equals(principal));
    }
  }
}
