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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.Properties;
import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({DistributedTest.class, SecurityTest.class})
public class ClientAuthDUnitTest {
  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName()).withAutoStart();

  @Test
  public void authWithCorrectPasswordShouldPass() throws Exception {
    startClientWithUsernameAndPassword(0, "test", "test");
  }

  @Test
  public void authWithIncorrectPasswordShouldFail() throws Exception {
    IgnoredException.addIgnoredException(AuthenticationFailedException.class.getName());

    assertThatThrownBy(() -> startClientWithUsernameAndPassword(0, "test", "invalidPassword"))
        .isInstanceOf(AuthenticationFailedException.class);
  }

  private ClientVM startClientWithUsernameAndPassword(int index, String username, String password)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, username);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());

    Consumer<ClientCacheFactory> consumer =
        (Serializable & Consumer<ClientCacheFactory>) ((cacheFactory) -> {
          cacheFactory.setPoolSubscriptionEnabled(true);
          for (int serverPort : new int[] {server.getPort()}) {
            cacheFactory.addPoolServer("localhost", serverPort);
          }
        });
    return lsRule.startClientVM(index, props, consumer);
  }

}
