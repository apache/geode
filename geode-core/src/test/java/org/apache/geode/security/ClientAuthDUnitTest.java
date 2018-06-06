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
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.io.Serializable;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

// flaky: GEODE-3692
@Category({DistributedTest.class, SecurityTest.class, FlakyTest.class})
public class ClientAuthDUnitTest {
  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName()).withAutoStart();

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void authWithCorrectPasswordShouldPass() throws Exception {
    lsRule.startClientVM(0, "test", "test", true, server.getPort(), new ClientCacheHook(lsRule));
  }

  @Test
  public void authWithIncorrectPasswordShouldFail() throws Exception {
    IgnoredException.addIgnoredException(AuthenticationFailedException.class.getName());

    assertThatThrownBy(() -> lsRule.startClientVM(0, "test", "invalidPassword", true,
        server.getPort(), new ClientCacheHook(lsRule)))
            .isInstanceOf(AuthenticationFailedException.class);
  }

  static class ClientCacheHook implements Runnable, Serializable {
    final ClusterStartupRule lsRule;

    ClientCacheHook(ClusterStartupRule lsRule) {
      this.lsRule = lsRule;
    }

    public void run() {
      // Perform an operation that causes the cache to lazy-initialize a pool with the invalid
      // authentication so as to induce the exception.
      ClientCache clientCache = lsRule.getClientCache();
      ClientRegionFactory clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      clientRegionFactory.create("region");
    }
  }
}
