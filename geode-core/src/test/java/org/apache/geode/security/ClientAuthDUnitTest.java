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

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
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
      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName())
      .withRegion(RegionShortcut.REPLICATE, "region");

  @Test
  public void authWithCorrectPasswordShouldPass() throws Exception {
    ClientVM clientVM = lsRule.startClientVM(0, "data", "data", true, server.getPort());
    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region region = clientRegionFactory.create("region");
      region.put("A", "A");
    });
  }

  @Test
  public void authWithIncorrectPasswordWithSubscriptionEnabled() throws Exception {
    IgnoredException.addIgnoredException(AuthenticationFailedException.class.getName());
    ClientVM clientVM = lsRule.startClientVM(0, "test", "invalidPassword", true, server.getPort());
    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      assertThatThrownBy(() -> clientRegionFactory.create("region"))
          .isInstanceOf(AuthenticationFailedException.class);
    });
  }

  @Test
  public void authWithIncorrectPasswordWithSubscriptionNotEnabled() throws Exception {
    IgnoredException.addIgnoredException(AuthenticationFailedException.class.getName());
    ClientVM clientVM = lsRule.startClientVM(0, "test", "invalidPassword", false, server.getPort());
    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region region = clientRegionFactory.create("region");
      assertThatThrownBy(() -> region.put("A", "A")).isInstanceOf(ServerOperationException.class)
          .hasCauseInstanceOf(AuthenticationFailedException.class);
    });
  }
}
