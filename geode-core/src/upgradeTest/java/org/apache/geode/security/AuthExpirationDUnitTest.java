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
import static org.apache.geode.test.version.VersionManager.CURRENT_VERSION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class AuthExpirationDUnitTest {

  @Parameterized.Parameter
  public String clientVersion;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    // only test the current version and the version before
    return Arrays.asList(CURRENT_VERSION, "1.13.3");
  }

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();


  @Rule
  public RestoreSystemProperties restore = new RestoreSystemProperties();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_MANAGER, ExpirableSecurityManager.class.getName())
      .withRegion(RegionShortcut.REPLICATE, "region");

  @After
  public void after() {
    // make sure after each test, the values of the ExpirationManager are reset
    ExpirableSecurityManager securityManager =
        (ExpirableSecurityManager) server.getCache().getSecurityService().getSecurityManager();
    securityManager.reset();
  }

  @Test
  public void clientShouldReAuthenticateWhenCredentialExpiredAndOperationSucceed()
      throws Exception {
    ExpirableSecurityManager.EXPIRE_AFTER = 10;
    int serverPort = server.getPort();
    ClientVM clientVM = lsRule.startClientVM(0, clientVersion,
        c -> c.withCredential("data", "data")
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region region = clientRegionFactory.create("region");
      IntStream.range(0, 100).forEach(i -> {
        region.put(i, "value" + i);
      });
    });

    ExpirableSecurityManager securityManager =
        (ExpirableSecurityManager) server.getCache().getSecurityService().getSecurityManager();

    // assert that re-authentication happens
    assertThat(securityManager.isExpired()).isTrue();

    // all put operation succeeded
    Region<Object, Object> region = server.getCache().getRegion("/region");
    assertThat(region.size()).isEqualTo(100);
  }

  @Test
  public void client() throws Exception {}
}
