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
import static org.apache.geode.test.version.VersionManager.CURRENT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
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
  private static RegionService user0Service;
  private static RegionService user1Service;

  @Parameterized.Parameter
  public String clientVersion;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    // only test the current version and the latest released version
    return Arrays.asList(CURRENT_VERSION, "1.13.3");
  }

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public RestoreSystemProperties restore = new RestoreSystemProperties();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withSecurityManager(ExpirableSecurityManager.class)
      .withRegion(RegionShortcut.REPLICATE, "region");

  @Test
  public void clientShouldReAuthenticateWhenCredentialExpiredAndOperationSucceed()
      throws Exception {
    int serverPort = server.getPort();
    ClientVM clientVM = lsRule.startClientVM(0, clientVersion,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      UpdatableUserAuthInitialize.setUser("user1");
      assertThat(clientCache).isNotNull();
      ClientRegionFactory<Object, Object> clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<Object, Object> region = clientRegionFactory.create("region");
      region.put(0, "value0");
    });

    // expire the current user
    ExpirableSecurityManager securityManager = getSecurityManager();
    securityManager.addExpiredUser("user1");

    // do a second put, if this is successful, it means new credentials are provided
    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user2");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      Region<Object, Object> region = clientCache.getRegion("region");
      region.put(1, "value1");
    });

    // all put operation succeeded
    Region<Object, Object> region = server.getCache().getRegion("/region");
    assertThat(region.size()).isEqualTo(2);
    Map<String, List<String>> authorizedOps = securityManager.getAuthorizedOps();
    Map<String, List<String>> unAuthorizedOps = securityManager.getUnAuthorizedOps();
    assertThat(authorizedOps.keySet().size()).isEqualTo(2);
    assertThat(authorizedOps.get("user1")).asList().containsExactly("DATA:WRITE:region:0");
    assertThat(authorizedOps.get("user2")).asList().containsExactly("DATA:WRITE:region:1");
    assertThat(unAuthorizedOps.keySet().size()).isEqualTo(1);
    assertThat(unAuthorizedOps.get("user1")).asList().containsExactly("DATA:WRITE:region:1");
  }

  @Test
  public void userShouldReAuthenticateWhenCredentialExpiredAndOperationSucceed() throws Exception {
    int serverPort = server.getPort();
    ClientVM clientVM = lsRule.startClientVM(0, clientVersion,
        c -> c.withMultiUser(true)
            .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user0");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
      Properties userSecurityProperties = new Properties();
      userSecurityProperties.put(SECURITY_CLIENT_AUTH_INIT,
          UpdatableUserAuthInitialize.class.getName());
      user0Service = clientCache.createAuthenticatedView(userSecurityProperties);
      Region<Object, Object> region = user0Service.getRegion("/region");
      region.put(0, "value0");

      UpdatableUserAuthInitialize.setUser("user1");
      userSecurityProperties.put(SECURITY_CLIENT_AUTH_INIT,
          UpdatableUserAuthInitialize.class.getName());
      user1Service = clientCache.createAuthenticatedView(userSecurityProperties);
      region = user1Service.getRegion("/region");
      region.put(1, "value1");
    });

    ExpirableSecurityManager securityManager = getSecurityManager();
    securityManager.addExpiredUser("user1");
    clientVM.invoke(() -> {

      Region<Object, Object> region = user0Service.getRegion("/region");
      region.put(2, "value3");

      UpdatableUserAuthInitialize.setUser("user1_extended");
      region = user1Service.getRegion("/region");
      region.put(3, "value2");

      user0Service.close();
      user1Service.close();
    });

    Region<Object, Object> region = server.getCache().getRegion("/region");
    assertThat(region.size()).isEqualTo(4);

    Map<String, List<String>> authorizedOps = securityManager.getAuthorizedOps();
    assertThat(authorizedOps.keySet().size()).isEqualTo(3);
    assertThat(authorizedOps.get("user0")).asList().containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:2");
    assertThat(authorizedOps.get("user1")).asList().containsExactly("DATA:WRITE:region:1");
    assertThat(authorizedOps.get("user1_extended")).asList().containsExactly("DATA:WRITE:region:3");

    Map<String, List<String>> unAuthorizedOps = securityManager.getUnAuthorizedOps();
    assertThat(unAuthorizedOps.keySet().size()).isEqualTo(1);
    assertThat(unAuthorizedOps.get("user1")).asList().containsExactly("DATA:WRITE:region:3");
  }

  private ExpirableSecurityManager getSecurityManager() {
    return (ExpirableSecurityManager) server.getCache().getSecurityService().getSecurityManager();
  }
}
