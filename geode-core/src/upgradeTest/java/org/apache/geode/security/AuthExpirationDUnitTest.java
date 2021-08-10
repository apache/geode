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
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
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
import org.apache.geode.cache.client.ServerOperationException;
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

  @After
  public void after() {
    // make sure after each test, the values of the ExpirationManager are reset
    ExpirableSecurityManager.reset();
  }

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
      assert clientCache != null;
      ClientRegionFactory<Object, Object> clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<Object, Object> region = clientRegionFactory.create("region");
      region.put(0, "value0");
    });

    // expire the current user
    ExpirableSecurityManager.addExpiredUser("user1");

    // do a second put, if this is successful, it means new credentials are provided
    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user2");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assert clientCache != null;
      Region<Object, Object> region = clientCache.getRegion("region");
      region.put(1, "value1");
    });

    // all put operation succeeded
    Region<Object, Object> region = server.getCache().getRegion("/region");
    assertThat(ExpirableSecurityManager.getExpiredUsers().size()).isEqualTo(1);
    assertThat(ExpirableSecurityManager.getExpiredUsers().contains("user1")).isTrue();
    assertThat(region.size()).isEqualTo(2);
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
      UpdatableUserAuthInitialize.setUser("user1");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assert clientCache != null;
      clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
      Properties userSecurityProperties = new Properties();
      userSecurityProperties.put("security-username", "serviceUser0");
      userSecurityProperties.put("security-password", "serviceUser0");
      RegionService regionService = clientCache.createAuthenticatedView(userSecurityProperties);
      Region<Object, Object> region = regionService.getRegion("/region");
      region.put(0, "value0");
    });

    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Properties userSecurityProperties = new Properties();
      userSecurityProperties.put("security-username", "serviceUser1");
      userSecurityProperties.put("security-password", "serviceUser1");
      assert clientCache != null;
      RegionService regionService = clientCache.createAuthenticatedView(userSecurityProperties);
      Region<Object, Object> region = regionService.getRegion("/region");
      region.put(1, "value1");
      regionService.close();
    });

    ExpirableSecurityManager.addExpiredUser("serviceUser1");

    List<String> errorInfo = clientVM.invoke(() -> {
      List<String> eInfo = new ArrayList<>();
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Properties userSecurityProperties = new Properties();
      userSecurityProperties.put("security-username", "serviceUser1");
      userSecurityProperties.put("security-password", "serviceUser1");
      assert clientCache != null;
      RegionService regionService = clientCache.createAuthenticatedView(userSecurityProperties);
      Region<Object, Object> region = regionService.getRegion("/region");
      try {
        region.put(2, "value2");
      } catch (ServerOperationException soe) {
        eInfo.add(soe.getMessage());
        eInfo.add(soe.getClass().getSimpleName());
        eInfo.add(soe.getCause().getClass().getSimpleName());
        Arrays.stream(soe.getStackTrace()).forEach(element -> eInfo.add(element.toString()));
        userSecurityProperties.put("security-username", "serviceUser2");
        userSecurityProperties.put("security-password", "serviceUser2");
        regionService.close();
        regionService = clientCache.createAuthenticatedView(userSecurityProperties);
        region = regionService.getRegion("/region");
        region.put(2, "value2");
      }
      regionService.close();
      return eInfo;
    });

    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Properties userSecurityProperties = new Properties();
      userSecurityProperties.put("security-username", "serviceUser0");
      userSecurityProperties.put("security-password", "serviceUser0");
      assert clientCache != null;
      RegionService regionService = clientCache.createAuthenticatedView(userSecurityProperties);
      Region<Object, Object> region = regionService.getRegion("/region");
      region.put(3, "value3");
      regionService.close();
    });

    Region<Object, Object> region = server.getCache().getRegion("/region");
    assertThat(ExpirableSecurityManager.getExpiredUsers().size()).isEqualTo(1);
    assertThat(ExpirableSecurityManager.getExpiredUsers().contains("serviceUser1")).isTrue();
    Map<Object, List<ResourcePermission>> authorizedOps =
        ExpirableSecurityManager.getAuthorizedOps();
    assertThat(authorizedOps.size()).isEqualTo(3);
    assertThat(authorizedOps.get("serviceUser0").size()).isEqualTo(2);
    assertThat(authorizedOps.get("serviceUser1").size()).isEqualTo(1);
    assertThat(authorizedOps.get("serviceUser2").size()).isEqualTo(1);
    assertThat(region.size()).isEqualTo(4);
    assertThat(errorInfo.size()).isGreaterThan(0);
  }

}
