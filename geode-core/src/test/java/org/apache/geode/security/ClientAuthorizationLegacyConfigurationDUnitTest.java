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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.security.templates.SimpleAccessController;
import org.apache.geode.security.templates.SimpleAuthenticator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClientAuthorizationLegacyConfigurationDUnitTest {

  @Rule
  public ClusterStartupRule csRule = new ClusterStartupRule();

  private MemberVM locator;
  private MemberVM server;
  private static String regionName = "testRegion";

  // Some data values against which we will test.
  private static final String initKey = "server-placed-key";
  private static final String initValue = "server-placed-value";
  private static final String singleKey = "single-key";
  private static final String singleValue = "single-value";
  private static final String mapKey1 = "map-key1";
  private static final String mapValue1 = "map-value1";
  private static final String mapKey2 = "map-key2";
  private static final String mapValue2 = "map-value2";
  private static Map<String, String> keyValueMap = new HashMap<>();
  static {
    keyValueMap.put(mapKey1, mapValue1);
    keyValueMap.put(mapKey2, mapValue2);
  }

  // Using a client of every version...
  @Parameterized.Parameter
  public String clientVersion;

  @Parameterized.Parameters(name = "clientVersion={0}")
  public static Collection<String> data() {
    return VersionManager.getInstance().getVersions();
  }

  @Test
  public void everythingFailsWithInvalidAuthenticator() throws Exception {
    Properties clusterProps = new Properties();
    clusterProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR,
        "org.apache.geode.no.such.authenticator.create");
    clusterProps.setProperty(SECURITY_CLIENT_ACCESSOR,
        SimpleAccessController.class.getName() + ".create");
    clusterProps.setProperty(UserPasswordAuthInit.USER_NAME, "cluster,data");
    clusterProps.setProperty(UserPasswordAuthInit.PASSWORD, "cluster,data");
    clusterProps.setProperty(SECURITY_CLIENT_AUTH_INIT,
        UserPasswordAuthInit.class.getCanonicalName() + ".create");

    locator = csRule.startLocatorVM(0, clusterProps);
    server = csRule.startServerVM(1, clusterProps, locator.getPort());
    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.PARTITION);
      Region<String, String> region = rf.create(regionName);
      region.put(initKey, initValue);
    });

    Properties clientProps = new Properties();
    clusterProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR,
        "org.apache.geode.no.such.authenticator.create");
    clusterProps.setProperty(SECURITY_CLIENT_ACCESSOR,
        SimpleAccessController.class.getName() + ".create");
    clientProps.setProperty(UserPasswordAuthInit.USER_NAME, "data");
    clientProps.setProperty(UserPasswordAuthInit.PASSWORD, "data");
    clientProps.setProperty(SECURITY_CLIENT_AUTH_INIT,
        UserPasswordAuthInit.class.getCanonicalName() + ".create");

    int locatorPort = locator.getPort();

    ClientVM client = csRule.startClientVM(2, clientProps, cf -> cf
        .addPoolLocator("localhost", locatorPort), clientVersion);

    client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<String, String> rf =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region = rf.create(regionName);

      // Assert that everything is horrible
      assertThatThrownBy(() -> region.get(initKey))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.get(singleKey, null))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.getAll(keyValueMap.keySet()))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.getAll(keyValueMap.keySet(), null))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.put(singleKey, singleValue))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.put(singleKey, singleValue, null))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.putAll(keyValueMap))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.putAll(keyValueMap, null))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
    });
  }

  @Test
  public void everythingFailsWithInvalidAccessor() throws Exception {
    Properties clusterProps = new Properties();
    clusterProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR,
        SimpleAuthenticator.class.getCanonicalName() + ".create");
    clusterProps.setProperty(SECURITY_CLIENT_ACCESSOR, "org.apache.geode.no.such.accessor.create");
    // give cluster members super-user permissions for ease of testing / RMI invocation
    clusterProps.setProperty(UserPasswordAuthInit.USER_NAME, "cluster,data");
    clusterProps.setProperty(UserPasswordAuthInit.PASSWORD, "cluster,data");
    clusterProps.setProperty(SECURITY_CLIENT_AUTH_INIT,
        UserPasswordAuthInit.class.getCanonicalName() + ".create");

    locator = csRule.startLocatorVM(0, clusterProps);
    server = csRule.startServerVM(1, clusterProps, locator.getPort());
    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.PARTITION);
      Region<String, String> region = rf.create(regionName);
      region.put(initKey, initValue);
    });

    Properties clientProps = new Properties();
    clientProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR,
        SimpleAuthenticator.class.getCanonicalName() + ".create");
    clientProps.setProperty(SECURITY_CLIENT_ACCESSOR, "org.apache.geode.no.such.accessor.create");
    // give cluster members super-user permissions for ease of testing / RMI invocation
    clientProps.setProperty(UserPasswordAuthInit.USER_NAME, "data");
    clientProps.setProperty(UserPasswordAuthInit.PASSWORD, "data");
    clientProps.setProperty(SECURITY_CLIENT_AUTH_INIT,
        UserPasswordAuthInit.class.getCanonicalName() + ".create");

    int locatorPort = locator.getPort();

    ClientVM client = csRule.startClientVM(2, clientProps, cf -> cf
        .addPoolLocator("localhost", locatorPort), clientVersion);
    client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<String, String> rf =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region = rf.create(regionName);

      // Assert that everything is horrible
      assertThatThrownBy(() -> region.get(initKey))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.get(singleKey, null))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.getAll(keyValueMap.keySet()))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.getAll(keyValueMap.keySet(), null))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.put(singleKey, singleValue))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.put(singleKey, singleValue, null))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.putAll(keyValueMap))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
      assertThatThrownBy(() -> region.putAll(keyValueMap, null))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
    });
  }


}
