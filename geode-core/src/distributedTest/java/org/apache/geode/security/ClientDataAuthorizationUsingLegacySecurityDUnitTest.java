/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
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
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.security.templates.SimpleAccessController;
import org.apache.geode.security.templates.SimpleAuthenticator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.security.templates.UsernamePrincipal;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Tests for authorization from client to server for data puts and gets. For similar test in the
 * case of failover, see {@link ClientDataAuthorizationUsingLegacySecurityWithFailoverDUnitTest}.
 *
 * @since GemFire 5.5
 */
@Category({DistributedTest.class, SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClientDataAuthorizationUsingLegacySecurityDUnitTest {
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

  @Before
  public void setup() throws Exception {
    // We want the cluster VMs to be super-users for ease of testing / remote invocation.
    Properties clusterMemberProperties = getVMPropertiesWithPermission("cluster,data");

    int version = Integer.parseInt(clientVersion);
    if (version == 0 || version >= 140) {
      clusterMemberProperties.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.security.templates.UsernamePrincipal");
    }

    locator = csRule.startLocatorVM(0, clusterMemberProperties);
    server = csRule.startServerVM(1, clusterMemberProperties, locator.getPort());
    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.PARTITION);
      Region<String, String> region = rf.create(regionName);
      region.put(initKey, initValue);
    });
  }

  @Test
  public void dataWriteClientCanPut() throws Exception {
    Properties props = getVMPropertiesWithPermission("dataWrite");
    int locatorPort = locator.getPort();

    ClientVM clientVM = csRule.startClientVM(2, props, cf -> cf
        .addPoolLocator("localhost", locatorPort), clientVersion);

    // Client adds data
    clientVM.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<String, String> rf =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region = rf.create(regionName);

      region.put(singleKey, singleValue);
      region.putAll(keyValueMap);
    });

    // Confirm server data has been updated.
    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getRegion(regionName))
          .containsOnlyKeys(initKey, singleKey, mapKey1, mapKey2).containsEntry(initKey, initValue)
          .containsEntry(singleKey, singleValue).containsEntry(mapKey1, mapValue1)
          .containsEntry(mapKey2, mapValue2);
    });
  }

  @Test
  public void dataWriteCannotGet() throws Exception {
    Properties props = getVMPropertiesWithPermission("dataWrite");
    int version = Integer.parseInt(clientVersion);
    if (version == 0 || version >= 140) {
      props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.security.templates.UsernamePrincipal");
    }
    int locatorPort = locator.getPort();

    ClientVM client = csRule.startClientVM(2, props, cf -> cf
        .addPoolLocator("localhost", locatorPort), clientVersion);

    // Client cannot get through any avenue
    client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<String, String> rf =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region = rf.create(regionName);

      assertThatThrownBy(() -> region.get(initKey))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.get(singleKey, null))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      // An unauthorized getAll does not throw; it just does not return the requested values.
      // See GEODE-3632.
      assertThat(region.getAll(keyValueMap.keySet())).isEmpty();
      assertThat(region.getAll(keyValueMap.keySet(), null)).isEmpty();
    });
  }

  @Test
  public void dataReadClientCanGet() throws Exception {
    Properties props = getVMPropertiesWithPermission("dataRead");
    int locatorPort = locator.getPort();

    ClientVM client = csRule.startClientVM(2, props, cf -> cf
        .addPoolLocator("localhost", locatorPort), clientVersion);

    // Add some values for the client to get
    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<String, String> region = cache.getRegion(regionName);
      region.put(singleKey, singleValue);
      region.put(mapKey1, mapValue1);
      region.put(mapKey2, mapValue2);
    });

    // Client can successfully get the data
    client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<String, String> rf =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region = rf.create(regionName);

      assertThat(region.get(initKey)).isEqualTo(initValue);
      assertThat(region.get(singleKey)).isEqualTo(singleValue);
      assertThat(region.getAll(keyValueMap.keySet())).containsAllEntriesOf(keyValueMap);
    });
  }

  @Test
  public void dataReadCannotPut() throws Exception {
    Properties props = getVMPropertiesWithPermission("dataRead");
    int version = Integer.parseInt(clientVersion);
    if (version == 0 || version >= 140) {
      props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.security.templates.UsernamePrincipal");
    }

    int locatorPort = locator.getPort();

    ClientVM clientVM = csRule.startClientVM(2, props, cf -> cf
        .addPoolLocator("localhost", locatorPort), clientVersion);

    clientVM.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<String, String> rf =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region = rf.create(regionName);

      assertThatThrownBy(() -> region.put(singleKey, singleValue))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.put(singleKey, singleValue, null))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.putAll(keyValueMap))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.putAll(keyValueMap, null))
          .hasCauseInstanceOf(NotAuthorizedException.class);
    });

    // Confirm server-side that no put went through:
    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getRegion(regionName)).containsOnlyKeys(initKey)
          .containsEntry(initKey, initValue);
    });

  }

  private Properties getVMPropertiesWithPermission(String permission) {
    Properties props = new Properties();
    // Using the legacy security framework
    props.setProperty(SECURITY_CLIENT_AUTHENTICATOR,
        SimpleAuthenticator.class.getCanonicalName() + ".create");
    props.setProperty(SECURITY_CLIENT_ACCESSOR,
        SimpleAccessController.class.getCanonicalName() + ".create");

    // Using the given username/perission string
    props.setProperty(UserPasswordAuthInit.USER_NAME, permission);
    props.setProperty(UserPasswordAuthInit.PASSWORD, permission);
    props.setProperty(SECURITY_CLIENT_AUTH_INIT,
        UserPasswordAuthInit.class.getCanonicalName() + ".create");

    // We can't sent the object filter property versions before 1.4.0 because
    // it's not a valid property, but we must set it in 140 and above to allow
    // serialization of UsernamePrincipal
    if (clientVersion.compareTo("140") >= 0) {
      props.setProperty(SERIALIZABLE_OBJECT_FILTER, UsernamePrincipal.class.getCanonicalName());
    }
    return props;
  }
}
