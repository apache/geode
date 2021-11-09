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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.Query;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.security.templates.CountableUserPasswordAuthInit;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class MultiUserAPIDUnitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM server;

  @Rule
  public ClientCacheRule client = new ClientCacheRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void setUp() throws Exception {
    MemberVM locator =
        cluster.startLocatorVM(0, c -> c.withSecurityManager(SimpleSecurityManager.class));
    server = cluster.startServerVM(1, s -> s.withCredential("cluster", "cluster")
        .withConnectionToLocator(locator.getPort()));

    server.invoke(() -> {
      ClusterStartupRule.memberStarter.createRegion(RegionShortcut.REPLICATE, "region");
    });
  }

  @Test
  public void testSingleUserUnsupportedAPIs() throws Exception {
    client.withCredential("stranger", "stranger").withMultiUser(false)
        .withServerConnection(server.getPort());
    ClientCache clientCache = client.createCache();

    assertThatThrownBy(() -> clientCache.createAuthenticatedView(new Properties()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("did not have multiuser-authentication set to true");
  }

  @Test
  public void testMultiUserUnsupportedAPIs() throws Exception {
    client.withCredential("stranger", "stranger")
        .withPoolSubscription(true)
        .withMultiUser(true)
        .withServerConnection(server.getPort());
    client.createCache();
    Region region = client.createProxyRegion("region");
    Pool pool = client.getCache().getDefaultPool();

    RegionService regionService = client.createAuthenticatedView("data", "data");
    Region regionView = regionService.getRegion("region");

    assertThatThrownBy(() -> region.create("key", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> region.put("key", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> region.get("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> region.containsKeyOnServer("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> region.remove("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> region.destroy("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> region.destroyRegion())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> region.registerInterest("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> region.clear()).isInstanceOf(UnsupportedOperationException.class);


    assertThatThrownBy(() -> regionView.createSubregion("subRegion", null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionView.forceRolling())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionView.getAttributesMutator())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionView.loadSnapshot(null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionView.saveSnapshot(null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionView.registerInterest("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionView.setUserAttribute(null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionView.unregisterInterestRegex("*"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionView.localDestroy("key"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionView.localInvalidate("key"))
        .isInstanceOf(UnsupportedOperationException.class);

    assertThatThrownBy(() -> FunctionService.onRegion(region))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> FunctionService.onServer(pool))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> FunctionService.onServers(pool))
        .isInstanceOf(UnsupportedOperationException.class);


    assertThatThrownBy(() -> {
      Query query = pool.getQueryService().newQuery("SELECT * FROM " + SEPARATOR + "region");
      query.execute();
    }).isInstanceOf(UnsupportedOperationException.class);

    CqQuery cqQuery =
        pool.getQueryService().newCq("SELECT * FROM " + SEPARATOR + "region",
            new CqAttributesFactory().create());
    assertThatThrownBy(() -> cqQuery.execute())
        .hasCauseInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> cqQuery.executeWithInitialResults())
        .hasCauseInstanceOf(UnsupportedOperationException.class);

    assertThatThrownBy(() -> regionService.getQueryService().getIndexes())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionService.getQueryService().getIndexes(null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionService.getQueryService().createIndex(null, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> regionService.getQueryService().removeIndexes())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void singleUserSubscriptionEnabled() throws Exception {
    ClientCache clientCache = client.withPoolSubscription(true)
        .withServerConnection(server.getPort())
        .createCache();
    assertThat(clientCache).isNotNull();
    assertThatThrownBy(() -> client.createProxyRegion("region"))
        .isInstanceOf(AuthenticationRequiredException.class);
  }

  @Test
  public void singleUserSubscriptionNotEnabled() throws Exception {
    ClientCache clientCache = client.withPoolSubscription(false)
        .withServerConnection(server.getPort())
        .createCache();
    assertThat(clientCache).isNotNull();
    Region region = client.createProxyRegion("region");
    assertThat(region).isNotNull();
    assertThatThrownBy(() -> region.put("key", "value"))
        .hasCauseInstanceOf(AuthenticationRequiredException.class);
  }

  @Test
  public void noCredentialCanCreateCacheWithMultiUser() throws Exception {
    ClientCache clientCache = client.withServerConnection(server.getPort()).withMultiUser(true)
        .createCache();

    // with multiuser, client cache can be created without credential and proxy region can be
    // created. It's the same as single user with no pool subscription case
    Region region = client.createProxyRegion("region");
    assertThat(clientCache).isNotNull();
    assertThat(region).isNotNull();
    assertThatThrownBy(() -> region.put("key", "value"))
        .isInstanceOf(UnsupportedOperationException.class);

    RegionService validView = client.createAuthenticatedView("data", "data");
    Region<String, Object> validRegion = validView.getRegion("region");
    validRegion.put("key", "value");

    RegionService notAuthenticatedView = client.createAuthenticatedView("test", "invalid");
    Region<String, Object> notAuthenticatedRegion = notAuthenticatedView.getRegion("region");
    assertThatThrownBy(() -> notAuthenticatedRegion.put("key", "value"))
        .hasCauseInstanceOf(AuthenticationFailedException.class);

    RegionService notAuthorizedView = client.createAuthenticatedView("test", "test");
    Region<String, Object> notAuthorizedRegion = notAuthorizedView.getRegion("region");
    assertThatThrownBy(() -> notAuthorizedRegion.put("key", "value"))
        .hasCauseInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void jsonFormatterOnTheClientWithSingleUser() throws Exception {
    client.withProperty(SECURITY_CLIENT_AUTH_INIT, CountableUserPasswordAuthInit.class.getName())
        .withProperty(UserPasswordAuthInit.USER_NAME, "data")
        .withProperty(UserPasswordAuthInit.PASSWORD, "data")
        .withMultiUser(false)
        .withServerConnection(server.getPort()).createCache();
    Region region = client.createProxyRegion("region");

    // with single user, the static method in JSONFormatter can be used
    String json = "{\"key\" : \"value\"}";
    PdxInstance value = JSONFormatter.fromJSON(json);
    region.put("key", value);

    // make sure the client only needs to authenticate once
    assertThat(CountableUserPasswordAuthInit.count.get()).isEqualTo(1);
  }

  @Test
  public void multiUser_OneUserShouldOnlyAuthenticateOnceByDifferentThread() throws Exception {
    ClientCache cache = client.withServerConnection(server.getPort())
        .withProperty(SECURITY_CLIENT_AUTH_INIT, CountableUserPasswordAuthInit.class.getName())
        .withMultiUser(true)
        .createCache();
    Properties properties = new Properties();
    properties.setProperty(UserPasswordAuthInit.USER_NAME, "data");
    properties.setProperty(UserPasswordAuthInit.PASSWORD, "data");
    RegionService regionService = cache.createAuthenticatedView(properties);

    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
    Region region = regionService.getRegion(SEPARATOR + "region");

    Future<Object> put1 = executor.submit(() -> region.put("key", "value"));
    Future<Object> put2 = executor.submit(() -> region.put("key", "value"));

    put1.get();
    put2.get();
    assertThat(CountableUserPasswordAuthInit.count.get()).isEqualTo(1);
  }


  @After
  public void after() throws Exception {
    CountableUserPasswordAuthInit.reset();
  }

  @Test
  public void jsonFormatterOnTheClientWithMultiUser() throws Exception {
    client.withMultiUser(true).withServerConnection(server.getPort()).createCache();
    client.createProxyRegion("region");

    RegionService regionService = client.createAuthenticatedView("data", "data");
    Region<String, Object> regionView = regionService.getRegion("region");

    String json = "{\"key\" : \"value\"}";

    // in multiUser view, can not use the static methods in JSONFormatter directly
    assertThatThrownBy(() -> JSONFormatter.fromJSON(json))
        .hasCauseInstanceOf(UnsupportedOperationException.class);

    // need to get the jsonFormatter from the proxy cache
    PdxInstance value = regionService.getJsonFormatter().toPdxInstance(json);
    regionView.put("key", value);
  }

  @Test
  public void multiUserWithCQ_Should_Authentiate() throws Exception {
    ClientCache cache = client.withServerConnection(server.getPort())
        .withPoolSubscription(true)
        .withProperty(SECURITY_CLIENT_AUTH_INIT, CountableUserPasswordAuthInit.class.getName())
        .withMultiUser(true)
        .createCache();
    Properties properties = new Properties();
    properties.setProperty(UserPasswordAuthInit.USER_NAME, "data");
    properties.setProperty(UserPasswordAuthInit.PASSWORD, "wrongPassword");
    RegionService regionService = cache.createAuthenticatedView(properties);

    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
    CqQuery cqQuery =
        regionService.getQueryService()
            .newCq("select * from /region", new CqAttributesFactory().create());
    assertThatThrownBy(() -> cqQuery.execute())
        .hasCauseInstanceOf(AuthenticationFailedException.class);
  }
}
