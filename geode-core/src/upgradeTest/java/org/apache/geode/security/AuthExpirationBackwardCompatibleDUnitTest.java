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

import static java.util.stream.Collectors.toList;
import static org.apache.geode.cache.query.dunit.SecurityTestUtils.createAndExecuteCQ;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.version.VmConfigurations.hasGeodeVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.dunit.SecurityTestUtils.EventsCqListner;
import org.apache.geode.cache.query.dunit.SecurityTestUtils.KeysCacheListener;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.TestVersions;
import org.apache.geode.test.version.VmConfiguration;
import org.apache.geode.test.version.VmConfigurations;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class AuthExpirationBackwardCompatibleDUnitTest {
  // only test versions greater than or equal to 1.14.0
  private static final TestVersion test_start_version = TestVersion.valueOf("1.14.0");
  private static final TestVersion feature_start_version = TestVersion.valueOf("1.15.0");
  private static RegionService user0Service;
  private static RegionService user1Service;

  @Parameterized.Parameter
  public VmConfiguration clientVmConfiguration;

  @Parameterized.Parameters(name = "Client {0}")
  public static Collection<VmConfiguration> data() {
    return VmConfigurations.all().stream()
        .filter(hasGeodeVersion(TestVersions.atLeast(test_start_version)))
        .collect(toList());
  }

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public RestoreSystemProperties restore = new RestoreSystemProperties();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withSecurityManager(ExpirableSecurityManager.class)
      .withRegion(RegionShortcut.REPLICATE, "region");


  private ClientVM clientVM;

  @After
  public void after() {
    if (clientVM != null) {
      clientVM.invoke(UpdatableUserAuthInitialize::reset);
    }
  }

  @Test
  public void clientWithNoUserRefreshWillNotSucceed() throws Exception {
    int serverPort = server.getPort();
    ClientVM clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<Object, Object> clientRegionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<Object, Object> region = clientRegionFactory.create("region");
      region.put(0, "value0");
    });

    // expire the current user
    ExpirableSecurityManager securityManager = getSecurityManager();
    securityManager.addExpiredUser("user1");

    // if client, even after getting AuthExpiredExpiration, still sends in
    // old credentials, the operation will fail (we only try re-authenticate once)
    // this test makes sure no lingering old credentials will allow the operations to succeed.
    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache.getRegion("region");
      doPutAndExpectFailure(region, 100);
    });

    Region<Object, Object> region = server.getCache().getRegion("/region");
    assertThat(region).hasSize(1);
    Map<String, List<String>> authorizedOps = securityManager.getAuthorizedOps();
    Map<String, List<String>> unAuthorizedOps = securityManager.getUnAuthorizedOps();
    assertThat(authorizedOps.keySet()).containsExactly("user1");
    assertThat(authorizedOps.get("user1")).containsExactly("DATA:WRITE:region:0");
    assertThat(unAuthorizedOps.keySet()).containsExactly("user1");
  }

  private static void doPutAndExpectFailure(Region<Object, Object> region, int times) {
    for (int i = 1; i < times; i++) {
      assertThatThrownBy(() -> region.put(1, "value1"))
          .isInstanceOf(ServerOperationException.class)
          .getCause().isInstanceOfAny(AuthenticationFailedException.class,
              AuthenticationRequiredException.class);
    }
  }

  @Test
  public void singleUserModeShouldReAuthenticateWhenCredentialExpiredAndOperationSucceed()
      throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
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
    assertThat(authorizedOps.keySet()).hasSize(2);
    assertThat(authorizedOps.get("user1")).containsExactly("DATA:WRITE:region:0");
    assertThat(authorizedOps.get("user2")).containsExactly("DATA:WRITE:region:1");
    assertThat(unAuthorizedOps.keySet()).hasSize(1);
    assertThat(unAuthorizedOps.get("user1")).containsExactly("DATA:WRITE:region:1");
  }

  @Test
  public void multiUserModeShouldReAuthenticateWhenCredentialExpiredAndOperationSucceed()
      throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
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

  private static EventsCqListner CQLISTENER0;
  private static EventsCqListner CQLISTENER1;


  @Test
  public void cqOlderClientWillNotReAuthenticateAutomatically() throws Exception {
    // this test should only test the older client
    if (clientVmConfiguration.geodeVersion().greaterThanOrEqualTo(feature_start_version)) {
      return;
    }

    startClientWithCQ();

    Region<Object, Object> region = server.getCache().getRegion("/region");
    region.put("1", "value1");
    clientVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(CQLISTENER0.getKeys())
              .containsExactly("1"));
    });

    // expire the current user
    ExpirableSecurityManager securityManager = getSecurityManager();
    securityManager.addExpiredUser("user1");

    // do a second put, the event should not be delivered to the client
    region.put("2", "value2");

    clientVM.invoke(() -> {
      // even user gets refreshed, the old client wouldn't be able to send in the new credentials
      UpdatableUserAuthInitialize.setUser("user2");
      await().during(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> assertThat(CQLISTENER0.getKeys())
                  .containsExactly("1"));

      // queue is closed, client would re-connect with the new credential, but the old queue is lost
      Region<Object, Object> clientRegion =
          ClusterStartupRule.clientCacheRule.createProxyRegion("region");
      clientRegion.put("3", "value3");

      await()
          .untilAsserted(
              () -> assertThat(CQLISTENER0.getKeys())
                  .containsExactly("1", "3"));
    });
    Map<String, List<String>> authorizedOps = securityManager.getAuthorizedOps();
    assertThat(authorizedOps.get("user1")).asList().containsExactly("DATA:READ:region",
        "DATA:READ:region:1");

    Map<String, List<String>> unAuthorizedOps = securityManager.getUnAuthorizedOps();
    assertThat(unAuthorizedOps.keySet().size()).isEqualTo(1);
    assertThat(unAuthorizedOps.get("user1")).asList().contains("DATA:READ:region:2");
  }

  @Test
  // re-authentication in Multi-user mode in event dispatching is not supported.
  // we do support re-auth in multi-user mode in non-event dispatching case.
  // in event dispatching case, if one user's credential expired,
  // the client will be terminated by the CacheClientUpdater
  public void multiUserCq() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withMultiUser(true)
            .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user0");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
      Properties userSecurityProperties = new Properties();
      userSecurityProperties.put(SECURITY_CLIENT_AUTH_INIT,
          UpdatableUserAuthInitialize.class.getName());
      user0Service = clientCache.createAuthenticatedView(userSecurityProperties);
      CQLISTENER0 = createAndExecuteCQ(user0Service.getQueryService(), "cq1",
          "select * from /region r where r.length<=2");


      UpdatableUserAuthInitialize.setUser("user1");
      user1Service = clientCache.createAuthenticatedView(userSecurityProperties);
      CQLISTENER1 = createAndExecuteCQ(user1Service.getQueryService(), "cq2",
          "select * from /region r where r.length>=2");
    });

    Region<Object, Object> region = server.getCache().getRegion("/region");
    region.put("1", "1");
    getSecurityManager().addExpiredUser("user1");
    region.put("11", "11");
    region.put("111", "111");

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1_extended");
      // user0Service listener will get one event
      await().during(10, TimeUnit.SECONDS).untilAsserted(
          () -> assertThat(CQLISTENER0.getKeys())
              .containsExactly("1"));

      // user1Service listener will not get any events
      await().during(10, TimeUnit.SECONDS).untilAsserted(
          () -> assertThat(CQLISTENER1.getKeys()).isEmpty());

      user0Service.close();
      user1Service.close();
    });

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    assertThat(unAuthorizedOps.keySet()).hasSize(1);
    assertThat(unAuthorizedOps.get("user1")).asList().contains("DATA:READ:region:11");
  }

  @Test
  public void createCQWillReAuth() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      Region<Object, Object> proxyRegion =
          ClusterStartupRule.clientCacheRule.createProxyRegion("region");
      proxyRegion.put("key1", "value1");
    });

    getSecurityManager().addExpiredUser("user1");

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user2");
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqQuery cq =
          queryService.newCq("CQ1", "select * from /region", new CqAttributesFactory().create());
      cq.execute();
    });

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    Map<String, List<String>> authorizedOps = getSecurityManager().getAuthorizedOps();
    assertThat(unAuthorizedOps.keySet()).containsExactly("user1");
    assertThat(unAuthorizedOps.get("user1")).containsExactly("DATA:READ:region");
    assertThat(authorizedOps.keySet()).containsExactly("user1", "user2");
    assertThat(authorizedOps.get("user2")).containsExactly("DATA:READ:region");
  }

  @Test
  public void stopCQ() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqQuery cq =
          queryService.newCq("CQ1", "select * from /region", new CqAttributesFactory().create());
      cq.execute();
    });

    getSecurityManager().addExpiredUser("user1");

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user2");
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqQuery cq = queryService.getCq("CQ1");
      cq.stop();
    });

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    Map<String, List<String>> authorizedOps = getSecurityManager().getAuthorizedOps();
    assertThat(unAuthorizedOps.keySet()).containsExactly("user1");
    assertThat(unAuthorizedOps.get("user1")).containsExactly("CLUSTER:MANAGE:QUERY");
    assertThat(authorizedOps.keySet()).containsExactly("user1", "user2");
    assertThat(authorizedOps.get("user2")).containsExactly("CLUSTER:MANAGE:QUERY");
  }

  @Test
  public void closeCQ() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqQuery cq =
          queryService.newCq("CQ1", "select * from /region", new CqAttributesFactory().create());
      cq.execute();
    });

    getSecurityManager().addExpiredUser("user1");

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user2");
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqQuery cq = queryService.getCq("CQ1");
      cq.close();
    });

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    Map<String, List<String>> authorizedOps = getSecurityManager().getAuthorizedOps();
    assertThat(unAuthorizedOps.keySet()).containsExactly("user1");
    assertThat(unAuthorizedOps.get("user1")).containsExactly("DATA:READ:region");
    assertThat(authorizedOps.keySet()).containsExactly("user1", "user2");
    assertThat(authorizedOps.get("user2")).containsExactly("DATA:READ:region");
  }

  @Test
  public void registeredInterestForDefaultInterestPolicy() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withCacheSetup(
                ccf -> ccf.setPoolSubscriptionEnabled(true).setPoolSubscriptionRedundancy(0))
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
      clientRegion.registerInterestForAllKeys();
    });

    Region<Object, Object> region = server.getCache().getRegion("/region");
    region.put("1", "value1");

    // refresh user before we expire user1, otherwise we might still be using expired
    // users in some client operations
    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user2");
    });

    getSecurityManager().addExpiredUser("user1");
    region.put("2", "value2");

    // for new client, a message will be sent to client to trigger re-auth
    // for old client, server close the proxy, but client have reconnect mechanism which
    // also triggers re-auth. In both cases, no message loss since old client
    // will re-register interests with default interest policy
    clientVM.invoke(() -> {
      Region<Object, Object> clientRegion =
          ClusterStartupRule.getClientCache().getRegion("region");
      await().untilAsserted(
          () -> assertThat(clientRegion.keySet()).hasSize(2));
      // but client will reconnect successfully using the 2nd user
      clientRegion.put("2", "value2");
    });

    // user1 should not be used to put key2 to the region in any cases
    assertThat(getSecurityManager().getAuthorizedOps().get("user1"))
        .doesNotContain("DATA:READ:region:2");
  }

  @Test
  public void registeredInterest_PolicyNone_non_durableClient() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
      clientRegion.registerInterestForAllKeys(InterestResultPolicy.NONE);
    });

    Region<Object, Object> region = server.getCache().getRegion("/region");
    region.put("1", "value1");
    clientVM.invoke(() -> {
      Region<Object, Object> clientRegion =
          ClusterStartupRule.getClientCache().getRegion("region");
      await().untilAsserted(
          () -> assertThat(clientRegion.keySet()).containsExactly("1"));
      UpdatableUserAuthInitialize.setUser("user2");
    });

    getSecurityManager().addExpiredUser("user1");
    region.put("2", "value2");

    // for old client, server close the proxy, client have reconnect mechanism which
    // also triggers re-auth, clients re-register interest, but with InterestResultPolicy.NONE
    // there would be message loss
    if (clientVmConfiguration.geodeVersion().lessThan(feature_start_version)) {
      clientVM.invoke(() -> {
        Region<Object, Object> clientRegion =
            ClusterStartupRule.getClientCache().getRegion("region");
        await().during(10, TimeUnit.SECONDS).untilAsserted(
            () -> assertThat(clientRegion).hasSizeLessThan(2));
        // but client will reconnect successfully using the 2nd user
        clientRegion.put("2", "value2");
      });
    } else {
      // new client would have no message loss
      clientVM.invoke(() -> {
        Region<Object, Object> clientRegion =
            ClusterStartupRule.getClientCache().getRegion("region");
        await().untilAsserted(
            () -> assertThat(clientRegion.keySet()).containsExactly("1", "2"));
      });
    }

    // user1 should not be used to put key2 to the region in any cases
    assertThat(getSecurityManager().getAuthorizedOps().get("user1"))
        .doesNotContain("DATA:READ:region:2");
  }

  private static KeysCacheListener myListener = new KeysCacheListener();

  @Test
  public void registeredInterestForInterestPolicyNone_durableClient() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withProperty(DURABLE_CLIENT_ID, "123456")
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      myListener = new KeysCacheListener();
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> clientRegion = clientCache
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .addCacheListener(myListener)
          .create("region");
      clientRegion.registerInterestForAllKeys(InterestResultPolicy.NONE, true);
      clientCache.readyForEvents();
    });

    Region<Object, Object> region = server.getCache().getRegion("/region");
    region.put("1", "value1");
    clientVM.invoke(() -> {
      Region<Object, Object> clientRegion =
          ClusterStartupRule.getClientCache().getRegion("region");
      await().untilAsserted(
          () -> assertThat(clientRegion.keySet()).containsExactly("1"));
      UpdatableUserAuthInitialize.setUser("user2");
    });

    getSecurityManager().addExpiredUser("user1");
    region.put("2", "value2");

    // client will get both keys
    clientVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(myListener.keys).containsExactly("1", "2"));
    });

    // user1 should not be used to put key2 to the region in any cases
    assertThat(getSecurityManager().getAuthorizedOps().get("user1"))
        .doesNotContain("DATA:READ:region:2");
  }

  @Test
  public void registeredInterest_FailedReAuth_non_durableClient() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
      region.registerInterestForAllKeys();
      UpdatableUserAuthInitialize.setUser("user11");
      // invalid wait time will cause re-auth to throw exception
      UpdatableUserAuthInitialize.setWaitTime(-1);
    });

    getSecurityManager().addExpiredUser("user1");
    Region<Object, Object> region = server.getCache().getRegion("/region");
    region.put("key1", "value1");

    clientVM.invoke(() -> {
      IgnoredException.addIgnoredException(AuthenticationFailedException.class);
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache().getRegion("region");
      await().during(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(clientRegion).isEmpty());
      assertThatThrownBy(() -> clientRegion.put("key100", "value100"))
          .hasCauseInstanceOf(AuthenticationFailedException.class);
    });

    // client can't re-authenticate back, no CacheClientProxy exists (old queue destroyed
    // eventually)
    await().untilAsserted(() -> assertThat(server.getServer().getAllClientSessions()).isEmpty());
  }

  @Test
  public void registeredInterest_FailedReAuth_durableClient() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withProperty(DURABLE_CLIENT_ID, "123456")
            .withProperty(DURABLE_CLIENT_TIMEOUT, "10")
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
      region.registerInterestForAllKeys(InterestResultPolicy.NONE, true);
      UpdatableUserAuthInitialize.setUser("user11");
      clientCache.readyForEvents();
      // invalid wait time will cause re-auth to throw exception
      UpdatableUserAuthInitialize.setWaitTime(-1);
    });

    getSecurityManager().addExpiredUser("user1");
    Region<Object, Object> region = server.getCache().getRegion("/region");
    IntStream.range(0, 10).forEach(i -> region.put("key" + i, "value" + i));

    clientVM.invoke(() -> {
      IgnoredException.addIgnoredException(AuthenticationFailedException.class);
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache().getRegion("region");
      await().during(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(clientRegion).isEmpty());
    });

    // since we set a timeout, queue will be destroyed
    await().untilAsserted(() -> assertThat(getDurableClientProxy("123456")).isNull());
  }

  private CacheClientProxy getDurableClientProxy(String durableId) {
    return (CacheClientProxy) server.getServer().getClientSession(durableId);
  }

  private void startClientWithCQ() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVmConfiguration,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withCacheSetup(
                ccf -> ccf.setPoolSubscriptionRedundancy(2).setPoolSubscriptionEnabled(true))
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      CQLISTENER0 = createAndExecuteCQ(ClusterStartupRule.getClientCache().getQueryService(), "CQ1",
          "select * from /region");
    });
  }
}
