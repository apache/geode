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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class AuthExpirationDUnitTest {
  private static String test_start_version = "1.14.0";
  private static RegionService user0Service;
  private static RegionService user1Service;

  @Parameterized.Parameter
  public String clientVersion;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    // only test versions greater than or equal to 1.14.0
    return VersionManager.getInstance().getVersionsLaterThanAndEqualTo(test_start_version);
  }

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public RestoreSystemProperties restore = new RestoreSystemProperties();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withSecurityManager(ExpirableSecurityManager.class)
      .withRegion(RegionShortcut.REPLICATE, "region");


  private ClientVM clientVM;

  @Test
  public void clientWithNoUserRefreshWillNotSucceed() throws Exception {
    int serverPort = server.getPort();
    ClientVM clientVM = lsRule.startClientVM(0, clientVersion,
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
    clientVM = lsRule.startClientVM(0, clientVersion,
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
    clientVM = lsRule.startClientVM(0, clientVersion,
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
  public void cqNewerClientWillReAuthenticateAutomatically() throws Exception {
    // this test should only test the newer client
    if (TestVersion.compare(clientVersion, test_start_version) <= 0) {
      return;
    }

    startClientWithCQ();

    Region<Object, Object> region = server.getCache().getRegion("/region");
    region.put("1", "value1");
    clientVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(CQLISTENER0.getKeys())
              .asList()
              .containsExactly("1"));
    });

    // expire the current user
    getSecurityManager().addExpiredUser("user1");

    // update the user to be used before we try to send the 2nd event
    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user2");
    });

    // do a second put, the event should be queued until client re-authenticate
    region.put("2", "value2");

    clientVM.invoke(() -> {
      // the client will eventually get the 2nd event
      await().untilAsserted(
          () -> assertThat(CQLISTENER0.getKeys())
              .asList()
              .containsExactly("1", "2"));
    });

    Map<String, List<String>> authorizedOps = getSecurityManager().getAuthorizedOps();
    assertThat(authorizedOps.keySet().size()).isEqualTo(2);
    assertThat(authorizedOps.get("user1")).asList().containsExactly("DATA:READ:region",
        "DATA:READ:region:1");
    assertThat(authorizedOps.get("user2")).asList().containsExactly("DATA:READ:region:2");

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    assertThat(unAuthorizedOps.keySet().size()).isEqualTo(1);
    assertThat(unAuthorizedOps.get("user1")).asList().containsExactly("DATA:READ:region:2");
  }

  @Test
  public void cqOlderClientWillNotReAuthenticate() throws Exception {
    // this test should only test the older client
    if (TestVersion.compare(clientVersion, test_start_version) > 0) {
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
      await().during(6, TimeUnit.SECONDS)
          .untilAsserted(
              () -> assertThat(CQLISTENER0.getKeys())
                  .containsExactly("1"));
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
    clientVM = lsRule.startClientVM(0, clientVersion,
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
  public void cqOlderClientWithClientInteractionWillDeliverEventEventually() throws Exception {
    // this test should only test the older client
    if (TestVersion.compare(clientVersion, test_start_version) > 0) {
      return;
    }
    startClientWithCQ();

    Region<Object, Object> region = server.getCache().getRegion("/region");
    region.put("1", "value1");
    getSecurityManager().addExpiredUser("user1");
    region.put("2", "value2");

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user2");
      Region<Object, Object> proxyRegion =
          ClusterStartupRule.clientCacheRule.createProxyRegion("region");
      proxyRegion.put("3", "value3");
      await().untilAsserted(
          () -> assertThat(CQLISTENER0.getKeys())
              .containsExactly("1", "2", "3"));

    });
  }

  private void startClientWithCQ() throws Exception {
    int serverPort = server.getPort();
    clientVM = lsRule.startClientVM(0, clientVersion,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      CQLISTENER0 = createAndExecuteCQ(ClusterStartupRule.getClientCache().getQueryService(), "CQ1",
          "select * from /region");
    });
  }

  private static EventsCqListner createAndExecuteCQ(QueryService queryService, String cqName,
      String query)
      throws CqExistsException, CqException, RegionNotFoundException {
    CqAttributesFactory cqaf = new CqAttributesFactory();
    EventsCqListner listenter = new EventsCqListner();
    cqaf.addCqListener(listenter);

    CqQuery cq = queryService.newCq(cqName, query, cqaf.create());
    cq.execute();
    return listenter;
  }

  private static class EventsCqListner implements CqListener {
    private List<String> keys = new ArrayList<>();

    @Override
    public void onEvent(CqEvent aCqEvent) {
      keys.add(aCqEvent.getKey().toString());
    }

    @Override
    public void onError(CqEvent aCqEvent) {}

    public List<String> getKeys() {
      return keys;
    }
  }
}
