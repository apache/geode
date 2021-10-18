/*
 *
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
 *
 */

package org.apache.geode.security;

import static org.apache.geode.cache.query.dunit.SecurityTestUtils.createAndExecuteCQ;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.dunit.SecurityTestUtils.EventsCqListner;
import org.apache.geode.cache.query.dunit.SecurityTestUtils.KeysCacheListener;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class AuthExpirationDUnitTest {
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
    getSecurityManager().close();
  }

  private static EventsCqListner CQLISTENER0;

  @Test
  public void cqClientWillReAuthenticateAutomatically() throws Exception {
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
  public void registeredInterest_slowReAuth_policyDefault() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    ClientVM client2 = cluster.startClientVM(1,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");

      // this test will succeed because when clients re-connects, it will re-register inteest
      // a new queue will be created with all the data. Old queue is destroyed.
      region.registerInterestForAllKeys();
      UpdatableUserAuthInitialize.setUser("user11");
      // wait for time longer than server's max time to wait to ree-authenticate
      UpdatableUserAuthInitialize.setWaitTime(6000);
    });

    AsyncInvocation<Void> invokePut = client2.invokeAsync(() -> {
      UpdatableUserAuthInitialize.setUser("user2");
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
      IntStream.range(0, 100).forEach(i -> region.put("key" + i, "value" + i));
    });

    getSecurityManager().addExpiredUser("user1");
    invokePut.await();

    // make sure this client recovers and get all the events and will be able to do client operation
    clientVM.invoke(() -> {
      Region<Object, Object> region = ClusterStartupRule.getClientCache().getRegion("region");
      await().untilAsserted(
          () -> assertThat(region.keySet()).hasSize(100));
      region.put("key100", "value100");
    });

    // user1 should not be used to put any keys to the region
    assertThat(getSecurityManager().getAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:region");
    assertThat(getSecurityManager().getUnAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:region:key0");
  }

  @Test
  @Ignore("unnecessary test case for re-auth, but it manifests GEODE-9704")
  public void registeredInterest_slowReAuth_policyKeys_durableClient() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withProperty(DURABLE_CLIENT_ID, "123456")
            .withPoolSubscription(true)
            .withServerConnection(serverPort));


    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");

      region.registerInterestForAllKeys(InterestResultPolicy.KEYS, true);
      clientCache.readyForEvents();
      UpdatableUserAuthInitialize.setUser("user11");
      // wait for time longer than server's max time to wait to re-authenticate
      UpdatableUserAuthInitialize.setWaitTime(6000);
    });

    getSecurityManager().addExpiredUser("user1");
    Region<Object, Object> region = server.getCache().getRegion("/region");
    IntStream.range(0, 100).forEach(i -> region.put("key" + i, "value" + i));

    // make sure this client recovers and get all the events and will be able to do client operation
    clientVM.invoke(() -> {
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache().getRegion("region");
      await().untilAsserted(() -> assertThat(clientRegion).hasSize(100));
      clientRegion.put("key100", "value100");
    });

    // user1 should not be used to put any keys to the region
    assertThat(getSecurityManager().getAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:region");
    assertThat(getSecurityManager().getUnAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:region:key0");
  }

  private static KeysCacheListener myListener = new KeysCacheListener();

  @Test
  public void registeredInterest_slowReAuth_policyNone_durableClient() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withProperty(DURABLE_CLIENT_ID, "123456")
            .withPoolSubscription(true)
            .withServerConnection(serverPort));


    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      myListener = new KeysCacheListener();
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .addCacheListener(myListener).create("region");

      // use NONE policy to make sure the old messages still sticks around
      region.registerInterestForAllKeys(InterestResultPolicy.NONE, true);
      clientCache.readyForEvents();
      UpdatableUserAuthInitialize.setUser("user11");
      // wait for time longer than server's max time to wait to re-authenticate
      UpdatableUserAuthInitialize.setWaitTime(6000);
    });

    getSecurityManager().addExpiredUser("user1");
    Region<Object, Object> region = server.getCache().getRegion("/region");
    IntStream.range(0, 100).forEach(i -> region.put("key" + i, "value" + i));

    // make sure this client recovers and get all the events and will be able to do client operation
    clientVM.invoke(() -> {
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache().getRegion("region");
      await().untilAsserted(() -> assertThat(myListener.keys).hasSize(100));
      clientRegion.put("key100", "value100");
    });

    // user1 should not be used to put any keys to the region
    assertThat(getSecurityManager().getAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:region");
    assertThat(getSecurityManager().getUnAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:region:key0");
  }


  @Test
  public void registeredInterest_slowReAuth_policyNone_nonDurableClient()
      throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));


    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");

      // use InterestResultPolicy.NONE to make sure the old queue is still around
      region.registerInterestForAllKeys(InterestResultPolicy.NONE);
      UpdatableUserAuthInitialize.setUser("user11");
      // wait for time longer than server's max time to wait to ree-authenticate
      UpdatableUserAuthInitialize.setWaitTime(6000);
    });

    getSecurityManager().addExpiredUser("user1");
    Region<Object, Object> region = server.getCache().getRegion("/region");
    IntStream.range(0, 100).forEach(i -> region.put("key" + i, "value" + i));

    // client will recover but there will be message loss
    clientVM.invoke(() -> {
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache().getRegion("region");
      await().during(10, TimeUnit.SECONDS).untilAsserted(
          () -> assertThat(clientRegion.keySet()).hasSizeLessThan(100));
      clientRegion.put("key100", "value100");
    });

    // user1 should not be used to put any keys to the region
    assertThat(getSecurityManager().getAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:region");
    assertThat(getSecurityManager().getAuthorizedOps().get("user11"))
        .contains("DATA:WRITE:region:key100");
    assertThat(getSecurityManager().getUnAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:region:key0");
  }

  private void startClientWithCQ() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0,
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

  private ExpirableSecurityManager getSecurityManager() {
    return (ExpirableSecurityManager) server.getCache().getSecurityService().getSecurityManager();
  }


}
