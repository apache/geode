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
 *
 */

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.cache.TXManagerImpl;
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
public class AuthExpirationTransactionUpgradeTest {
  // only test versions greater than or equal to 1.14.0
  private static final String test_start_version = "1.14.0";
  private static final String feature_start_version = "1.15.0";

  @Parameterized.Parameter
  public String clientVersion;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    return VersionManager.getInstance().getVersionsLaterThanAndEqualTo(test_start_version);
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

  @Before
  public void init() throws Exception {
    int serverPort = server.getPort();
    clientVM = cluster.startClientVM(0, clientVersion,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withServerConnection(serverPort));
  }

  @After
  public void after() {
    if (clientVM != null) {
      clientVM.invoke(UpdatableUserAuthInitialize::reset);
    }
  }

  @Test
  public void transactionSucceedsWhenAuthenticationExpires() {
    TransactionId txId =
        clientVM.invoke(() -> firstSetOfPutOperations("transaction0", "region", 0, 3));

    getSecurityManager().addExpiredUser("transaction0");

    clientVM.invoke(() -> {
      CacheTransactionManager txManager =
          secondSetOfPutOperations("transaction1", "/region", txId, 3, 6);
      txManager.commit();
    });

    verifyServerRegion(6, "/region");

    assertThat(getSecurityManager().getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = getSecurityManager().getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2");
    assertThat(authorizedOps.get("transaction1")).containsExactly("DATA:WRITE:region:3",
        "DATA:WRITE:region:4", "DATA:WRITE:region:5");

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:3");
  }

  @Test
  public void transactionCanCommitWhenAuthExpiresAndReAuthenicationFails() {
    TransactionId txId =
        clientVM.invoke(() -> firstSetOfPutOperations("transaction0", "region", 0, 3));

    getSecurityManager().addExpiredUser("transaction0");
    String client_version = clientVersion;

    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache.getRegion("/region");
      TXManagerImpl txManager = (TXManagerImpl) clientCache.getCacheTransactionManager();
      assertThat(txManager.getTXState()).isNotNull();
      assertThat(txManager.getTXState().isInProgress()).isTrue();
      assertThat(txManager.getTransactionId()).isEqualTo(txId);
      if (TestVersion.compare(client_version, feature_start_version) < 0) {
        IntStream.range(3, 6)
            .forEach(num -> assertThatThrownBy(() -> region.put(num, "value" + num)).isInstanceOf(
                ServerOperationException.class)
                .hasCause(
                    new AuthenticationRequiredException("While performing a remote authenticate")));
      } else {
        IntStream.range(3, 6)
            .forEach(num -> assertThatThrownBy(() -> region.put(num, "value" + num)).isInstanceOf(
                ServerOperationException.class)
                .hasCause(new AuthenticationFailedException("User already expired.")));
      }
      txManager.commit();
    });

    verifyServerRegion(3, "/region");

    assertThat(getSecurityManager().getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = getSecurityManager().getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2");

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:3",
        "DATA:WRITE:region:4", "DATA:WRITE:region:5");
  }

  @Test
  public void transactionCanRollbackWhenAuthExpiresAndReAuthenticationFails() {
    TransactionId txId =
        clientVM.invoke(() -> firstSetOfPutOperations("transaction0", "region", 0, 3));

    getSecurityManager().addExpiredUser("transaction0");
    String client_version = clientVersion;

    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache.getRegion("/region");
      TXManagerImpl txManager = (TXManagerImpl) clientCache.getCacheTransactionManager();
      assertThat(txManager.getTXState()).isNotNull();
      assertThat(txManager.getTXState().isInProgress()).isTrue();
      assertThat(txManager.getTransactionId()).isEqualTo(txId);
      if (TestVersion.compare(client_version, feature_start_version) < 0) {
        IntStream.range(3, 6)
            .forEach(num -> assertThatThrownBy(() -> region.put(num, "value" + num)).isInstanceOf(
                ServerOperationException.class)
                .hasCause(
                    new AuthenticationRequiredException("While performing a remote authenticate")));
      } else {
        IntStream.range(3, 6)
            .forEach(num -> assertThatThrownBy(() -> region.put(num, "value" + num)).isInstanceOf(
                ServerOperationException.class)
                .hasCause(new AuthenticationFailedException("User already expired.")));
      }
      txManager.rollback();
    });

    verifyServerRegion(0, "/region");

    assertThat(getSecurityManager().getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = getSecurityManager().getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2");

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:3",
        "DATA:WRITE:region:4", "DATA:WRITE:region:5");
  }

  @Test
  public void transactionCanRollbackWhenAuthenticationExpires() {
    TransactionId txId =
        clientVM.invoke(() -> firstSetOfPutOperations("transaction0", "region", 0, 3));

    getSecurityManager().addExpiredUser("transaction0");

    clientVM.invoke(() -> {
      CacheTransactionManager txManager =
          secondSetOfPutOperations("transaction1", "/region", txId, 3, 6);
      txManager.rollback();
    });

    verifyServerRegion(0, "/region");

    assertThat(getSecurityManager().getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = getSecurityManager().getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2");
    assertThat(authorizedOps.get("transaction1")).containsExactly("DATA:WRITE:region:3",
        "DATA:WRITE:region:4", "DATA:WRITE:region:5");

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:3");
  }

  @Test
  public void transactionCanResumeWhenAuthenticationExpires() {
    TransactionId txId = clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("transaction0");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache.createClientRegionFactory(
          ClientRegionShortcut.PROXY).create("region");
      CacheTransactionManager txManager = clientCache.getCacheTransactionManager();
      txManager.begin();
      IntStream.range(0, 3).forEach(num -> region.put(num, "value" + num));
      return txManager.suspend();
    });

    getSecurityManager().addExpiredUser("transaction0");

    clientVM.invoke(() -> {
      UpdatableUserAuthInitialize.setUser("transaction1");
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> region = clientCache.getRegion("/region");
      TXManagerImpl txManager = (TXManagerImpl) clientCache.getCacheTransactionManager();
      txManager.resume(txId);
      assertThat(txManager.getTXState()).isNotNull();
      assertThat(txManager.getTXState().isInProgress()).isTrue();
      assertThat(txManager.getTransactionId()).isEqualTo(txId);
      IntStream.range(3, 6).forEach(num -> region.put(num, "value" + num));
      txManager.commit();
    });

    verifyServerRegion(6, "/region");

    assertThat(getSecurityManager().getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = getSecurityManager().getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2");
    assertThat(authorizedOps.get("transaction1")).containsExactly("DATA:WRITE:region:3",
        "DATA:WRITE:region:4", "DATA:WRITE:region:5");

    Map<String, List<String>> unAuthorizedOps = getSecurityManager().getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:3");
  }

  private void verifyServerRegion(int numTransactions, String regionPath) {
    Region<Object, Object> serverRegion = server.getCache().getRegion(regionPath);
    assertThat(serverRegion.keySet()).hasSize(numTransactions);
    IntStream.range(0, numTransactions)
        .forEach(num -> assertThat(serverRegion.get(num)).isEqualTo("value" + num));
  }

  private static TransactionId firstSetOfPutOperations(String user, String regionName,
      int startRange, int endRange) {
    UpdatableUserAuthInitialize.setUser(user);
    ClientCache clientCache = ClusterStartupRule.getClientCache();
    Region<Object, Object> region = clientCache.createClientRegionFactory(
        ClientRegionShortcut.PROXY).create(regionName);
    CacheTransactionManager txManager = clientCache.getCacheTransactionManager();
    txManager.begin();
    IntStream.range(startRange, endRange).forEach(num -> region.put(num, "value" + num));
    return txManager.getTransactionId();
  }

  private static CacheTransactionManager secondSetOfPutOperations(String user, String regionPath,
      TransactionId txId, int startRange, int endRange) {
    UpdatableUserAuthInitialize.setUser(user);
    ClientCache clientCache = ClusterStartupRule.getClientCache();
    Region<Object, Object> region = clientCache.getRegion(regionPath);
    TXManagerImpl txManager = (TXManagerImpl) clientCache.getCacheTransactionManager();
    assertThat(txManager.getTXState()).isNotNull();
    assertThat(txManager.getTXState().isInProgress()).isTrue();
    assertThat(txManager.getTransactionId()).isEqualTo(txId);
    IntStream.range(3, 6).forEach(num -> region.put(num, "value" + num));
    return txManager;
  }

  private ExpirableSecurityManager getSecurityManager() {
    return (ExpirableSecurityManager) server.getCache().getSecurityService().getSecurityManager();
  }
}
