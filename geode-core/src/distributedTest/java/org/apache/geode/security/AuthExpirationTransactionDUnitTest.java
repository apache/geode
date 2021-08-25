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

import static org.apache.geode.cache.query.dunit.SecurityTestUtils.collectSecurityManagers;
import static org.apache.geode.cache.query.dunit.SecurityTestUtils.getSecurityManager;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({SecurityTest.class})
public class AuthExpirationTransactionDUnitTest {
  private MemberVM locator;
  private MemberVM server0;
  private MemberVM server1;
  private MemberVM server2;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Before
  public void setup() {
    locator = clusterStartupRule.startLocatorVM(0,
        l -> l.withSecurityManager(ExpirableSecurityManager.class));
    int locatorPort = locator.getPort();

    server0 = clusterStartupRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withSecurityManager(ExpirableSecurityManager.class).withCredential("test", "test"));
    server1 = clusterStartupRule.startServerVM(2, s -> s.withConnectionToLocator(locatorPort)
        .withSecurityManager(ExpirableSecurityManager.class).withCredential("test", "test"));
    server2 = clusterStartupRule.startServerVM(3, s -> s.withConnectionToLocator(locatorPort)
        .withSecurityManager(ExpirableSecurityManager.class).withCredential("test", "test"));

    VMProvider.invokeInEveryMember(() -> ClusterStartupRule.getCache()
        .createRegionFactory(RegionShortcut.REPLICATE).create("region"), server0, server1, server2);

    clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withLocatorConnection(locatorPort);
  }

  @Test
  public void transactionSucceedsWhenAuthenticationExpires() throws Exception {
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("transaction0");

    Region<Object, Object> region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
    CacheTransactionManager txManager = clientCache.getCacheTransactionManager();

    txManager.begin();
    IntStream.range(0, 3).forEach(num -> region.put(num, "value" + num));

    UpdatableUserAuthInitialize.setUser("transaction1");
    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("transaction0"),
        locator, server0, server1, server2);

    IntStream.range(3, 7).forEach(num -> region.put(num, "value" + num));
    txManager.commit();

    VMProvider.invokeInEveryMember(
        () -> checkServerState("/region", 7),
        server0, server1, server2);

    ExpirableSecurityManager consolidated = collectSecurityManagers(server0, server1, server2);
    assertThat(consolidated.getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2");
    assertThat(authorizedOps.get("transaction1")).containsExactly("DATA:WRITE:region:3",
        "DATA:WRITE:region:4", "DATA:WRITE:region:5", "DATA:WRITE:region:6");

    Map<String, List<String>> unAuthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:3");
  }

  @Test
  public void transactionSucceedsWhenAuthenticationExpiresWithPartitionRegion() throws Exception {
    VMProvider.invokeInEveryMember(() -> ClusterStartupRule.getCache()
        .createRegionFactory(RegionShortcut.PARTITION).create("partitionRegion"), server0, server1,
        server2);

    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("transaction0");

    Region<Object, Object> partitionRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("partitionRegion");
    CacheTransactionManager txManager = clientCache.getCacheTransactionManager();

    txManager.begin();
    List<Integer> partitionKeys1 = IntStream.range(0, 10)
        .filter(num -> tryPartitionPut(partitionRegion, num))
        .boxed().collect(Collectors.toList());

    UpdatableUserAuthInitialize.setUser("transaction1");
    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("transaction0"),
        locator, server0, server1, server2);

    List<Integer> partitionKeys2 = IntStream.range(10, 20)
        .filter(num -> tryPartitionPut(partitionRegion, num))
        .boxed().collect(Collectors.toList());
    txManager.commit();

    partitionKeys1.addAll(partitionKeys2);
    VMProvider.invokeInEveryMember(
        () -> checkPartitionServerState("/partitionRegion", partitionKeys1),
        server0, server1, server2);

    ExpirableSecurityManager consolidated = collectSecurityManagers(server0, server1, server2);
    assertThat(consolidated.getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:partitionRegion:0",
        "DATA:WRITE:partitionRegion:1", "DATA:WRITE:partitionRegion:2",
        "DATA:WRITE:partitionRegion:3", "DATA:WRITE:partitionRegion:4",
        "DATA:WRITE:partitionRegion:5", "DATA:WRITE:partitionRegion:6",
        "DATA:WRITE:partitionRegion:7", "DATA:WRITE:partitionRegion:8",
        "DATA:WRITE:partitionRegion:9");
    assertThat(authorizedOps.get("transaction1")).containsExactly("DATA:WRITE:partitionRegion:10",
        "DATA:WRITE:partitionRegion:11", "DATA:WRITE:partitionRegion:12",
        "DATA:WRITE:partitionRegion:13", "DATA:WRITE:partitionRegion:14",
        "DATA:WRITE:partitionRegion:15", "DATA:WRITE:partitionRegion:16",
        "DATA:WRITE:partitionRegion:17", "DATA:WRITE:partitionRegion:18",
        "DATA:WRITE:partitionRegion:19");

    Map<String, List<String>> unAuthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0"))
        .containsExactly("DATA:WRITE:partitionRegion:10");
  }

  @Test
  public void transactionCanRollbackWhenAuthExpiresAndReAuthenticationFails() throws Exception {
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("transaction0");

    Region<Object, Object> region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
    CacheTransactionManager txManager = clientCache.getCacheTransactionManager();

    txManager.begin();
    IntStream.range(0, 3).forEach(num -> region.put(num, "value" + num));

    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("transaction0"),
        locator, server0, server1, server2);

    IntStream.range(3, 6)
        .forEach(num -> assertThatThrownBy(() -> region.put(num, "value" + num))
            .isInstanceOf(ServerOperationException.class)
            .hasCause(new AuthenticationFailedException("User already expired.")));
    txManager.rollback();

    VMProvider.invokeInEveryMember(
        () -> checkServerState("/region", 0),
        server0, server1, server2);

    ExpirableSecurityManager consolidated = collectSecurityManagers(server0, server1, server2);
    assertThat(consolidated.getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2");

    Map<String, List<String>> unAuthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:3",
        "DATA:WRITE:region:4", "DATA:WRITE:region:5");
  }

  @Test
  public void transactionCanCommitWhenAuthExpiresAndReAuthenticationFails() throws Exception {
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("transaction0");

    Region<Object, Object> region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
    CacheTransactionManager txManager = clientCache.getCacheTransactionManager();

    txManager.begin();
    IntStream.range(0, 3).forEach(num -> region.put(num, "value" + num));

    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("transaction0"),
        locator, server0, server1, server2);

    IntStream.range(3, 6)
        .forEach(num -> assertThatThrownBy(() -> region.put(num, "value" + num))
            .isInstanceOf(ServerOperationException.class)
            .hasCause(new AuthenticationFailedException("User already expired.")));
    txManager.commit();

    VMProvider.invokeInEveryMember(
        () -> checkServerState("/region", 3),
        server0, server1, server2);

    ExpirableSecurityManager consolidated = collectSecurityManagers(server0, server1, server2);
    assertThat(consolidated.getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2");

    Map<String, List<String>> unAuthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:3",
        "DATA:WRITE:region:4", "DATA:WRITE:region:5");
  }

  @Test
  public void transactionCanRollbackWhenAuthenticationExpires() throws Exception {
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("transaction0");

    Region<Object, Object> region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
    CacheTransactionManager txManager = clientCache.getCacheTransactionManager();

    txManager.begin();
    IntStream.range(0, 4).forEach(num -> region.put(num, "value" + num));

    UpdatableUserAuthInitialize.setUser("transaction1");
    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("transaction0"),
        locator, server0, server1, server2);

    IntStream.range(4, 6).forEach(num -> region.put(num, "value" + num));
    txManager.rollback();

    VMProvider.invokeInEveryMember(
        () -> checkServerState("/region", 0), server0, server1, server2);

    ExpirableSecurityManager consolidated = collectSecurityManagers(server0, server1, server2);
    assertThat(consolidated.getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2", "DATA:WRITE:region:3");
    assertThat(authorizedOps.get("transaction1")).containsExactly("DATA:WRITE:region:4",
        "DATA:WRITE:region:5");

    Map<String, List<String>> unAuthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:4");
  }

  @Test
  public void transactionCanResumeWhenAuthenticationExpires() throws Exception {
    ClientCache clientCache = clientCacheRule.createCache();
    UpdatableUserAuthInitialize.setUser("transaction0");

    Region<Object, Object> region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");
    CacheTransactionManager txManager = clientCache.getCacheTransactionManager();

    txManager.begin();
    IntStream.range(0, 3).forEach(num -> region.put(num, "value" + num));

    TransactionId transId = txManager.suspend();

    UpdatableUserAuthInitialize.setUser("transaction1");
    VMProvider.invokeInEveryMember(() -> getSecurityManager().addExpiredUser("transaction0"),
        locator, server0, server1, server2);

    txManager.resume(transId);

    IntStream.range(3, 6).forEach(num -> region.put(num, "value" + num));
    txManager.commit();

    VMProvider.invokeInEveryMember(
        () -> checkServerState("/region", 6), server0, server1, server2);

    ExpirableSecurityManager consolidated = collectSecurityManagers(server0, server1, server2);
    assertThat(consolidated.getExpiredUsers()).containsExactly("transaction0");

    Map<String, List<String>> authorizedOps = consolidated.getAuthorizedOps();
    assertThat(authorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:0",
        "DATA:WRITE:region:1", "DATA:WRITE:region:2");
    assertThat(authorizedOps.get("transaction1")).containsExactly("DATA:WRITE:region:3",
        "DATA:WRITE:region:4", "DATA:WRITE:region:5");

    Map<String, List<String>> unAuthorizedOps = consolidated.getUnAuthorizedOps();
    assertThat(unAuthorizedOps.get("transaction0")).containsExactly("DATA:WRITE:region:3");
  }

  private static boolean tryPartitionPut(Region<Object, Object> region, int num) {
    try {
      region.put(num, "value" + num);
    } catch (TransactionDataNotColocatedException transactionDataNotColocatedException) {
      return false;
    }
    return true;
  }

  private static void checkServerState(String regionPath, int numTransactions) {
    InternalCache cache = ClusterStartupRule.getCache();
    Region<Object, Object> region = cache.getRegion(regionPath);
    assertThat(region.keySet()).hasSize(numTransactions);
    IntStream.range(0, numTransactions).forEach(
        transaction -> assertThat(region.get(transaction)).isEqualTo("value" + transaction));
  }

  private static void checkPartitionServerState(String regionPath, List<Integer> keys) {
    InternalCache cache = ClusterStartupRule.getCache();
    Region<Object, Object> region = cache.getRegion(regionPath);
    assertThat(region.keySet()).hasSize(keys.size());
    keys.forEach(
        transaction -> assertThat(region.get(transaction)).isEqualTo("value" + transaction));
  }
}
