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
package org.apache.geode.security.query;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Rule;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public abstract class AbstractQuerySecurityDistributedTest implements Serializable {
  protected Object[] keys;
  protected Object[] values;
  protected MemberVM server;
  protected ClientVM superUserClient, specificUserClient;
  protected final String regionName = "region";
  private final transient UserPermissions userPerms = new UserPermissions();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  protected void setUpSuperUserClientAndServer(RegionShortcut regionShortcut) throws Exception {
    server = cluster.startServerVM(1, cf -> cf
        .withSecurityManager(TestSecurityManager.class)
        .withProperty(TestSecurityManager.SECURITY_JSON,
            "org/apache/geode/management/internal/security/clientServer.json")
        .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.security.query.data.*")
        .withRegion(regionShortcut, regionName));

    String superUserPassword = userPerms.getUserPassword("super-user");
    superUserClient =
        cluster.startClientVM(2, ccf -> ccf.withCredential("super-user", superUserPassword)
            .withPoolSubscription(true)
            .withServerConnection(server.getPort())
            .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.security.query.data.*"));
    superUserClient.invoke(() -> {
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(regionName);
    });
  }

  protected void setUpSpecificClient(String specificUser) throws Exception {
    String specificUserPassword = userPerms.getUserPassword(specificUser);
    specificUserClient =
        cluster.startClientVM(3, ccf -> ccf.withCredential(specificUser, specificUserPassword)
            .withPoolSubscription(true)
            .withServerConnection(server.getPort())
            .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.security.query.data.*"));
    specificUserClient.invoke(() -> {
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(regionName);
    });
  }

  public void setUp(String specificUser, RegionShortcut regionShortcut) throws Exception {
    setUpSuperUserClientAndServer(regionShortcut);
    setUpSpecificClient(specificUser);
  }

  public ClientCache getClientCache() {
    return ClusterStartupRule.clientCacheRule.getCache();
  }

  protected void putIntoRegion(ClientVM vm, Object[] keys, Object[] values, String regionName) {
    vm.invoke(() -> {
      Region<Object, Object> region = getClientCache().getRegion(regionName);
      assertThat(values.length)
          .as("The list of keys does not have the same length as the list of values.")
          .isEqualTo(keys.length);

      IntStream.range(0, keys.length).forEach(i -> region.put(keys[i], values[i]));
    });
  }

  private void assertExceptionOccurred(QueryService qs, String query, String authErrorRegexp) {
    assertThatThrownBy(() -> qs.newQuery(query).execute())
        .hasMessageMatching(authErrorRegexp)
        .hasCauseInstanceOf(NotAuthorizedException.class);
  }

  @SuppressWarnings("unchecked")
  protected void assertQueryResults(ClientCache clientCache, String queryString,
      Object[] bindParameters, List<Object> expectedResults) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    Collection results;
    Query query = clientCache.getQueryService().newQuery(queryString);

    if (bindParameters == null) {
      results = (Collection) query.execute();
    } else {
      results = (Collection) query.execute(bindParameters);
    }

    assertThat(results).isNotNull();
    assertThat(results.size())
        .as("Query results size did not match expected for " + query)
        .isEqualTo(expectedResults.size());

    results.forEach((i) -> assertThat(expectedResults.contains(i))
        .as("Result:" + i + " was not found in the expectedResults")
        .isTrue());
  }

  private void assertQueryResults(ClientCache clientCache, String query,
      List<Object> expectedResults) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    assertQueryResults(clientCache, query, null, expectedResults);
  }

  protected void assertRegionData(ClientVM vm, List<Object> expectedRegionResults) {
    vm.invoke(() -> assertQueryResults(getClientCache(), "SELECT * FROM " + SEPARATOR + regionName,
        expectedRegionResults));
  }

  protected void executeQueryAndAssertExpectedResults(ClientVM vm, String query,
      List<Object> expectedSuccessfulQueryResults) {
    vm.invoke(() -> assertQueryResults(getClientCache(), query, expectedSuccessfulQueryResults));
  }

  protected void executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(ClientVM vm, String query,
      String regexForExpectedExceptions) {
    vm.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      assertExceptionOccurred(getClientCache().getQueryService(), query,
          regexForExpectedExceptions);

      Pool pool = PoolManager.find(region);
      assertExceptionOccurred(pool.getQueryService(), query, regexForExpectedExceptions);
    });
  }
}
