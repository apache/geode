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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.security.SecurityTestUtil;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public abstract class QuerySecurityBase extends JUnit4DistributedTestCase {

  public RegionShortcut getRegionType() {
    return RegionShortcut.REPLICATE;
  }

  protected String regionName = "region";
  protected Object[] keys;
  protected Object[] values;

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withRegion(getRegionType(), regionName);

  // Varibles used to store caches between invoke methods
  private static ClientCache clientCache;

  protected transient UserPermissions userPerms = new UserPermissions();

  protected Host host;
  protected VM superUserClient;
  protected VM specificUserClient;

  @Before
  public void configureTest() {
    host = Host.getHost(0);
    superUserClient = VM.getVM(1);
    specificUserClient = VM.getVM(2);
    createClientCache(superUserClient, "super-user", userPerms.getUserPassword("super-user"));
    createProxyRegion(superUserClient, regionName);
  }

  private void closeAnyPollutedCache() {
    if (GemFireCacheImpl.getInstance() != null) {
      GemFireCacheImpl.getInstance().close();
    }
  }

  public void setClientCache(ClientCache cache) {
    clientCache = cache;
  }

  public ClientCache getClientCache() {
    return clientCache;
  }

  public void createClientCache(VM vm, String userName, String password) {
    vm.invoke(() -> {
      closeAnyPollutedCache();
      ClientCache cache = SecurityTestUtil.createClientCache(userName, password, server.getPort());
      setClientCache(cache);
    });
  }

  public void createProxyRegion(VM vm, String regionName) {
    vm.invoke(() -> {
      SecurityTestUtil.createProxyRegion(getClientCache(), regionName);
    });
  }

  @After
  public void closeClientCaches() {
    closeClientCache(superUserClient);
    closeClientCache(specificUserClient);
  }

  private void closeClientCache(VM vm) {
    vm.invoke(() -> {
      if (getClientCache() != null) {
        getClientCache().close();
      }
    });
  }

  private void stackTraceMatchesMessage(Exception exception, String authErrorRegexp) {
    if (!exception.getMessage().matches(authErrorRegexp)) {
      Throwable cause = exception.getCause();
      while (cause != null) {
        if (cause.getMessage().matches(authErrorRegexp)) {
          return;
        }
        cause = cause.getCause();
      }
      exception.printStackTrace();
      fail("Expression " + authErrorRegexp + " not found within the stack trace.");
    }
  }

  private void assertExceptionOccurred(QueryService qs, String query, String authErrorRegexp) {
    try {
      qs.newQuery(query).execute();
      fail("An exception should have been thrown.");
    } catch (Exception e) {
      e.printStackTrace();
      stackTraceMatchesMessage(e, authErrorRegexp);
    }
  }

  void assertExceptionOccurred(QueryService qs, String query, Object[] bindParams,
      String authErrorRegexp) {
    System.out.println("Execution exception should match:" + authErrorRegexp);
    try {
      qs.newQuery(query).execute(bindParams);
      fail("An exception should have been thrown.");
    } catch (Exception e) {
      stackTraceMatchesMessage(e, authErrorRegexp);
    }
  }

  private void assertQueryResults(ClientCache clientCache, String query,
      List<Object> expectedResults) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    assertQueryResults(clientCache, query, null, expectedResults);
  }

  @SuppressWarnings("unchecked")
  void assertQueryResults(ClientCache clientCache, String queryString,
      Object[] bindParameters, List<Object> expectedResults) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    Query query = clientCache.getQueryService().newQuery(queryString);
    Collection results;
    if (bindParameters == null) {
      results = (Collection) query.execute();
    } else {
      results = (Collection) query.execute(bindParameters);
    }
    assertThat(results).isNotNull();
    assertThat(results.size()).as("Query results size did not match expected for " + query)
        .isEqualTo(expectedResults.size());

    results.forEach((i) -> assertThat(expectedResults.contains(i))
        .as("Result:" + i + " was not found in the expectedResults").isTrue());
  }

  void executeAndConfirmRegionMatches(VM vm, String regionName,
      List<Object> expectedRegionResults) {
    vm.invoke(() -> assertQueryResults(getClientCache(), "select * from /" + regionName,
        expectedRegionResults));
  }

  @SuppressWarnings("unchecked")
  protected void putIntoRegion(VM vm, Object[] keys, Object[] values, String regionName) {
    vm.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      assertThat(values.length)
          .as("Bad region put. The list of keys does not have the same length as the list of values.")
          .isEqualTo(keys.length);
      for (int i = 0; i < keys.length; i++) {
        region.put(keys[i], values[i]);
      }
    });
  }

  void executeQueryWithCheckForAccessPermissions(VM vm, String query, String regionName,
      List<Object> expectedSuccessfulQueryResults) {
    vm.invoke(() -> {
      @SuppressWarnings("unused")
      Region region = getClientCache().getRegion(regionName);
      assertQueryResults(getClientCache(), query, expectedSuccessfulQueryResults);
    });
  }

  void executeQueryWithCheckForAccessPermissions(VM vm, String query, String regionName,
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
