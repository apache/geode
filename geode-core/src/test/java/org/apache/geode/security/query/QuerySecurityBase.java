package org.apache.geode.security.query;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
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
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.security.SecurityTestUtil;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.ServerStarterRule;

public class QuerySecurityBase extends JUnit4DistributedTestCase {

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
          .withProperty(TestSecurityManager.SECURITY_JSON,
              "org/apache/geode/management/internal/security/clientServer.json")
          .withRegion(getRegionType(), UserPermissions.REGION_NAME);

  //Varibles used to store caches between invoke methods
  private static ClientCache clientCache;

  protected static transient UserPermissions userPerms;

  protected  Host host;
  protected VM superUserClient;
  protected  VM specificUserClient;


  protected UserPermissions configurePermissions() {
    return new UserPermissions("DATA:READ");
  }

  public RegionShortcut getRegionType() {
    return RegionShortcut.REPLICATE;
  }

  public List<String> getAllUsers() {
    return userPerms.getAllUsers();
  }

  public List<String> getAllUsersWhoCanExecuteQuery() {
    return Arrays.asList("dataReader", "dataReaderRegion", "clusterManagerDataReader",
        "clusterManagerDataReaderRegion", "super-user");
  }

  public List<String> getAllUsersWhoCannotExecuteQuery() {
    return Arrays.asList("stranger", "dataReaderRegionKey",
        "clusterManager", "clusterManagerQuery", "clusterManagerDataReaderRegionKey");
  }
  @Before
  public void setup() {
    host = Host.getHost(0);
    superUserClient = host.getVM(1);
    specificUserClient = host.getVM(2);
    userPerms = configurePermissions();
    createClientCache(superUserClient, "super-user", userPerms.getUserPassword("super-user"));
    createProxyRegion(superUserClient, UserPermissions.REGION_NAME);
  }

  public void setClientCache(ClientCache cache) {
    clientCache = cache;
  }

  public ClientCache getClientCache() {
    return clientCache;
  }

  public void createClientCache(VM vm, String userName, String password) {
    vm.invoke(() -> {
      ClientCache cache = SecurityTestUtil.createClientCache(userName, password,
          server.getPort());
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

  public void closeClientCache(VM vm) {
    vm.invoke(() -> {
      if (getClientCache() != null) {
        getClientCache().close();
      }
    });
  }

  protected void assertExceptionOccurred(QueryService qs, String query, String authErrorRegexp) {
    System.out.println("Execution exception should match:" + authErrorRegexp);
    try {
      qs.newQuery(query).execute();
    } catch (Exception e) {
      if (!e.getMessage().matches(authErrorRegexp)) {
        Throwable cause = e.getCause();
        while (cause != null) {
          if (cause.getMessage().matches(authErrorRegexp)) {
            return;
          }
          cause = cause.getCause();
        }
        e.printStackTrace();
        fail();
      }
    }
  }

  protected void assertExceptionOccurred(QueryService qs, String query, Object[] bindParams, String authErrorRegexp) {
    System.out.println("Execution exception should match:" + authErrorRegexp);
    try {
      qs.newQuery(query).execute(bindParams);
      fail();
    } catch (Exception e) {
      if (!e.getMessage().matches(authErrorRegexp)) {
        Throwable cause = e.getCause();
        while (cause != null) {
          if (cause.getMessage().matches(authErrorRegexp)) {
            return;
          }
          cause = cause.getCause();
        }
        e.printStackTrace();
        fail();
      }
    }
  }


  public void executeAndConfirmQueryResults(VM vm, String query,
                                            List<Object> expectedResults) throws Exception {
    vm.invoke(()-> {
      assertQueryResults(getClientCache(), query, expectedResults);
    });
  }

  private void assertQueryResults(ClientCache clientCache, String query, List<Object> expectedResults)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    Collection
        results =
        (Collection) clientCache.getQueryService().newQuery(query).execute();
    assertNotNull("Query results were null when they should have been " + expectedResults + ".",
        results);
    assertEquals(
        "The number of expected results does not match the number of results obtained from the query."
            + query, expectedResults.size(), results.size());
    assertTrue("The query returned results that were not in the expected result list.",
        expectedResults.containsAll(results));
  }

  public  void assertRegionMatches(VM vm, String regionName, List<Object> expectedRegionResults) throws Exception {
    executeAndConfirmQueryResults(vm,"select * from /" + regionName, expectedRegionResults);
  }

  public void executeQueryWithCheckForAccessPermissions(VM vm, String query, String user, String regionName, List<Object> expectedSuccessfulQueryResults) {
    List<String> possibleAuthorizationErrors = userPerms.getMissingUserAuthorizationsForQuery(user, query);
    String regexForExpectedExceptions = userPerms.regexForExpectedException(possibleAuthorizationErrors);
    executeQueryWithCheckForAccessPermissions(vm, query, user, possibleAuthorizationErrors, regexForExpectedExceptions, regionName, expectedSuccessfulQueryResults);
}


  private void executeQueryWithCheckForAccessPermissions(VM vm, String query, String user, List<String> possibleAuthorizationErrors, String regexForExpectedExceptions,
                                                        String regionName, List<Object> expectedSuccessfulQueryResults) {

    vm.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      if (possibleAuthorizationErrors.isEmpty()) {
        assertQueryResults(getClientCache(), query, expectedSuccessfulQueryResults);
      } else {
        String authErrorRegexp = regexForExpectedExceptions;
        assertExceptionOccurred(getClientCache().getQueryService(), query, authErrorRegexp);
        Pool pool = PoolManager.find(region);
        assertExceptionOccurred(pool.getQueryService(), query, authErrorRegexp);
      }
    });
  }

  protected void putIntoRegion(VM vm, Object[] keys, Object[] values,
                                          String regionName) {
    vm.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
        assertEquals(
            "Bad region put. The list of keys does not have the same length as the list of values.",
            keys.length, values.length);
        for (int i = 0; i < keys.length; i++) {
          region.put(keys[i], values[i]);
        }
    });
  }
}
