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
package org.apache.geode.cache.query.cq.dunit;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.security.query.QuerySecurityBase;
import org.apache.geode.security.query.QuerySecurityDUnitTest;
import org.apache.geode.security.query.UserPermissions;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(JUnitParamsRunner.class)
public class CqSecurityDUnitTest extends QuerySecurityBase {

  protected UserPermissions configurePermissions() {
    return new UserPermissions(Arrays.asList("DATA:READ"));
  }

  // Variables that need to be shared across invoke calls.
  protected static CqSecurityTestCqListener cqListener = null;

  // Should be the same as the key specified for the region key specific users in the
  // clientServer.json
  public static final String REGION_PUT_KEY = "key";

  public List<String> getAllUsersThatCannotInvokeCq() {
    return Arrays.asList("stranger", "dataReaderRegionKey", "clusterManagerQuery", "clusterManagerDataReaderRegionKey");
  }

  public List<String> getAllUsersThatCanInvokeCq() {
    return Arrays.asList(
        "dataReader", "dataReaderRegion", "clusterManagerDataReader", "clusterManagerDataReaderRegion", "super-user");
  }


  public List<String> getAllUsersOnlyAllowedWrite() {
    return Arrays.asList(
        "dataWriter");
  }


  @Test
  @Parameters(method = "getAllUsersThatCanInvokeCq")
  public void cqExecuteNoMethodInvocationWithUsersWithCqPermissionsWithPrepopulatedRegionShouldBeAllowed(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    Object[] keys = {REGION_PUT_KEY};
    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary")};
    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_PUBLIC_FIELD;
    List<String> expectedExceptionMessages =
        userPerms.getMissingUserAuthorizationsForQuery(user, query);

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      cq.execute();
    });

    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);

    specificUserClient.invoke(() -> {
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
          .until(() -> assertEquals(1, cqListener.getNumEvent()));
    });
  }

  @Test
  @Parameters(method = "getAllUsersThatCannotInvokeCq")
  public void cqExecuteNoMethodInvocationWithUsersWithoutCqPermissionsWithPrepopulatedRegionShouldThrowSecurityException(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    Object[] keys = {REGION_PUT_KEY};
    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary")};
    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_PUBLIC_FIELD;
    List<String> expectedExceptionMessages =
        userPerms.getMissingUserAuthorizationsForQuery(user, query);

    String regexForExpectedExceptions = userPerms.regexForExpectedException(expectedExceptionMessages);

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      executeCqButExpectException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void cqExecuteWithMethodInvocationWithUsersWithCqPermissionsWithPrepopulatedRegionIsGettingExceptionInReplicatedRegion(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    Object[] keys = {REGION_PUT_KEY};
    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary")};
    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;
    List<String> expectedExceptionMessages =
        userPerms.getMissingUserAuthorizationsForQuery(user, query);
    String regexForExpectedExceptions = userPerms.regexForExpectedException(expectedExceptionMessages);

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      executeCqButExpectException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void cqExecuteWithInitialResultsWithMethodInvocationWithUsersWithCqPermissionsWithPrepopulatedRegionShouldBeDeniedBecauseOfInvocation(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    Object[] keys = {REGION_PUT_KEY};
    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary")};
    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;
    List<String> expectedExceptionMessages =
        userPerms.getMissingUserAuthorizationsForQuery(user, query);
    String regexForExpectedExceptions = userPerms.regexForExpectedException(expectedExceptionMessages);

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      executeCqWithInitialResultsButExpectException(cq, user,regexForExpectedExceptions);
    });
  }


  @Test
  @Parameters(method = "getAllUsersThatCanInvokeCq")
  public void cqExecuteWithInitialResultsWithMethodInvocationWithUnpopulatedRegionAndFollowedByAPutShouldTriggerCqError(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    // execute cq
    // put into region
    // security should trigger listener with error
    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;
    List<String> expectedExceptionMessages =
        userPerms.getMissingUserAuthorizationsForQuery(user, query);

    specificUserClient.invoke(() -> {
      String regionName = UserPermissions.REGION_NAME;
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      cq.executeWithInitialResults();
    });

    Object[] keys = {REGION_PUT_KEY};
    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary")};
    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);

    specificUserClient.invoke(() -> {
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
          .until(() -> assertEquals(1, cqListener.getNumErrors()));
    });
  }

  @Test
  @Parameters(method = "getAllUsersThatCannotInvokeCq")
  public void cqExecuteWithInitialResultsWithMethodInvocationWithoutPermissionWithUnpopulatedRegionThrowSecurityException(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    // execute cq
    // put into region
    // security should trigger listener with error
    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;
    List<String> expectedExceptionMessages =
        userPerms.getMissingUserAuthorizationsForQuery(user, query);

    String regexForExpectedExceptions = userPerms.regexForExpectedException(expectedExceptionMessages);
    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      executeCqWithInitialResultsButExpectException(cq, user,regexForExpectedExceptions);
    });
  }

  @Test
  @Parameters(method = "getAllUsersThatCanInvokeCq")
  public void cqExecuteWithMethodInvocationWithUnpopulatedRegionAndFollowedByAPutShouldTriggerCqError(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    // execute cq
    // put into region
    // security should trigger listener wit gh error
    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;
    List<String> expectedExceptionMessages =
        userPerms.getMissingUserAuthorizationsForQuery(user, query);

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      cq.execute();
    });

    Object[] keys = {REGION_PUT_KEY};
    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary")};
    putIntoRegion(superUserClient, keys, values, UserPermissions.REGION_NAME);

    specificUserClient.invoke(() -> {
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
          .until(() -> assertEquals(1, cqListener.getNumErrors()));
    });
  }

  @Test
  @Parameters(method = "getAllUsersThatCannotInvokeCq")
  public void cqExecuteWithOutPermissionsWithUnpopulatedRegionShouldNotAllowCq(String user)
      throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    // execute cq
    // put into region
    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;
    List<String> expectedExceptionMessages =
        userPerms.getMissingUserAuthorizationsForQuery(user, query);

    String regexForExpectedExceptions = userPerms.regexForExpectedException(expectedExceptionMessages);

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      executeCqButExpectException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  @Parameters(method = "getAllUsersOnlyAllowedWrite")
  public void cqCreatedByAllowedUserButPutDoneByUnallowedReaderShouldStillExecuteWithCqEvent(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_PUBLIC_FIELD;

    superUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      cq.execute();
    });

    Object[] keys = {REGION_PUT_KEY};
    Object[] values = {new QuerySecurityDUnitTest.QueryTestObject(1, "Mary")};
    putIntoRegion(specificUserClient, keys, values, UserPermissions.REGION_NAME);

    superUserClient.invoke(() -> {
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
          .until(() -> assertEquals(1, cqListener.getNumEvent()));
    });
  }

  @Test
  @Parameters(method = "getAllUsersThatCanInvokeCq")
  public void cqCanBeClosedByTheCreator(
      String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_PUBLIC_FIELD;

    specificUserClient.invoke(() -> {
      QueryService queryService = getClientCache().getQueryService();
      CqSecurityTestCqListener cqListener = new CqSecurityTestCqListener();
      CqSecurityDUnitTest.cqListener = cqListener;
      CqQuery cq = createCq(queryService, query, cqListener);
      cq.execute();
      cq.close();
      assertTrue(cq.isClosed());
    });
    assertEquals(0,  server.getCache().getCqService().getAllCqs().size());
  }


  protected CqQuery createCq(QueryService queryService, String query, CqListener cqListener)
      throws CqException {
    CqAttributesFactory cqaf = new CqAttributesFactory();
    cqaf.addCqListener(cqListener);
    CqQuery cq = queryService.newCq(query, cqaf.create());
    return cq;
  }

  protected void executeCqButExpectException(CqQuery cq, String user,
      String regexForExpectedException) {
    try {
      cq.execute();
      fail("Expected an exception when executing cq:" + cq.getQueryString() + " with user:" + user);
    } catch (RegionNotFoundException | CqException e) {
      if (!e.getMessage().matches(regexForExpectedException)) {
        Throwable cause = e.getCause();
        while (cause != null) {
          if (cause.getMessage().matches(regexForExpectedException)) {
            return;
          }
          cause = cause.getCause();
        }
        e.printStackTrace();
        fail("Exception thrown did not match:" + regexForExpectedException + ".  Instead was:" + e);
      }
    }
  }

  private void executeCqWithInitialResultsButExpectException(CqQuery cq, String user,
      String regexForExpectedException) {
    try {
      cq.executeWithInitialResults();
      fail("Expected an exception when executing cq:" + cq + " with user:" + user);
    } catch (RegionNotFoundException | CqException e) {
      e.printStackTrace();
      if (!e.getMessage().matches(regexForExpectedException)) {
        Throwable cause = e.getCause();
        while (cause != null) {
          if (cause.getMessage() != null && cause.getMessage().matches(regexForExpectedException)) {
            return;
          }
          cause = cause.getCause();
        }
        e.printStackTrace();
        fail("Exception thrown did not match:" + regexForExpectedException + ".  Instead was:" + e);
      }
    }

  }

  public class CqSecurityTestCqListener implements CqListener {

    private int numEvents = 0;
    private int numErrors = 0;

    @Override
    public void onEvent(CqEvent aCqEvent) {
      numEvents++;
    }

    @Override
    public void onError(CqEvent aCqEvent) {
      numErrors++;
    }

    public int getNumEvent() {
      return numEvents;
    }

    public int getNumErrors() {
      return numErrors;
    }

    @Override
    public void close() {

    }
  }
}
