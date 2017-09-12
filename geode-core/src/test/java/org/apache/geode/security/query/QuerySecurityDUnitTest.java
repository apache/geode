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

import static org.junit.Assert.fail;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.index.DummyQRegion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(JUnitParamsRunner.class)
public class QuerySecurityDUnitTest extends QuerySecurityBase {


  public static final String REGION_PUT_KEY = "key";

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectAllQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_ALL_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY};
    Object[] values = {new QueryTestObject(1, "Mary")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  /* ----- Public Field Tests ----- */
  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByPublicFieldQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_PUBLIC_FIELD;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values[0]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectPublicFieldQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_PUBLIC_FIELD_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(1, 2);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectCountOfPublicFieldQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_COUNT_OF_PUBLIC_FIELD_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(2);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectMaxOfPublicFieldQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_MAX_OF_PUBLIC_FIELD_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(3);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectMinOfPublicFieldQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_MIN_OF_PUBLIC_FIELD_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(0);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectPublicFieldFromRegionByPublicFieldFromRegionQuery(
      String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_WHERE_PUBLIC_FIELD_IN_REGION_PUBLIC_FIELDS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }
  /* ----- Public Field Tests ----- */

  /* ----- Implicit Getter Tests ----- */
  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByImplicitGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_IMPLICIT_GETTER;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectImplicitGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_IMPLICIT_GETTER_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("John", "Beth");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectCountOfImplicitGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_COUNT_OF_IMPLICIT_GETTER_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(2);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectMaxOfImplicitGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_MAX_OF_IMPLICIT_GETTER_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("John");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectMinOfImplicitGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_MIN_OF_IMPLICIT_GETTER_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("Beth");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectImplicitGetterFromRegionByImplicitGetterFromRegionQuery(
      String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query =
        UserPermissions.SELECT_FROM_REGION1_WHERE_IMPLICIT_GETTER_IN_REGION2_IMPLICIT_GETTERS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }
  /* ----- Implicit Getter Tests ----- */

  /* ----- Direct Getter Tests ----- */
  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByDirectGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_DIRECT_GETTER;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectDirectGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_DIRECT_GETTER_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("John", "Beth");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectCountOfDirectGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_COUNT_OF_DIRECT_GETTER_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(2);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectMaxOfDirectGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_MAX_OF_DIRECT_GETTER_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("John");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectMinOfDirectGetterQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_MIN_OF_DIRECT_GETTER_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("Beth");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectDirectGetterFromRegionByDirectGetterFromRegionQuery(
      String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_WHERE_DIRECT_GETTER_IN_REGION_DIRECT_GETTERS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }
  /* ----- Direct Getter Tests ----- */

  /* ----- Deployed Method Tests ----- */
  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByDeployedMethodQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_DEPLOYED_METHOD;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values[0]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectDeployedMethodQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_DEPLOYED_METHOD_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("John:1", "Beth:2");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectCountOfDeployedMethodQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_COUNT_OF_DEPLOYED_METHOD_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(2);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectMaxOfDeployedMethodQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_MAX_OF_DEPLOYED_METHOD_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("John:1");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectMinOfDeployedMethodQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_MIN_OF_DEPLOYED_METHOD_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("Beth:3");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectDeployedMethodFromRegionByDeployedMethodFromRegionQuery(
      String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query =
        UserPermissions.SELECT_FROM_REGION_WHERE_DEPLOYED_METHOD_IN_REGION_DEPLOYED_METHODS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }
  /* ----- Deployed Method Tests ----- */

  /* ----- Geode Method Tests ----- */
  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectRegionContainsKeyQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_CONTAINS_KEY;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(true);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectRegionContainsValueQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_CONTAINS_VALUE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(false);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCannotExecuteQuery")
  public void usersWhoCannotExecuteQueryShouldGetExceptionForSelectRegionCreateQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_CREATE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldNotInvokeRegionCreateForSelectRegionCreateQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_CREATE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    List<Object> expectedFullRegionResults = Arrays.asList(values[0], values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
    assertRegionMatches(specificUserClient, regionName, expectedFullRegionResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCannotExecuteQuery")
  public void usersWhoCanExecuteQueryShouldGetExceptionForSelectCreateFromRegionQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_CREATE_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(QueryService.UNDEFINED, QueryService.UNDEFINED);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldGetResultsForSelectCreateFromRegionQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_CREATE_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(QueryService.UNDEFINED, QueryService.UNDEFINED);
    List<Object> expectedFullRegionResults = Arrays.asList(values[0], values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
    assertRegionMatches(specificUserClient, regionName, expectedFullRegionResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCannotExecuteQuery")
  public void usersWhoCannotExecuteQueryShouldGetExceptionForSelectRegionDestroyQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_DESTROY;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldNotInvokeDestroyForSelectRegionDestroyQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_DESTROY;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    List<Object> expectedFullRegionResults = Arrays.asList(values[0], values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
    assertRegionMatches(specificUserClient, regionName, expectedFullRegionResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCannotExecuteQuery")
  public void usersWhoCanExecuteQueryShouldGetExceptionForSelectRegionDestroyRegionQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_DESTROY_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    try {
      executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
    } catch (Throwable throwable) {
      if (!(throwable.getCause().getCause() instanceof RegionDestroyedException)) {
        throw throwable;
      }
    }
  }

  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldNotMethodInvokeADestroyForSelectRegionDestroyRegionQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_DESTROY_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    List<Object> expectedFullRegionResults = Arrays.asList(values[0], values[1]);
    try {
      executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
      assertRegionMatches(specificUserClient, regionName, expectedFullRegionResults);
    } catch (Throwable throwable) {
      if (!(throwable.getCause().getCause() instanceof RegionDestroyedException)) {
        throw throwable;
      }
    }
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectRegionDestroyRegionParenQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_DESTROY_REGION_PAREN;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    try {
      executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
    } catch (Throwable throwable) {
      if (!(throwable.getCause().getCause() instanceof RegionDestroyedException)) {
        throw throwable;
      }
    }
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectRegionGetQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_GET;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values[0]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCannotExecuteQuery")
  public void usersWhoCannotExecuteQueryShouldGetExceptionForSelectRegionPutQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_PUT;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldNotInvokeRegionPutForSelectRegionPutQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_PUT;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList();
    List<Object> expectedFullRegionResults = Arrays.asList(values[0], values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
    assertRegionMatches(specificUserClient, regionName, expectedFullRegionResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCannotExecuteQuery")
  public void usersWhoCanExecuteQueryShouldReceiveExceptionForSelectRegionPutIfAbsentQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_PUT_IF_ABSENT;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldNotInvokePutIfAbsentForSelectRegionPutIfAbsentQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_PUT_IF_ABSENT;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList();
    List<Object> expectedFullRegionResults = Arrays.asList(values[0], values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
    assertRegionMatches(specificUserClient, regionName, expectedFullRegionResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCannotExecuteQuery")
  public void checkUserAuthorizationsForSelectRegionRemoveQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_REMOVE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }


  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldNotInvokedRegionRemoveForSelectRegionRemoveQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_REMOVE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList();
    List<Object> expectedFullRegionResults = Arrays.asList(values[0], values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
    assertRegionMatches(specificUserClient, regionName, expectedFullRegionResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCannExecuteQueryShouldReceiveExpectedResultsForSelectRegionReplaceQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_REPLACE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList();
    List<Object> expectedFullRegionResults = Arrays.asList(values[0], values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
    assertRegionMatches(specificUserClient, regionName, expectedFullRegionResults);
  }

  @Test
  @Parameters(method = "getAllUsersWhoCannotExecuteQuery")
  public void usersWhoCannotExecuteQueryShouldReceiveExceptionForSelectRegionReplaceQuery(String user) throws Exception {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_REPLACE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectGetInterestListRegexRegionQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_GET_INTEREST_LIST_REGEX_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(QueryService.UNDEFINED, QueryService.UNDEFINED);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectGetInterestListRegexParenRegionQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_GET_INTEREST_LIST_REGEX_PAREN_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(1, "John"), new QueryTestObject(2, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(QueryService.UNDEFINED, QueryService.UNDEFINED);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }
  /* ----- Geode Method Tests ----- */


  /* ----- Whitelisted Methods Tests ----- */
  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectValuesQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_VALUES;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectKeySetQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_KEYSET;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(keys);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectEntriesQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_ENTRIES;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(keys);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectEntrySetQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_ENTRY_SET;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(REGION_PUT_KEY, REGION_PUT_KEY + "1");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectKeyFromEntrySetQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_KEY_FROM_REGION_ENTRYSET;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(keys);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectGetKeyFromEntrySetQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_GET_KEY_FROM_REGION_ENTRYSET;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(keys);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectValueFromEntrySetQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_VALUE_FROM_REGION_ENTRYSET;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectGetValueFromEntrySetQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_GET_VALUE_FROM_REGION_ENTRYSET;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByToStringQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_TO_STRING;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectToStringQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_TO_STRING_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList("Test_Object", "Test_Object");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByToStringToUpperCaseQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_TO_STRING_TO_UPPER_CASE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByToStringToLowerCaseQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_TO_STRING_TO_LOWER_CASE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectIntValueQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_INT_VALUE_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(0, 3);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectLongValueQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_LONG_VALUE_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(0L, 3L);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectDoubleValueQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_DOUBLE_VALUE_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList(0d, 3d);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectShortValueQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_SHORT_VALUE_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = Arrays.asList((short) 0, (short) 3);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }
  /* ----- Whitelisted Methods Tests ----- */

  /* ----- Blacklisted Methods Tests ----- */
  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByGetClassQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_GET_CLASS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByGetClassParenQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_GET_CLASS_PAREN;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectGetClassRegionQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_GET_CLASS_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectGetClassParenRegionQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_GET_CLASS_PAREN_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByClassQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_CLASS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectClassRegionQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_CLASS_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectByCapitalClassQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_BY_CAPITAL_CLASS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectCapitalClassRegionQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_CAPITAL_CLASS_FROM_REGION;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectRegionGetClassQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_GET_CLASS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectRegionGetClassParenQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_GET_CLASS_PAREN;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectRegionClassQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_CLASS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectRegionCapitalClassQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_CAPITAL_CLASS;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectRegionCloneQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_CLONE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  @Parameters(method = "getAllUsers")
  public void checkUserAuthorizationsForSelectToDateQuery(String user) {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String query = UserPermissions.SELECT_FROM_REGION_TO_DATE;
    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    QueryTestObject obj1 = new QueryTestObject(0, "John");
    obj1.setDateField("08/08/2018");
    QueryTestObject obj2 = new QueryTestObject(3, "Beth");
    obj2.setDateField("08/08/2018");
    Object[] values = {obj1, obj2};
    putIntoRegion(superUserClient, keys, values, regionName);

    List<Object> expectedResults = new ArrayList<>();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, user, regionName, expectedResults);
  }

  @Test
  public void userWithRegionAccessAndPassingInWrappedbindParameterShouldExecute() {
    String user = "dataReaderRegion";
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    String query = "select v from $1 r, r.values() v";
    String regexForExpectedException = ".*DATA:READ.*";
    specificUserClient.invoke(()-> {
      Region region = getClientCache().getRegion(regionName);
      HashSet hashset = new HashSet();
      hashset.add(region);
      getClientCache().getQueryService().newQuery(query).execute(new Object[]{hashset});
    });
  }

  @Test
  public void userWithoutRegionAccessAndPassingInWrappedBindParameterShouldThrowException() {
    String user = "dataReaderRegionKey";
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    String query = "select v from $1 r, r.values() v";
    String regexForExpectedException = ".*DATA:READ.*";
    specificUserClient.invoke(()-> {
      Region region = getClientCache().getRegion(regionName);
      HashSet hashset = new HashSet();
      hashset.add(region);
      assertExceptionOccurred(getClientCache().getQueryService(), query, new Object[]{hashset}, regexForExpectedException);
    });
  }

  //If DummyQRegion is every serializable, then this test will fail and a security hole with query will have been opened
  //That means a user could wrap a region in a dummy region and bypass the MethodInvocationAuthorizer
  @Test
  public void userWithoutRegionAccessAndPassingInWrappedInDummyQRegionBindParameterShouldThrowSerializationException() {
    String user = "dataReaderRegionKey";
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, UserPermissions.REGION_NAME);

    String regionName = UserPermissions.REGION_NAME;

    Object[] keys = {REGION_PUT_KEY, REGION_PUT_KEY + "1"};
    Object[] values = {new QueryTestObject(0, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);

    String query = "select v from $1 r, r.values() v";
    String regexForExpectedException = ".*failed serializing object.*";
    specificUserClient.invoke(()-> {
      Region region = getClientCache().getRegion(regionName);
      HashSet hashset = new HashSet();
      hashset.add(new DummyQRegion(region));
      assertExceptionOccurred(getClientCache().getQueryService(), query, new Object[]{hashset}, regexForExpectedException);
    });
  }

  public static class QueryTestObject implements Serializable {
    public int id = -1;

    private String name;
    private String Class;

    public Date dateField;

    public QueryTestObject(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public String someMethod() {
      return name + ":" + id;
    }

    public void setDateField(String dateString) {
      try {
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
        dateField = sdf.parse(dateString);
      } catch (ParseException e) {

      }
    }

    @Override
    public String toString() {
      return "Test_Object";
    }

    @Override
    public boolean equals(Object obj) {
      QueryTestObject qto = (QueryTestObject) obj;
      return (this.id == qto.id && this.name.equals(qto.getName()));
    }
  }
}
