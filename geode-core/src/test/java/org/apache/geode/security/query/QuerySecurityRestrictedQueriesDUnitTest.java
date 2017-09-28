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

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class QuerySecurityRestrictedQueriesDUnitTest extends QuerySecurityBase {

  @Parameterized.Parameters
  public static Object[] usersAllowed() {
    return new Object[] {"dataReader", "dataReaderRegion", "clusterManagerDataReader",
        "clusterManagerDataReaderRegion", "super-user"};
  }

  @Parameterized.Parameter
  public String user;

  @Before
  public void configureCache() {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, regionName);

    keys = new Object[] {"key-0", "key-1"};
    values = new Object[] {new QueryTestObject(1, "John"), new QueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);
  }

  protected String regexForExpectedExceptions = ".*Unauthorized access.*";


  /* ----- Implicit Getter Tests ----- */
  @Test
  public void checkUserAuthorizationsForSelectByImplicitGetterQuery() {
    String query = "select * from /" + regionName + " r where r.name = 'Beth'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectImplicitGetterQuery() {
    String query = "select r.name from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectCountOfImplicitGetterQuery() {
    String query = "select count(r.name) from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectMaxOfImplicitGetterQuery() {
    String query = "select max(r.name) from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectMinOfImplicitGetterQuery() {
    String query = "select min(r.name) from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectImplicitGetterFromRegionByImplicitGetterFromRegionQuery() {
    String query = "select * from /" + regionName + " r1 where r1.name in (select r2.name from /"
        + regionName + " r2)";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }
  /* ----- Implicit Getter Tests ----- */

  /* ----- Direct Getter Tests ----- */
  @Test
  public void checkUserAuthorizationsForSelectByDirectGetterQuery() {
    String query = "select * from /" + regionName + " r where r.getName = 'Beth'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectDirectGetterQuery() {
    String query = "select r.getName() from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectCountOfDirectGetterQuery() {
    String query = "select count(r.getId) from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectMaxOfDirectGetterQuery() {
    String query = "select max(r.getId()) from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectMinOfDirectGetterQuery() {
    String query = "select min(getId()) from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectDirectGetterFromRegionByDirectGetterFromRegionQuery() {
    String query = "select * from /" + regionName
        + " r1 where r1.getName in (select r2.getName from /" + regionName + " r2)";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectRegionContainsValueQuery() {
    String query = "select * from /" + regionName + ".containsValue('value')";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void usersWhoCanExecuteQueryShouldNotInvokeRegionCreateForSelectRegionCreateQuery()
      throws Exception {
    String query = "select * from /" + regionName + ".create('key2', 15)";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }


  @Test
  // @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldGetResultsForSelectCreateFromRegionQuery()
      throws Exception {
    String query = "select r.create('key2', 15) from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }

  @Test
  // @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldNotInvokeDestroyForSelectRegionDestroyQuery()
      throws Exception {
    String query = "select * from /" + regionName + ".destroyKey('" + keys[0] + "')";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }

  @Test
  public void checkUserAuthorizationsForSelectRegionDestroyRegionQuery() {
    String query = "select * from /" + regionName + ".destroyRegion()";
    try {
      executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
          regexForExpectedExceptions);
    } catch (Throwable throwable) {
      if (!(throwable.getCause().getCause() instanceof RegionDestroyedException)) {
        throw throwable;
      }
    }
  }

  @Test
  public void checkUserAuthorizationsForSelectRegionGetQuery() {
    String query = "select * from /" + regionName + ".getKey('" + keys[0] + "')";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void usersWhoCanExecuteQueryShouldNotInvokeRegionPutForSelectRegionPutQuery()
      throws Exception {
    String query = "select * from /" + regionName + ".put('key-2', 'something')";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }



  @Test
  public void usersWhoCanExecuteQueryShouldNotInvokePutIfAbsentForSelectRegionPutIfAbsentQuery()
      throws Exception {
    String query = "select * from /" + regionName + ".putIfAbsent('key-2', 'something')";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }


  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCanExecuteQueryShouldNotInvokedRegionRemoveForSelectRegionRemoveQuery()
      throws Exception {
    String query = "select * from /" + regionName + ".remove('key-0')";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }

  @Test
  @Parameters(method = "getAllUsersWhoCanExecuteQuery")
  public void usersWhoCannExecuteQueryShouldReceiveExpectedResultsForSelectRegionReplaceQuery()
      throws Exception {
    String query = "select * from /" + regionName + ".replace('key-0', 'something')";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }

  @Test
  public void checkUserAuthorizationsForSelectGetInterestListRegexRegionQuery() {
    String query = "select r.getInterestListRegex from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectGetInterestListRegexParenRegionQuery() {
    String query = "select r.getInterestListRegex() from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectByGetClassQuery() {
    String query = "select * from /" + regionName + " r where r.getClass != '1'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectGetClassRegionQuery() {
    String query = "select r.getClass from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectGetClassWithParenthesisRegionQuery() {
    String query = "select r.getClass() from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectByClassQuery() {
    String query = "select * from /" + regionName + " r where r.getClass != 'blah'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectByGetClassWithParenthesisQuery() {
    String query = "select * from /" + regionName + " r where r.getClass() != '1'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }


  @Test
  public void checkUserAuthorizationsForSelectByCapitalClassQuery() {
    String query = "select * from /" + regionName + " r where r.Class != '1'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void checkUserAuthorizationsForSelectRegionCloneQuery() {
    String query = "select * from /" + regionName + ".clone";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }
}
