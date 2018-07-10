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

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.query.QueryService;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class QuerySecurityRetrictedButMethodsDoNotExistDUnitTest extends QuerySecurityBase {

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

  @Test
  public void executingMethodThatDoesNotExistOnResultsWillReturnUndefinedAsAResult() {
    String query = "select r.getInterestListRegex() from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(QueryService.UNDEFINED, QueryService.UNDEFINED);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void cloneExecutedOnQRegionWillReturnEmptyResults() {
    String query = "select * from /" + regionName + ".clone";
    List<Object> expectedResults = Arrays.asList();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void executingMethodThatDoesNotExistOnQRegionWillReturnEmptyResult() {
    String query = "select * from /" + regionName + ".getKey('" + keys[0] + "')";
    List<Object> expectedResults = Arrays.asList();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void executingCreateOnRegionWillResultInUndefinedButNotModifyRegion() throws Exception {
    String query = "select r.create('key2', 15) from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(QueryService.UNDEFINED, QueryService.UNDEFINED);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }

  @Test
  public void executingDestroyKeyOnRegionWillReturnEmptyResultsAndNotModifyRegion()
      throws Exception {
    String query = "select * from /" + regionName + ".destroyKey('" + keys[0] + "')";
    List<Object> expectedResults = Arrays.asList();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }

  @Test
  public void executingPutIfAbsentOnRegionWillReturnEmptyResultsAndNotModifyRegion()
      throws Exception {
    String query = "select * from /" + regionName + ".putIfAbsent('key-2', 'something')";
    List<Object> expectedResults = Arrays.asList();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }

  @Test
  public void executingReplaceOnRegionWillReturnEmptyResultsAndNotModifyRegion() throws Exception {
    String query = "select * from /" + regionName + ".replace('key-0', 'something')";
    List<Object> expectedResults = Arrays.asList();
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
    executeAndConfirmRegionMatches(specificUserClient, regionName, Arrays.asList(values));
  }

  @Test
  public void checkUserAuthorizationsForSelectGetInterestListRegexParenRegionQuery() {
    String query = "select r.getInterestListRegex() from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(QueryService.UNDEFINED, QueryService.UNDEFINED);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

}
