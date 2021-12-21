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
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Category({SecurityTest.class, OQLQueryTest.class})
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class QuerySecurityForbiddenQueriesDistributedTest
    extends AbstractQuerySecurityDistributedTest {

  @Parameterized.Parameters(name = "User:{0}, RegionType:{1}")
  public static Object[] usersAndRegionTypes() {
    return new Object[][] {
        {"super-user", REPLICATE}, {"super-user", PARTITION},
        {"dataReader", REPLICATE}, {"dataReader", PARTITION},
        {"dataReaderRegion", REPLICATE}, {"dataReaderRegion", PARTITION},
        {"dataReaderRegionKey", REPLICATE}, {"dataReaderRegionKey", PARTITION},
        {"clusterManagerDataReader", REPLICATE}, {"clusterManagerDataReader", PARTITION},
        {"clusterManagerDataReaderRegion", REPLICATE}, {"clusterManagerDataReaderRegion", PARTITION}
    };
  }

  @Parameterized.Parameter
  public String user;

  @Parameterized.Parameter(1)
  public RegionShortcut regionShortcut;

  private final String regexForExpectedExceptions =
      ".*Unauthorized access.*|.*dataReaderRegionKey not authorized for DATA:READ:region.*";

  @Before
  public void setUp() throws Exception {
    super.setUp(user, regionShortcut);

    keys = new Object[] {"key-0", "key-1"};
    values = new Object[] {
        new QueryTestObject(1, "John"),
        new QueryTestObject(3, "Beth")
    };

    putIntoRegion(superUserClient, keys, values, regionName);
  }

  /* ----- Implicit Getter Tests ----- */
  @Test
  public void queryWithImplicitMethodInvocationOnWhereClauseShouldThrowSecurityException() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.name = 'Beth'";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query,
        regexForExpectedExceptions);
  }

  @Test
  public void queryWithImplicitMethodInvocationOnSelectClauseShouldThrowSecurityException() {
    String query = "SELECT r.name FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query,
        regexForExpectedExceptions);
  }

  @Test
  public void queriesWithImplicitMethodInvocationUsedWithinAggregateFunctionsShouldThrowSecurityException() {
    String queryCount = "SELECT COUNT(r.name) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryCount,
        regexForExpectedExceptions);

    String queryMax = "SELECT MAX(r.name) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryMax,
        regexForExpectedExceptions);

    String queryMin = "SELECT MIN(r.name) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryMin,
        regexForExpectedExceptions);

    String queryAvg = "SELECT AVG(r.name) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryAvg,
        regexForExpectedExceptions);

    String querySum = "SELECT SUM(r.name) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, querySum,
        regexForExpectedExceptions);
  }

  @Test
  public void queryWithImplicitMethodInvocationUsedWithinDistinctClauseShouldThrowSecurityException() {
    String query = "<TRACE> SELECT DISTINCT * from " + SEPARATOR + regionName
        + " r WHERE r.name IN SET('John', 'Beth') ORDER BY r.id asc LIMIT 2";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query,
        regexForExpectedExceptions);
  }

  @Test
  public void queryWithImplicitMethodInvocationOnInnerQueriesShouldThrowSecurityException() {
    String query = "SELECT * FROM " + SEPARATOR + regionName
        + " r1 WHERE r1.name IN (SELECT r2.name FROM " + SEPARATOR
        + regionName + " r2)";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query,
        regexForExpectedExceptions);
  }

  /* ----- Direct Getter Tests ----- */
  @Test
  public void queryWithExplicitMethodInvocationOnWhereClauseShouldThrowSecurityException() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getName = 'Beth'";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query,
        regexForExpectedExceptions);
  }

  @Test
  public void queryWithExplicitMethodInvocationOnSelectClauseShouldThrowSecurityException() {
    String query = "SELECT r.getName FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query,
        regexForExpectedExceptions);
  }

  @Test
  public void queriesWithExplicitMethodInvocationUsedWithinAggregateFunctionsShouldThrowSecurityException() {
    String queryCount = "SELECT COUNT(r.getName) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryCount,
        regexForExpectedExceptions);

    String queryMax = "SELECT MAX(r.getName) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryMax,
        regexForExpectedExceptions);

    String queryMin = "SELECT MIN(r.getName) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryMin,
        regexForExpectedExceptions);

    String queryAvg = "SELECT AVG(r.getName) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryAvg,
        regexForExpectedExceptions);

    String querySum = "SELECT SUM(r.getName) FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, querySum,
        regexForExpectedExceptions);
  }

  @Test
  public void queryWithExplicitMethodInvocationUsedWithinDistinctClauseShouldThrowSecurityException() {
    String query = "<TRACE> SELECT DISTINCT * from " + SEPARATOR + regionName
        + " r WHERE r.getName IN SET('John', 'Beth') ORDER BY r.id asc LIMIT 2";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query,
        regexForExpectedExceptions);
  }

  @Test
  public void queryWithExplicitMethodInvocationOnInnerQueriesShouldThrowSecurityException() {
    String query = "SELECT * FROM " + SEPARATOR + regionName
        + " r1 WHERE r1.getName IN (SELECT r2.getName FROM " + SEPARATOR + regionName + " r2)";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query,
        regexForExpectedExceptions);
  }

  /* ----- Region Methods ----- */
  @Test
  public void queriesWithAllowedRegionMethodInvocationsShouldThrowSecurityExceptionForNonAuthorizedUsers() {
    Assume.assumeTrue(user.equals("dataReaderRegionKey"));

    String queryValues = "SELECT * FROM " + SEPARATOR + regionName + ".values";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryValues,
        regexForExpectedExceptions);

    String queryKeySet = "SELECT * FROM " + SEPARATOR + regionName + ".keySet";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryKeySet,
        regexForExpectedExceptions);

    String queryContainsKey =
        "SELECT * FROM " + SEPARATOR + regionName + ".containsKey('" + keys[0] + "')";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryContainsKey,
        regexForExpectedExceptions);

    String queryEntrySet = "SELECT * FROM " + SEPARATOR + regionName + ".get('" + keys[0] + "')";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryEntrySet,
        regexForExpectedExceptions);
  }

  @Test
  public void queriesWithRegionMutatorMethodInvocationsShouldThrowSecurityException() {
    String queryCreate = "SELECT * FROM " + SEPARATOR + regionName + ".create('key2', 15)";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryCreate,
        regexForExpectedExceptions);
    assertRegionData(superUserClient, Arrays.asList(values));

    String queryPut = "SELECT * FROM " + SEPARATOR + regionName + ".put('key-2', 'something')";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryPut,
        regexForExpectedExceptions);
    assertRegionData(superUserClient, Arrays.asList(values));

    String queryRemove = "SELECT * FROM " + SEPARATOR + regionName + ".remove('key-0')";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryRemove,
        regexForExpectedExceptions);
    assertRegionData(superUserClient, Arrays.asList(values));

    String queryDestroy = "SELECT * FROM " + SEPARATOR + regionName + ".destroyRegion()";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, queryDestroy,
        regexForExpectedExceptions);
  }

  /* ----- Other Forbidden Methods ----- */
  @Test
  public void queryWithGetClassShouldThrowSecurityException() {
    String query1 = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getClass != '1'";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query1,
        regexForExpectedExceptions);

    String query2 = "SELECT r.getClass FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query2,
        regexForExpectedExceptions);

    String query3 = "SELECT r.getClass() FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query3,
        regexForExpectedExceptions);

    String query4 = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getClass != 'blah'";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query4,
        regexForExpectedExceptions);

    String query5 = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getClass() != '1'";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query5,
        regexForExpectedExceptions);

    String query6 = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.Class != '1'";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query6,
        regexForExpectedExceptions);
  }

  @Test
  public void queryWithExplicitNonExistingMethodInvocationShouldReturnUndefined() {
    Assume.assumeFalse(user.equals("dataReaderRegionKey"));
    String query = "SELECT r.getInterestListRegex() FROM " + SEPARATOR + regionName + " r";
    List<Object> expectedResults = Arrays.asList(QueryService.UNDEFINED, QueryService.UNDEFINED);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  @Test
  public void queryWithCloneMethodOnQRegionShouldReturnEmptyResult() {
    Assume.assumeFalse(user.equals("dataReaderRegionKey"));
    String query = "SELECT * FROM " + SEPARATOR + regionName + ".clone";
    List<Object> expectedResults = Collections.emptyList();
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  @Test
  public void queryWithExplicitNonExistingMethodInvocationOnQRegionShouldReturnEmptyResult() {
    Assume.assumeFalse(user.equals("dataReaderRegionKey"));
    String query = "SELECT * FROM " + SEPARATOR + regionName + ".getKey('" + keys[0] + "')";
    List<Object> expectedResults = Collections.emptyList();
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  @Test
  public void queryWithExplicitCreateMethodInvocationOnRegionShouldReturnUndefinedAndDoNotModifyRegion() {
    Assume.assumeFalse(user.equals("dataReaderRegionKey"));
    String query = "SELECT r.create('key2', 15) FROM " + SEPARATOR + regionName + " r";
    List<Object> expectedResults = Arrays.asList(QueryService.UNDEFINED, QueryService.UNDEFINED);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
    assertRegionData(specificUserClient, Arrays.asList(values));
  }

  @Test
  public void queryWithExplicitMutatorMethodInvocationsOnRegionShouldReturnEmptyResultAndDoNotModifyRegion() {
    Assume.assumeFalse(user.equals("dataReaderRegionKey"));
    String queryDestroy =
        "SELECT * FROM " + SEPARATOR + regionName + ".destroyKey('" + keys[0] + "')";
    executeQueryAndAssertExpectedResults(specificUserClient, queryDestroy, Collections.emptyList());
    assertRegionData(superUserClient, Arrays.asList(values));

    String queryPutIfAbsent =
        "SELECT * FROM " + SEPARATOR + regionName + ".putIfAbsent('key-2', 'something')";
    executeQueryAndAssertExpectedResults(specificUserClient, queryPutIfAbsent,
        Collections.emptyList());
    assertRegionData(superUserClient, Arrays.asList(values));

    String queryReplace =
        "SELECT * FROM " + SEPARATOR + regionName + ".replace('key-0', 'something')";
    executeQueryAndAssertExpectedResults(specificUserClient, queryReplace, Collections.emptyList());
    assertRegionData(superUserClient, Arrays.asList(values));
  }
}
