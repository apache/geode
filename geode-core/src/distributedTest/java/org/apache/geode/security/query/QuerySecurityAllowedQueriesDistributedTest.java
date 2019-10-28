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

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Category({SecurityTest.class, OQLQueryTest.class})
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class QuerySecurityAllowedQueriesDistributedTest
    extends AbstractQuerySecurityDistributedTest {

  @Parameterized.Parameters(name = "User:{0}, RegionType:{1}")
  public static Object[] usersAndRegionTypes() {
    return new Object[][] {
        {"super-user", REPLICATE}, {"super-user", PARTITION},
        {"dataReader", REPLICATE}, {"dataReader", PARTITION},
        {"dataReaderRegion", REPLICATE}, {"dataReaderRegion", PARTITION},
        {"clusterManagerDataReader", REPLICATE}, {"clusterManagerDataReader", PARTITION},
        {"clusterManagerDataReaderRegion", REPLICATE}, {"clusterManagerDataReaderRegion", PARTITION}
    };
  }

  @Parameterized.Parameter
  public String user;

  @Parameterized.Parameter(1)
  public RegionShortcut regionShortcut;

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

  @Test
  public void queryWithSelectStarShouldNotThrowSecurityException() {
    String query = "SELECT * FROM /" + regionName;
    List<Object> expectedResults = Arrays.asList(values);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  /* ----- Public Field Tests ----- */
  @Test
  public void queryWithPublicFieldOnWhereClauseShouldNotThrowSecurityException() {
    String query = "SELECT * FROM /" + regionName + " r WHERE r.id = 1";
    List<Object> expectedResults = Collections.singletonList(values[0]);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  @Test
  public void queryingByPublicFieldOnSelectClauseShouldNotThrowSecurityException() {
    String query = "SELECT r.id FROM /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(1, 3);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  @Test
  public void queriesWithPublicFieldUsedWithinAggregateFunctionsShouldNotThrowSecurityException() {
    String queryCount = "SELECT COUNT(r.id) FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, queryCount,
        Collections.singletonList(2));

    String queryMax = "SELECT MAX(r.id) FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, queryMax,
        Collections.singletonList(3));

    String queryMin = "SELECT MIN(r.id) FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, queryMin,
        Collections.singletonList(1));

    String queryAvg = "SELECT AVG(r.id) FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, queryAvg,
        Collections.singletonList(2));

    String querySum = "SELECT SUM(r.id) FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, querySum,
        Collections.singletonList(4));
  }

  @Test
  public void queryWithPublicFieldUsedWithinDistinctClauseShouldNotThrowSecurityException() {
    String query = "<TRACE> SELECT DISTINCT * from /" + regionName
        + " WHERE id IN SET(1, 3) ORDER BY id asc LIMIT 2";
    executeQueryAndAssertExpectedResults(specificUserClient, query, Arrays.asList(values));
  }

  @Test
  public void queryWithByPublicFieldOnInnerQueriesShouldNotThrowSecurityException() {
    String query = "SELECT * FROM /" + regionName + " r1 WHERE r1.id IN (SELECT r2.id FROM /"
        + regionName + " r2)";
    List<Object> expectedResults = Arrays.asList(values);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  /* ----- Default Allowed Methods Tests ----- */
  @Test
  public void queriesWithAllowedRegionMethodInvocationsShouldNotThrowSecurityException() {
    String queryValues = "SELECT * FROM /" + regionName + ".values";
    executeQueryAndAssertExpectedResults(specificUserClient, queryValues, Arrays.asList(values));

    String queryKeySet = "SELECT * FROM /" + regionName + ".keySet";
    executeQueryAndAssertExpectedResults(specificUserClient, queryKeySet, Arrays.asList(keys));

    String queryContainsKey = "SELECT * FROM /" + regionName + ".containsKey('" + keys[0] + "')";
    executeQueryAndAssertExpectedResults(specificUserClient, queryContainsKey,
        Collections.singletonList(true));

    String queryEntrySet = "SELECT * FROM /" + regionName + ".get('" + keys[0] + "')";
    executeQueryAndAssertExpectedResults(specificUserClient, queryEntrySet,
        Collections.singletonList(values[0]));
  }

  @Test
  public void queriesWithAllowedRegionEntryMethodInvocationsShouldNotThrowSecurityException() {
    List<Object> expectedKeys = Arrays.asList(keys);
    String queryKeyEntrySet = "SELECT e.key FROM /" + regionName + ".entrySet e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryKeyEntrySet, expectedKeys);
    String queryGetKeyEntrySet = "SELECT e.getKey FROM /" + regionName + ".entrySet e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryGetKeyEntrySet, expectedKeys);
    String queryKeyEntries = "SELECT e.key FROM /" + regionName + ".entries e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryKeyEntries, expectedKeys);
    String queryGetKeyEntries = "SELECT e.getKey FROM /" + regionName + ".entries e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryGetKeyEntries, expectedKeys);

    List<Object> expectedValues = Arrays.asList(values);
    String queryValueEntrySet = "SELECT e.value FROM /" + regionName + ".entrySet e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryValueEntrySet, expectedValues);
    String queryGetValueEntrySet = "SELECT e.getValue FROM /" + regionName + ".entrySet e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryGetValueEntrySet, expectedValues);
    String queryValueEntries = "SELECT e.value FROM /" + regionName + ".entries e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryValueEntries, expectedValues);
    String queryGetValueEntries = "SELECT e.getValue FROM /" + regionName + ".entries e";
    executeQueryAndAssertExpectedResults(specificUserClient, queryGetValueEntries, expectedValues);
  }

  @Test
  public void queriesWithAllowedStringMethodInvocationsShouldNotThrowSecurityException() {
    String toStringWhere = "SELECT * FROM /" + regionName + " r WHERE r.toString = 'Test_Object'";
    executeQueryAndAssertExpectedResults(specificUserClient, toStringWhere, Arrays.asList(values));

    String toStringSelect = "SELECT r.toString() FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, toStringSelect,
        Arrays.asList("Test_Object", "Test_Object"));

    String toUpperCaseWhere =
        "SELECT * FROM /" + regionName + " r WHERE r.toString().toUpperCase = 'TEST_OBJECT'";
    executeQueryAndAssertExpectedResults(specificUserClient, toUpperCaseWhere,
        Arrays.asList(values));

    String toLowerCaseWhere =
        "SELECT * FROM /" + regionName + " r WHERE r.toString().toLowerCase = 'test_object'";
    executeQueryAndAssertExpectedResults(specificUserClient, toLowerCaseWhere,
        Arrays.asList(values));
  }

  @Test
  public void queriesWithAllowedNumberMethodInvocationsShouldNotThrowSecurityException() {
    String intValue = "SELECT r.id.intValue() FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, intValue, Arrays.asList(1, 3));

    String longValue = "SELECT r.id.longValue() FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, longValue, Arrays.asList(1L, 3L));

    String doubleValue = "SELECT r.id.doubleValue() FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, doubleValue, Arrays.asList(1d, 3d));

    String shortValue = "SELECT r.id.shortValue() FROM /" + regionName + " r";
    executeQueryAndAssertExpectedResults(specificUserClient, shortValue,
        Arrays.asList((short) 1, (short) 3));
  }

  @Test
  public void queriesWithAllowedDateMethodInvocationsShouldNotThrowSecurityException() {
    String query =
        "SELECT * FROM /" + regionName + " WHERE dateField = to_date('08/08/2018', 'MM/dd/yyyy')";
    QueryTestObject obj1 = new QueryTestObject(0, "John");
    obj1.setDateField("08/08/2018");
    QueryTestObject obj2 = new QueryTestObject(3, "Beth");
    obj2.setDateField("08/08/2018");
    Object[] values = {obj1, obj2};
    putIntoRegion(superUserClient, keys, values, regionName);

    executeQueryAndAssertExpectedResults(specificUserClient, query, Arrays.asList(values));
  }

  @Test
  public void queriesWithAllowedMapMethodInvocationsShouldNotThrowSecurityException() {
    QueryTestObject valueObject1 = new QueryTestObject(1, "John");
    Map<Object, Object> map1 = new HashMap<>();
    map1.put("intData", 1);
    map1.put(1, 98);
    map1.put("strData1", "ABC");
    map1.put("strData2", "ZZZ");
    valueObject1.mapField = map1;

    QueryTestObject valueObject2 = new QueryTestObject(3, "Beth");
    Map<Object, Object> map2 = new HashMap<>();
    map2.put("intData", 99);
    map2.put(1, 99);
    map2.put("strData1", "XYZ");
    map2.put("strData2", "ZZZ");
    valueObject2.mapField = map2;

    values = new Object[] {valueObject1, valueObject2};
    putIntoRegion(superUserClient, keys, values, regionName);

    String query1 = String.format(
        "SELECT * FROM /%s WHERE mapField.get('intData') = 1 AND mapField.get(1) = 98 AND mapField.get('strData1') = 'ABC' AND mapField.get('strData2') = 'ZZZ'",
        regionName);
    executeQueryAndAssertExpectedResults(specificUserClient, query1,
        Arrays.asList(new Object[] {valueObject1}));

    String query2 =
        String.format("SELECT * FROM /%s WHERE mapField.get('strData2') = 'ZZZ'", regionName);
    executeQueryAndAssertExpectedResults(specificUserClient, query2, Arrays.asList(values));
  }
}
