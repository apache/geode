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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class QuerySecurityAllowedQueriesDUnitTest extends QuerySecurityBase {

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
  public void checkUserAuthorizationsForSelectAllQuery() {
    String query = "select * from /" + regionName;
    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  /* ----- Public Field Tests ----- */
  @Test
  public void checkUserAuthorizationsForSelectByPublicFieldQuery() {
    String query = "select * from /" + regionName + " r where r.id = 1";
    List<Object> expectedResults = Arrays.asList(values[0]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectPublicFieldQuery() {
    String query = "select r.id from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(1, 3);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectCountOfPublicFieldQuery() {
    String query = "select count(r.id) from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(2);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectMaxOfPublicFieldQuery() {
    String query = "select max(r.id) from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(3);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectMinOfPublicFieldQuery() {
    String query = "select min(r.id) from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(1);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectPublicFieldFromRegionByPublicFieldFromRegionQuery() {
    String query = "select * from /" + regionName + " r1 where r1.id in (select r2.id from /"
        + regionName + " r2)";
    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectRegionContainsKeyQuery() {
    String query = "select * from /" + regionName + ".containsKey('" + keys[0] + "')";
    List<Object> expectedResults = Arrays.asList(true);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectValuesQuery() {
    String query = "select * from /" + regionName + ".values";
    List<Object> expectedResults = Arrays.asList(values);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectKeySetQuery() {
    String query = "select * from /" + regionName + ".keySet";
    List<Object> expectedResults = Arrays.asList(keys);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectEntriesQuery() {
    String query = "select e.getKey from /" + regionName + ".entries e";
    List<Object> expectedResults = Arrays.asList(keys);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectKeyFromEntrySetQuery() {
    String query = "select e.key from /" + regionName + ".entrySet e";
    List<Object> expectedResults = Arrays.asList(keys);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectGetKeyFromEntrySetQuery() {
    String query = "select e.getKey from /" + regionName + ".entrySet e";
    List<Object> expectedResults = Arrays.asList(keys);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectValueFromEntrySetQuery() {
    String query = "select e.value from /" + regionName + ".entrySet e";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        Arrays.asList(values));
  }

  @Test
  public void checkUserAuthorizationsForSelectGetValueFromEntrySetQuery() {
    String query = "select e.getValue from /" + regionName + ".entrySet e";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        Arrays.asList(values));
  }

  @Test
  public void checkUserAuthorizationsForSelectByToStringQuery() {
    String query = "select * from /" + regionName + " r where r.toString = 'Test_Object'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        Arrays.asList(values));
  }

  @Test
  public void checkUserAuthorizationsForSelectToStringQuery() {
    String query = "select r.toString() from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList("Test_Object", "Test_Object");
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectByToStringToUpperCaseQuery() {
    String query =
        "select * from /" + regionName + " r where r.toString().toUpperCase = 'TEST_OBJECT'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        Arrays.asList(values));
  }

  @Test
  public void checkUserAuthorizationsForSelectByToStringToLowerCaseQuery() {
    String query =
        "select * from /" + regionName + " r where r.toString().toLowerCase = 'test_object'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        Arrays.asList(values));
  }

  @Test
  public void checkUserAuthorizationsForSelectIntValueQuery() {
    String query = "select r.id.intValue() from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(1, 3);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectLongValueQuery() {
    String query = "select r.id.longValue() from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(1L, 3L);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectDoubleValueQuery() {
    String query = "select r.id.doubleValue() from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList(1d, 3d);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectShortValueQuery() {
    String query = "select r.id.shortValue() from /" + regionName + " r";
    List<Object> expectedResults = Arrays.asList((short) 1, (short) 3);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectToDateQuery() {
    String query =
        "SELECT * FROM /" + regionName + " where dateField = to_date('08/08/2018', 'MM/dd/yyyy')";

    QueryTestObject obj1 = new QueryTestObject(0, "John");
    obj1.setDateField("08/08/2018");
    QueryTestObject obj2 = new QueryTestObject(3, "Beth");
    obj2.setDateField("08/08/2018");
    Object[] values = {obj1, obj2};
    putIntoRegion(superUserClient, keys, values, regionName);

    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        Arrays.asList(values));
  }

  @Test
  public void checkUserAuthorizationsForSelectByMapFieldQuery() {
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
    executeQueryWithCheckForAccessPermissions(specificUserClient, query1, regionName,
        Arrays.asList(new Object[] {valueObject1}));

    String query2 =
        String.format("SELECT * FROM /%s WHERE mapField.get('strData2') = 'ZZZ'", regionName);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query2, regionName,
        Arrays.asList(values));
  }
}
