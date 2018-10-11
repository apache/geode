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

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.security.query.data.PdxQueryTestObject;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category(SecurityTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PdxQuerySecurityAllowedQueriesDUnitTest extends QuerySecurityBase {
  @Parameterized.Parameters
  public static Object[] usersAllowed() {
    return new Object[] {"dataReader", "dataReaderRegion", "clusterManagerDataReader",
        "clusterManagerDataReaderRegion", "super-user"};
  }

  @Parameterized.Parameter
  public String user;

  @Before
  public void configureSpecificClientAndKeyAndValues() {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, regionName);

    keys = new Object[] {"key-0", "key-1"};
    values = new Object[] {new PdxQueryTestObject(0, "John"), new PdxQueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);
  }

  @Test
  public void checkUserAuthorizationsForSelectOnPublicFieldQuery() {
    String query = "select * from /" + regionName + " r where r.id = 3";
    List<Object> expectedResults = Collections.singletonList(values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }

  @Test
  public void checkUserAuthorizationsForSelectWithPdxFieldNamedGetQuery() {
    server.getCache().setReadSerializedForTest(true);
    String query = "select * from /" + regionName + " r where r.getName = 'Beth'";
    List<Object> expectedResults = Collections.singletonList(values[1]);
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        expectedResults);
  }
}
