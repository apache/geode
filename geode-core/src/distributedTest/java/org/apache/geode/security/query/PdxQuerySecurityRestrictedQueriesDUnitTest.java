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

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.security.query.data.PdxQueryTestObject;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
@RunWith(Parameterized.class)
public class PdxQuerySecurityRestrictedQueriesDUnitTest extends QuerySecurityBase {
  @Parameterized.Parameters
  public static Object[] usersAllowed() {
    return new Object[] {"dataReader", "dataReaderRegion", "clusterManagerDataReader",
        "clusterManagerDataReaderRegion", "super-user"};
  }

  @Parameterized.Parameter
  public String user;

  private String regexForExpectedExceptions = ".*Unauthorized access.*";

  @Before
  public void configureCache() {
    createClientCache(specificUserClient, user, userPerms.getUserPassword(user));
    createProxyRegion(specificUserClient, regionName);

    keys = new Object[] {"key-0", "key-1"};
    values = new Object[] {new PdxQueryTestObject(0, "John"), new PdxQueryTestObject(3, "Beth")};
    putIntoRegion(superUserClient, keys, values, regionName);
  }

  @Test
  public void selectWhenInvokingMethodOnPdxObjectQueryShouldBeRestricted() {
    String query = "select r.getClass from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void invokingMethodOnPdxObjectShouldBeRestricted() {
    String query = "select r.getAge from /" + regionName + " r";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }

  @Test
  public void selectWithPdxFieldNoExistingPublicFieldQueryShouldBeRestricted() {
    String query = "select * from /" + regionName + " r where r.name = 'Beth'";
    executeQueryWithCheckForAccessPermissions(specificUserClient, query, regionName,
        regexForExpectedExceptions);
  }
}
