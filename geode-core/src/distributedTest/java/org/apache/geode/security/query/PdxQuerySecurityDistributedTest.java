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

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.security.query.data.PdxQueryTestObject;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Category({SecurityTest.class, OQLQueryTest.class})
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PdxQuerySecurityDistributedTest extends AbstractQuerySecurityDistributedTest {

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

  private final String regexForExpectedExceptions = ".*Unauthorized access.*";

  @Before
  public void setUp() throws Exception {
    super.setUp(user, regionShortcut);

    keys = new Object[] {"key-0", "key-1"};
    values = new Object[] {
        new PdxQueryTestObject(0, "John"),
        new PdxQueryTestObject(3, "Beth")
    };

    putIntoRegion(superUserClient, keys, values, regionName);
  }

  @Test
  public void queryWithPublicFieldAccessOnWhereClauseShouldNotThrowSecurityException() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.id = 3";
    List<Object> expectedResults = Collections.singletonList(values[1]);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  @Test
  public void queryWithPdxNamedFieldAccessOnWhereClauseShouldNotThrowSecurityException() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getName = 'Beth'";
    List<Object> expectedResults = Collections.singletonList(values[1]);
    executeQueryAndAssertExpectedResults(specificUserClient, query, expectedResults);
  }

  @Test
  public void queryWithMethodInvocationShouldThrowSecurityExceptionForPdxObjects() {
    String query1 = "SELECT r.getAge FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query1,
        regexForExpectedExceptions);

    String query2 = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.name = 'Beth'";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query2,
        regexForExpectedExceptions);
  }

  @Test
  public void queryWithPermanentlyForbiddenMethodShouldThrowSecurityExceptionForPdxObjects() {
    String query1 = "SELECT r.getClass FROM " + SEPARATOR + regionName + " r";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query1,
        regexForExpectedExceptions);

    String query2 = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getClass() != 3";
    executeQueryAndAssertThatNoAuthorizedExceptionWasThrown(specificUserClient, query2,
        regexForExpectedExceptions);
  }
}
