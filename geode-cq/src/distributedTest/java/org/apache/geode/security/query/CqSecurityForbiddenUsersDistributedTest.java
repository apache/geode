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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assume.assumeTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Category({SecurityTest.class, OQLQueryTest.class})
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CqSecurityForbiddenUsersDistributedTest extends AbstractQuerySecurityDistributedTest {

  @Parameterized.Parameters(name = "User:{0}, RegionType:{1}")
  public static Object[] usersAndRegionTypes() {
    return new Object[][] {
        {"stranger", REPLICATE}, {"stranger", PARTITION},
        {"dataWriter", REPLICATE}, {"dataWriter", PARTITION},
        {"dataReaderRegionKey", REPLICATE}, {"dataReaderRegionKey", PARTITION},
        {"clusterManagerQuery", REPLICATE}, {"clusterManagerQuery", PARTITION},
        {"clusterManagerDataReaderRegionKey", REPLICATE},
        {"clusterManagerDataReaderRegionKey", PARTITION}
    };
  }

  @Parameterized.Parameter
  public String user;

  @Parameterized.Parameter(1)
  public RegionShortcut regionShortcut;

  // Variables that need to be shared across invoke calls.
  private static TestCqListener cqListener = null;
  private final String regexForExpectedExceptions = ".*DATA:READ:.*";

  @Before
  public void setUp() throws Exception {
    super.setUp(user, regionShortcut);

    keys = new Object[] {"key-0"};
    values = new Object[] {new QueryTestObject(0, "John")};

    putIntoRegion(superUserClient, keys, values, regionName);
  }

  private CqQuery createCq(String query) throws CqException {
    TestCqListener cqListener = new TestCqListener();
    QueryService queryService = getClientCache().getQueryService();
    CqSecurityForbiddenUsersDistributedTest.cqListener = cqListener;
    CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
    cqAttributesFactory.addCqListener(cqListener);

    return queryService.newCq(query, cqAttributesFactory.create());
  }

  private void executeCqAndAssertException(CqQuery cq, String user,
      String regexForExpectedException) {
    assertThatThrownBy(cq::execute)
        .as("Expected an exception when executing cq " + cq.getQueryString() + " with user " + user)
        .isInstanceOfAny(RegionNotFoundException.class, CqException.class)
        .hasMessageMatching(regexForExpectedException);
  }

  private void executeCqWithInitialResultsAndAssertException(CqQuery cq, String user,
      String regexForExpectedException) {
    assertThatThrownBy(cq::executeWithInitialResults)
        .as("Expected an exception when executing cq " + cq.getQueryString() + " with user " + user)
        .isInstanceOfAny(RegionNotFoundException.class, CqException.class)
        .hasMessageMatching(regexForExpectedException);
  }

  @Test
  public void cqQueryWithPublicFieldOnNonEmptyRegionShouldThrowExceptionDuringExecute() {
    putIntoRegion(superUserClient, keys, values, regionName);
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.id = 0";

    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      executeCqAndAssertException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  public void cqQueryWithImplicitMethodInvocationOnNonEmptyRegionShouldThrowExceptionDuringExecute() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.name = 'Beth'";

    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      executeCqAndAssertException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  public void cqQueryWithExplicitMethodInvocationOnNonEmptyRegionShouldThrowExceptionDuringExecute() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getName = 'Beth'";

    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      executeCqAndAssertException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  public void cqQueryWithImplicitMethodInvocationOnNonEmptyRegionShouldThrowExceptionDuringExecuteWithInitialResults() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.name = 'Beth'";

    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      executeCqWithInitialResultsAndAssertException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  public void cqQueryWithExplicitMethodInvocationOnNonEmptyRegionShouldThrowExceptionDuringExecuteWithInitialResults() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getName = 'Beth'";

    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      executeCqWithInitialResultsAndAssertException(cq, user, regexForExpectedExceptions);
    });
  }

  @Test
  public void cqCreatedByAllowedUserButPutDoneByForbiddenReaderShouldStillInvokeListener() {
    assumeTrue(user.equals("dataWriter"));
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.id = 1";

    superUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      cq.execute();
    });

    Object[] keys = {"key-0"};
    Object[] values = {new QueryTestObject(1, "Mary")};
    putIntoRegion(specificUserClient, keys, values, regionName);

    superUserClient.invoke(
        () -> await().untilAsserted(() -> assertThat(cqListener.getNumEvents()).isEqualTo(1)));
  }
}
