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

import org.junit.Assume;
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
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Category({SecurityTest.class, OQLQueryTest.class})
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CqSecurityAllowedUsersDistributedTest extends AbstractQuerySecurityDistributedTest {

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

  // Variables that need to be shared across invoke calls.
  private static TestCqListener cqListener = null;
  private final String regexForExpectedExceptions = ".*Unauthorized access.*";

  @Before
  public void setUp() throws Exception {
    super.setUp(user, regionShortcut);

    keys = new Object[] {"key-0"};
    values = new Object[] {
        new QueryTestObject(0, "John")
    };
  }

  private CqQuery createCq(String query) throws CqException {
    TestCqListener cqListener = new TestCqListener();
    QueryService queryService = getClientCache().getQueryService();
    CqSecurityAllowedUsersDistributedTest.cqListener = cqListener;
    CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
    cqAttributesFactory.addCqListener(cqListener);

    return queryService.newCq(query, cqAttributesFactory.create());
  }

  private void executeCqAndAssertExceptionThrown(String query) {
    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);

      assertThatThrownBy(cq::execute)
          .as("Expected an exception when executing cq " + cq.getQueryString() + " with user "
              + user)
          .isInstanceOfAny(RegionNotFoundException.class, CqException.class)
          .hasMessageMatching(regexForExpectedExceptions);
    });
  }

  private void executeCqWithInitialResultsAndAssertExceptionThrown(String query) {
    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);

      assertThatThrownBy(cq::executeWithInitialResults)
          .as("Expected an exception when executing cq " + cq.getQueryString() + " with user "
              + user)
          .isInstanceOfAny(RegionNotFoundException.class, CqException.class)
          .hasMessageMatching(regexForExpectedExceptions);
    });
  }

  private void executeCqAndAssertThatOnErrorIsInvokedOnNextMatchingEvent(String query) {
    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      cq.execute();
    });

    putIntoRegion(superUserClient, keys, values, regionName);
    specificUserClient.invoke(
        () -> await().untilAsserted(() -> assertThat(cqListener.getNumErrors()).isEqualTo(1)));
  }

  private void executeCqWithInitialResultsAndAssertThatOnErrorIsInvokedOnNextMatchingEvent(
      String query) {
    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      cq.executeWithInitialResults();
    });

    Object[] keys = {"key-0"};
    Object[] values = {new QueryTestObject(1, "Mary")};
    putIntoRegion(superUserClient, keys, values, regionName);
    specificUserClient.invoke(
        () -> await().untilAsserted(() -> assertThat(cqListener.getNumErrors()).isEqualTo(1)));
  }

  @Test
  public void cqQueryWithPublicFieldOnNonEmptyRegionShouldNotThrowException() {
    putIntoRegion(superUserClient, keys, values, regionName);
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.id = 0";

    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      cq.execute();
    });

    Object[] newValues = new Object[] {new QueryTestObject(0, "Bethany")};
    putIntoRegion(superUserClient, keys, newValues, regionName);
    specificUserClient.invoke(
        () -> await().untilAsserted(() -> assertThat(cqListener.getNumEvents()).isEqualTo(1)));
  }

  @Test
  public void cqQueryWithImplicitMethodInvocationOnEmptyRegionShouldNotThrowExceptionDuringExecuteWithInitialResultsAndInvokeOnErrorForNextMatchingEvent() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.name = 'Beth'";

    executeCqWithInitialResultsAndAssertThatOnErrorIsInvokedOnNextMatchingEvent(query);
  }

  @Test
  public void cqQueryWithExplicitMethodInvocationOnEmptyRegionShouldNotThrowExceptionDuringExecuteWithInitialResultsAndInvokeOnErrorForNextMatchingEvent() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getName = 'Beth'";

    executeCqWithInitialResultsAndAssertThatOnErrorIsInvokedOnNextMatchingEvent(query);
  }

  @Test
  public void cqQueryWithImplicitMethodInvocationOnNonEmptyReplicateRegionShouldThrowExceptionDuringExecute() {
    Assume.assumeTrue(regionShortcut.equals(REPLICATE));
    putIntoRegion(superUserClient, keys, values, regionName);
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.name = 'Beth'";

    executeCqAndAssertExceptionThrown(query);
  }

  @Test
  public void cqQueryWithExplicitMethodInvocationOnNonEmptyReplicateRegionShouldThrowExceptionDuringExecute() {
    Assume.assumeTrue(regionShortcut.equals(REPLICATE));
    putIntoRegion(superUserClient, keys, values, regionName);
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getName = 'Beth'";

    executeCqAndAssertExceptionThrown(query);
  }

  @Test
  public void cqQueryWithImplicitMethodInvocationOnNonEmptyPartitionRegionShouldNotThrowExceptionDuringExecuteAndInvokeOnErrorForNextMatchingEvent() {
    Assume.assumeTrue(regionShortcut.equals(PARTITION));
    putIntoRegion(superUserClient, keys, values, regionName);
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.name = 'Beth'";

    executeCqAndAssertThatOnErrorIsInvokedOnNextMatchingEvent(query);
  }

  @Test
  public void cqQueryWithExplicitMethodInvocationOnNonEmptyPartitionRegionShouldNotThrowExceptionDuringExecuteAndInvokeOnErrorForNextMatchingEvent() {
    Assume.assumeTrue(regionShortcut.equals(PARTITION));
    putIntoRegion(superUserClient, keys, values, regionName);
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getName = 'Beth'";

    executeCqAndAssertThatOnErrorIsInvokedOnNextMatchingEvent(query);
  }

  @Test
  public void cqQueryWithImplicitMethodInvocationOnNonEmptyRegionShouldThrowExceptionDuringExecuteWithInitialResults() {
    putIntoRegion(superUserClient, keys, values, regionName);
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.name = 'Beth'";

    executeCqWithInitialResultsAndAssertExceptionThrown(query);
  }

  @Test
  public void cqQueryWithExplicitMethodInvocationOnNonEmptyRegionShouldThrowExceptionDuringExecuteWithInitialResults() {
    putIntoRegion(superUserClient, keys, values, regionName);
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.getName = 'Beth'";

    executeCqWithInitialResultsAndAssertExceptionThrown(query);
  }

  @Test
  public void cqCanBeClosedByTheCreator() {
    String query = "SELECT * FROM " + SEPARATOR + regionName + " r WHERE r.id = 0";

    specificUserClient.invoke(() -> {
      CqQuery cq = createCq(query);
      cq.execute();
      cq.close();
      assertThat(cq.isClosed()).isTrue();
    });

    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      assertThat(ClusterStartupRule.getCache().getCqService().getAllCqs().size()).isEqualTo(0);
    });
  }
}
