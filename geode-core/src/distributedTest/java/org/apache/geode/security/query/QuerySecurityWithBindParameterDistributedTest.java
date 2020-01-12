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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.NotSerializableException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.index.DummyQRegion;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Category({SecurityTest.class, OQLQueryTest.class})
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class QuerySecurityWithBindParameterDistributedTest
    extends AbstractQuerySecurityDistributedTest {
  @Parameterized.Parameters(name = "RegionType:{0}")
  public static Object[] regionTypes() {
    return new Object[] {
        REPLICATE, PARTITION
    };
  }

  @Parameterized.Parameter
  public RegionShortcut regionShortcut;

  private final String queryString = "SELECT v FROM $1 r, r.values() v";

  private void assertExceptionOccurred(QueryService qs, Object[] bindParams,
      String authErrorRegexp) {
    assertThatThrownBy(() -> qs.newQuery(queryString).execute(bindParams))
        .hasMessageMatching(authErrorRegexp)
        .hasCauseInstanceOf(NotAuthorizedException.class);
  }

  @Before
  public void setUp() throws Exception {
    super.setUpSuperUserClientAndServer(regionShortcut);

    keys = new Object[] {"key-0", "key-1"};
    values = new Object[] {
        new QueryTestObject(1, "John"),
        new QueryTestObject(3, "Beth")
    };

    putIntoRegion(superUserClient, keys, values, regionName);
  }

  @Test
  public void queryWithRegionAsBindParameterShouldNotThrowSecurityExceptionWhenUserHasTheCorrectPrivileges()
      throws Exception {
    setUpSpecificClient("dataReaderRegion");

    specificUserClient.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      HashSet<Region> hashSet = new HashSet<>(Collections.singletonList(region));
      assertQueryResults(getClientCache(), queryString, new Object[] {hashSet},
          Arrays.asList(values));
    });
  }

  // If DummyQRegion is ever serializable, then this test will fail and a security hole with query
  // will have been opened
  // That means a user could wrap a region in a dummy region and bypass the
  // RestrictedMethodInvocationAuthorizer
  @Test
  public void queryWithQRegionAsBindParameterShouldThrowSerializationException() throws Exception {
    setUpSpecificClient("dataReaderRegionKey");
    String regexForExpectedException = ".*failed serializing object.*";

    specificUserClient.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      HashSet<QRegion> hashSet = new HashSet<>(Collections.singletonList(new DummyQRegion(region)));

      assertThatThrownBy(() -> getClientCache().getQueryService().newQuery(queryString)
          .execute(new Object[] {hashSet}))
              .hasMessageMatching(regexForExpectedException)
              .hasCauseInstanceOf(NotSerializableException.class);
    });
  }

  @Test
  public void queryWithRegionAsBindParameterShouldThrowSecurityExceptionWhenUserDoesNotHaveTheCorrectPrivileges()
      throws Exception {
    setUpSpecificClient("dataReaderRegionKey");
    String regexForExpectedException = ".*values.*";

    specificUserClient.invoke(() -> {
      Region region = getClientCache().getRegion(regionName);
      HashSet<Region> hashSet = new HashSet<>(Collections.singletonList(region));
      assertExceptionOccurred(getClientCache().getQueryService(), new Object[] {hashSet},
          regexForExpectedException);
    });
  }
}
