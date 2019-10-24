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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Category({SecurityTest.class, OQLIndexTest.class})
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class IndexSecurityDistributedTest extends AbstractQuerySecurityDistributedTest {

  @Parameterized.Parameters(name = "User:{0}, RegionType:{1}")
  public static Object[] usersAndRegionTypes() {
    return new Object[][] {
        {"dataWriter", REPLICATE}, {"dataWriter", PARTITION},
    };
  }

  @Parameterized.Parameter
  public String user;

  @Parameterized.Parameter(1)
  public RegionShortcut regionShortcut;

  @Before
  public void setUp() throws Exception {
    super.setUp(user, regionShortcut);

    keys = new Object[] {"key-0", "key-1", "key-2"};
    values = new Object[] {
        new QueryTestObject(1, "Mary"),
        new QueryTestObject(2, "Joe"),
        new QueryTestObject(3, "Joe")
    };
  }

  @Test
  public void indexCreatedOnPublicFieldFollowedByPutWithNoReadCredentialsShouldNotThrowSecurityException() {
    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      queryService.createIndex("IdIndex", "id", "/" + regionName);
    });

    putIntoRegion(specificUserClient, keys, values, regionName);
  }

  @Test
  public void indexCreatedOnPrivateFieldAccessibleThroughAccessorMethodFollowedByPutWithNoReadCredentialsShouldNotThrowSecurityException() {
    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      queryService.createIndex("IdIndex", "e.id", "/" + regionName + ".entries e");
    });

    putIntoRegion(specificUserClient, keys, values, regionName);
  }

  @Test
  public void indexCreationWithMethodInvocationOnPopulatedRegionShouldThrowSecurityException() {
    putIntoRegion(superUserClient, keys, values, regionName);

    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      assertThatThrownBy(
          () -> queryService.createIndex("IdIndex", "e.getName()", "/" + regionName + " e"))
              .isInstanceOf(IndexInvalidException.class)
              .hasMessageContaining("Unauthorized access to method: getName");
    });
  }

  @Test
  public void indexCreationWithMethodInvocationOnEmptyRegionFollowedByPutShouldMarkIndexAsInvalid() {
    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      queryService.createIndex("IdIndex", "e.getName()", "/" + regionName + " e");
    });

    putIntoRegion(superUserClient, keys, values, regionName);

    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Region region = ClusterStartupRule.getCache().getRegion(regionName);
      Index index = queryService.getIndex(region, "IdIndex");
      assertThat(index.isValid()).isFalse();
    });
  }
}
