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
package org.apache.geode.cache.query.cq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Test to make sure CQs that have invalid syntax throw QueryInvalidException, and CQs that have
 * unsupported CQ features throw UnsupportedOperationException
 */
@Category(ClientSubscriptionTest.class)
public class ContinuousQueryValidationDUnitTest {
  private QueryService queryService;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Before
  public void setUp() {
    MemberVM locator = clusterStartupRule.startLocatorVM(1, new Properties());
    MemberVM cacheServer = clusterStartupRule.startServerVM(2, locator.getPort());
    cacheServer.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region");
    });

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    clientCacheFactory.addPoolLocator("localhost", locator.getPort());
    clientCacheFactory.setPoolSubscriptionEnabled(true);
    ClientCache clientCache = clientCacheFactory.create();
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");

    queryService = clientCache.getQueryService();
  }

  @Test
  public void creationShouldSucceedForValidQueries() {
    CqAttributes attrs = new CqAttributesFactory().create();
    String[] validQueries = new String[] {
        "SELECT * FROM /region WHERE id = 1",
        "SELECT * FROM /region WHERE id <> 1",
        "SELECT * FROM /region WHERE id < 100",
        "SELECT * FROM /region WHERE status = 'active'",
        "SELECT * FROM /region t WHERE t.price > 100.00"
    };

    for (String queryString : validQueries) {
      assertThatCode(() -> queryService.newCq(queryString, attrs))
          .as(String.format("Query creation failed for %s but should have succeeded.", queryString))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void creationShouldThrowQueryInvalidExceptionForInvalidQueries() {
    CqAttributes attrs = new CqAttributesFactory().create();
    String[] invalidQueries = new String[] {
        "I AM NOT A QUERY",
        "SELECT * FROM /region WHERE MIN(id) > 0",
        "SELECT * FROM /region WHERE MAX(id) > 0",
        "SELECT * FROM /region WHERE AVG(id) > 0",
        "SELECT * FROM /region WHERE SUM(id) > 0",
        "SELECT * FROM /region WHERE COUNT(id) > 0",
    };

    for (String queryString : invalidQueries) {
      assertThatThrownBy(() -> queryService.newCq(queryString, attrs))
          .as(String.format("Query creation succeeded for %s but should have failed.", queryString))
          .isInstanceOf(QueryInvalidException.class);
    }
  }

  @Test
  public void creationShouldThrowUnsupportedOperationExceptionForUnsupportedQueries() {
    CqAttributes attrs = new CqAttributesFactory().create();
    String[] unsupportedCQs = new String[] {
        // not "just" a select statement
        "(SELECT * FROM /region WHERE status = 'active').isEmpty",

        // cannot be DISTINCT
        "SELECT DISTINCT * FROM /region WHERE status = 'active'",

        // references more than one region
        "SELECT * FROM /region1 r1, /region2 r2 WHERE r1 = r2",

        // where clause refers to a region
        "SELECT * FROM /region r WHERE r.val = /region.size",

        // more than one iterator in FROM clause
        "SELECT * FROM /portfolios p1, p1.positions p2 WHERE p2.id = 'IBM'",

        // first iterator in FROM clause is not just a region path
        "SELECT * FROM /region.entries e WHERE e.value.id = 23",

        // has projections
        "SELECT id FROM /region WHERE status = 'active'",

        // has ORDER BY
        "SELECT * FROM /region WHERE status = 'active' ORDER BY id",

        // has aggregates
        "SELECT MIN(id) FROM /region WHERE status = 'active'",
        "SELECT MAX(id) FROM /region WHERE status = 'active'",
        "SELECT SUM(id) FROM /region WHERE status = 'active'",
        "SELECT AVG(id) FROM /region WHERE status = 'active'",
        "SELECT COUNT(id) FROM /region WHERE status = 'active'",
    };

    for (String queryString : unsupportedCQs) {
      assertThatThrownBy(() -> queryService.newCq(queryString, attrs))
          .as(String.format("Query creation succeeded for %s but should have failed.", queryString))
          .isInstanceOf(UnsupportedOperationException.class);
    }
  }
}
