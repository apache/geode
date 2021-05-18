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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({OQLQueryTest.class})
public class QueryWithIndexesDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(5);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locator, server;

  @Before
  public void setUpServers() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withoutClusterConfigurationService());
    server = cluster.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testQueryExecutionWithMultipleIndexes() {
    gfsh.executeAndAssertThat("create region --name=exampleRegion --type=PARTITION")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "exampleRegion", 1);

    server.invoke(() -> {
      // create index.
      QueryService cacheQS = ClusterStartupRule.getCache().getQueryService();
      cacheQS.createIndex("IdIndex", "value.ID", SEPARATOR + "exampleRegion.entrySet");
      cacheQS.createIndex("StatusIndex ", "value.status", SEPARATOR + "exampleRegion.entrySet");
      populateRegion(0, 500);
    });

    locator.invoke(() -> {
      String query = "query --query=\"<trace> select value from" +
          SEPARATOR + "exampleRegion.entrySet where value.ID >= 0 AND value.ID < 500 " +
          "AND (value.status = 'active' or value.status = 'inactive')\"";

      ManagementService service =
          ManagementService.getExistingManagementService(ClusterStartupRule.getCache());
      MemberMXBean member = service.getMemberMXBean();
      String cmdResult = member.processCommand(query);

      assertThat(cmdResult).contains("\"Rows\":\"100\"");
      assertThat(cmdResult).contains("indexesUsed(2)");
    });
  }

  private static void populateRegion(int startingId, int endingId) {
    Region exampleRegion = ClusterStartupRule.getCache().getRegion("exampleRegion");
    for (int i = startingId; i < endingId; i++) {
      exampleRegion.put("" + i, new Portfolio(i));
    }
  }
}
