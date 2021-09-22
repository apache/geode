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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category(OQLQueryTest.class)
@RunWith(GeodeParamsRunner.class)
public class CompiledGroupBySelectIntegrationTest {
  private QueryService queryService;

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule()
      .withRegion(RegionShortcut.LOCAL, "portfolio")
      .withAutoStart();

  @Before
  public void setUp() throws Exception {
    queryService = serverStarterRule.getCache().getQueryService();
  }

  @Test
  public void parsingShouldSucceedForSupportedQueries() {
    List<String> queries = Arrays.asList(
        "SELECT MIN(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, MIN(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT MAX(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, MAX(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT AVG(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, AVG(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, AVG(DISTINCT pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT SUM(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, SUM(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, SUM(DISTINCT pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT COUNT(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, COUNT(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, COUNT(DISTINCT pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT MIN(pf.ID), MAX(pf.ID), AVG(pf.ID), SUM(pf.ID), COUNT(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, MIN(pf.ID), MAX(pf.ID), AVG(pf.ID), SUM(pf.ID), COUNT(pf.ID) FROM "
            + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, MIN(pf.ID), MAX(pf.ID), AVG(DISTINCT pf.ID), SUM(DISTINCT pf.ID), COUNT(DISTINCT pf.ID) FROM "
            + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status"

    );

    for (String queryStr : queries) {
      DefaultQuery query = (DefaultQuery) queryService.newQuery(queryStr);
      CompiledSelect cs = query.getSimpleSelect();
      assertThat(cs)
          .as(String.format("Query parsing failed for %s", queryStr))
          .isInstanceOf(CompiledGroupBySelect.class);
    }
  }

  @Test
  public void parsingShouldSucceedForSupportedQueriesWithNestedQueries() {
    String innerQuery =
        "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + "portfolio iter WHERE iter.ID = pf.ID)";
    List<String> queries = Arrays.asList(
        "SELECT MIN(" + innerQuery + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, MIN(" + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, MIN(" + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT MAX(" + innerQuery + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, MAX(" + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT AVG(" + innerQuery + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, AVG(" + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, AVG(DISTINCT " + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT SUM(" + innerQuery + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, SUM(" + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, SUM(DISTINCT " + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT COUNT(" + innerQuery + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, COUNT(" + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, COUNT(DISTINCT " + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT MIN(pf.ID), MAX(" + innerQuery + "), AVG(" + innerQuery + "), SUM(" + innerQuery
            + "), COUNT(" + innerQuery + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, MIN(" + innerQuery + "), MAX(" + innerQuery + "), AVG(" + innerQuery
            + "), SUM(" + innerQuery + "), COUNT(" + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, MIN(" + innerQuery + "), MAX(" + innerQuery + "), AVG(DISTINCT "
            + innerQuery + "), SUM(DISTINCT " + innerQuery + "), COUNT(DISTINCT " + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status"

    );

    for (String queryStr : queries) {
      DefaultQuery query = (DefaultQuery) queryService.newQuery(queryStr);
      CompiledSelect cs = query.getSimpleSelect();
      assertThat(cs)
          .as(String.format("Query parsing failed for %s", queryStr))
          .isInstanceOf(CompiledGroupBySelect.class);
    }
  }

  @Test
  public void parsingShouldSucceedForSupportedQueriesWithAliases() {
    List<String> queries = Arrays.asList(
        "SELECT MIN(pf.ID) AS mn FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status AS st, MIN(pf.ID) AS mn FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT MAX(pf.ID) AS mx FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status AS st, MAX(pf.ID) AS mx FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT AVG(pf.ID) AS ag FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status as st, AVG(pf.ID) AS ag FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY st",
        "SELECT pf.status as st, AVG(DISTINCT pf.ID) AS ag FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT SUM(pf.ID) AS sm FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status as st, SUM(pf.ID) AS sm FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY st",
        "SELECT pf.status as st, SUM(DISTINCT pf.ID) AS sm FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT COUNT(pf.ID) AS ct FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status as st, COUNT(pf.ID) AS ct FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY st",
        "SELECT pf.status as st, COUNT(DISTINCT pf.ID) AS ct FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT MIN(pf.ID) AS mn, MAX(pf.ID) AS mx, AVG(pf.ID) AS ag, SUM(pf.ID) AS sm, COUNT(pf.ID) FROM "
            + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status as st, MIN(pf.ID) AS mn, MAX(pf.ID) AS mx, AVG(pf.ID) AS ag, SUM(pf.ID) AS sm, COUNT(pf.ID) AS ct FROM "
            + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",
        "SELECT pf.status as st, MIN(pf.ID) AS mn, MAX(pf.ID) AS mx, AVG(DISTINCT pf.ID) AS ag, SUM(DISTINCT pf.ID) AS sm, COUNT(DISTINCT pf.ID) AS ct FROM "
            + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st");

    for (String queryStr : queries) {
      DefaultQuery query = (DefaultQuery) queryService.newQuery(queryStr);
      CompiledSelect cs = query.getSimpleSelect();
      assertThat(cs)
          .as(String.format("Query parsing failed for %s", queryStr))
          .isInstanceOf(CompiledGroupBySelect.class);
    }
  }

  @Test
  public void parsingShouldSucceedForSupportedQueriesWithAliasesAndNestedQueries() {
    String innerQuery =
        "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + "portfolio iter WHERE iter.ID = pf.ID)";
    List<String> queries = Arrays.asList(
        "SELECT MIN(" + innerQuery + ") AS mn FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status AS st, MIN(" + innerQuery
            + ") AS mn FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT MAX(" + innerQuery + ") AS mx FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status AS st, MAX(" + innerQuery
            + ") AS mx FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT AVG(" + innerQuery + ") AS ag FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status as st, AVG(" + innerQuery
            + ") AS ag FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",
        "SELECT pf.status as st, AVG(DISTINCT " + innerQuery
            + ") AS ag FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT SUM(" + innerQuery + ") AS sm FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status as st, SUM(" + innerQuery
            + ") AS sm FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",
        "SELECT pf.status as st, SUM(DISTINCT " + innerQuery
            + ") AS sm FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT COUNT(" + innerQuery + ") AS ct FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status as st, COUNT(" + innerQuery
            + ") AS ct FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",
        "SELECT pf.status as st, COUNT(DISTINCT " + innerQuery
            + ") AS ct FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",

        "SELECT MIN(" + innerQuery + ") AS mn, MAX(" + innerQuery + ") AS mx, AVG(" + innerQuery
            + ") AS ag, SUM(" + innerQuery + ") AS sm, COUNT(" + innerQuery
            + ") FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status as st, MIN(" + innerQuery + ") AS mn, MAX(" + innerQuery + ") AS mx, AVG("
            + innerQuery + ") AS ag, SUM(" + innerQuery + ") AS sm, COUNT(" + innerQuery
            + ") AS ct FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st",
        "SELECT pf.status as st, MIN(" + innerQuery + ") AS mn, MAX(" + innerQuery
            + ") AS mx, AVG(DISTINCT " + innerQuery + ") AS ag, SUM(DISTINCT " + innerQuery
            + ") AS sm, COUNT(DISTINCT " + innerQuery
            + ") AS ct FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY st");

    for (String queryStr : queries) {
      DefaultQuery query = (DefaultQuery) queryService.newQuery(queryStr);
      CompiledSelect cs = query.getSimpleSelect();
      assertThat(cs)
          .as(String.format("Query parsing failed for %s", queryStr))
          .isInstanceOf(CompiledGroupBySelect.class);
    }
  }

  @Test
  public void parsingShouldFailWhenGroupByContainsFieldsNotNotPresentInProjectedFields() {
    assertThatThrownBy(
        () -> queryService
            .newQuery("SELECT * FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.ID"))
                .isInstanceOf(QueryInvalidException.class)
                .hasMessageContaining(
                    "Query contains projected column not present in group by clause");

    assertThatThrownBy(() -> queryService
        .newQuery("SELECT * FROM " + SEPARATOR
            + "portfolio pf, pf.positions pos WHERE pf.ID > 0 GROUP BY pf"))
                .isInstanceOf(QueryInvalidException.class)
                .hasMessageContaining(
                    "Query contains projected column not present in group by clause");
  }

  @Test
  @Parameters({"MIN", "MAX", "AVG", "SUM", "COUNT"})
  public void parsingShouldFailWhenAggregateFunctionArgumentIsNotIncludedWithinGroupByClause(
      String aggregateFunction) {
    assertThatThrownBy(() -> queryService.newQuery(
        "SELECT " + aggregateFunction + "(p.ID), p.shortID FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0"))
                .isInstanceOf(QueryInvalidException.class)
                .hasMessageContaining(
                    "Query contains projected column not present in group by clause");

    assertThatThrownBy(() -> queryService.newQuery("SELECT " + aggregateFunction
        + "(p.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.shortID"))
            .isInstanceOf(QueryInvalidException.class)
            .hasMessageContaining(
                "Query contains group by columns not present in projected fields");
  }
}
