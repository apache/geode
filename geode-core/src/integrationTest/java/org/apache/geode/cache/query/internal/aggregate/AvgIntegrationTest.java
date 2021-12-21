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
package org.apache.geode.cache.query.internal.aggregate;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.query.internal.aggregate.AbstractAggregator.downCast;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category(OQLQueryTest.class)
public class AvgIntegrationTest extends AggregateFunctionQueryBaseIntegrationTest {
  private final Map<String, Number> queries = new HashMap<>();
  private final Map<String, Number> equiJoinQueries = new HashMap<>();

  private void prepareStructures() {
    Supplier<Stream<Portfolio>> supplierOne =
        () -> regionOneLocalCopy.values().stream().map(Portfolio.class::cast);
    Supplier<Stream<Portfolio>> supplierTwo =
        () -> regionTwoLocalCopy.values().stream().map(Portfolio.class::cast);

    for (int i = 1; i <= 70; i++) {
      Portfolio portfolio = new Portfolio(i);
      portfolio.shortID = (short) ((short) i / 5);
      regionOneLocalCopy.put(i, portfolio);
    }

    for (int i = 50; i <= 100; i++) {
      Portfolio portfolio = new Portfolio(i);
      portfolio.shortID = (short) ((short) i / 5);
      regionTwoLocalCopy.put(i, portfolio);
    }

    // Simple Queries
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName + " p",
        downCast(supplierOne.get().mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName + " p WHERE p.ID > 0",
        downCast(supplierOne.get().filter(p -> p.getID() > 0).mapToInt(Portfolio::getID).average()
            .orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName + " p WHERE p.ID > 50",
        downCast(supplierOne.get().filter(p -> p.getID() > 50).mapToInt(Portfolio::getID).average()
            .orElse(Double.NaN)));
    queries.put(
        "SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
            + " p WHERE p.ID > 0 OR p.status='active'",
        downCast(supplierOne.get().filter(p -> p.getID() > 0 || p.isActive())
            .mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    queries.put(
        "SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
            + " p WHERE p.ID > 0 OR p.status LIKE 'ina%'",
        downCast(supplierOne.get().filter(p -> p.getID() > 0 || p.status.startsWith("ina"))
            .mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p WHERE p.ID IN SET(1, 2, 3, 4, 5)",
        downCast(supplierOne.get().filter(p -> Arrays.asList(1, 2, 3, 4, 5).contains(p.getID()))
            .mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName + " p WHERE NOT (p.ID > 5)",
        downCast(supplierOne.get().filter(p -> p.getID() <= 5).mapToInt(Portfolio::getID).average()
            .orElse(Double.NaN)));

    // Distinct Queries
    queries.put(
        "SELECT AVG(DISTINCT p.ID) FROM " + SEPARATOR + firstRegionName + " p WHERE p.ID > 0",
        downCast(
            supplierOne.get().filter(p -> p.getID() > 0).filter(distinctByKey(Portfolio::getID))
                .mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    queries.put(
        "SELECT AVG(DISTINCT p.shortID) FROM " + SEPARATOR + firstRegionName + " p WHERE p.ID > 0",
        downCast(supplierOne.get().filter(p -> p.getID() > 0).filter(distinctByKey(p -> p.shortID))
            .mapToInt(p -> p.shortID).average().orElse(Double.NaN)));

    // StructSet queries.
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p, p.positions.values pos WHERE p.ID > 0 AND pos.secId = 'IBM'",
        downCast(supplierOne.get().filter(p -> p.getID() > 0 && p.getPositions().containsKey("IBM"))
            .mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p, p.positions.values pos WHERE p.ID < 100 AND pos.secId = 'IBM'",
        downCast(
            supplierOne.get().filter(p -> p.getID() < 100 && p.getPositions().containsKey("IBM"))
                .mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    queries.put("SELECT DISTINCT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p, p.positions.values pos WHERE p.ID > 0 AND pos.secId = 'IBM'",
        downCast(supplierOne.get().filter(p -> p.getID() > 0 && p.getPositions().containsKey("IBM"))
            .distinct().mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p, p.positions.values pos WHERE p.ID > 0 AND p.status = 'active' AND pos.secId = 'IBM'",
        downCast(supplierOne.get()
            .filter(p -> p.getID() > 0 && p.isActive() && p.getPositions().containsKey("IBM"))
            .mapToInt(Portfolio::getID).average().orElse(Double.NaN)));

    // Aggregate used as as WHERE condition within inner query.
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p WHERE p.ID IN (SELECT MIN(o.ID) FROM " + SEPARATOR + firstRegionName + " o)",
        downCast(supplierOne.get()
            .filter(p -> p.getID() == supplierOne.get().mapToInt(Portfolio::getID).min().orElse(-1))
            .mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p WHERE p.ID = ELEMENT(SELECT MAX(o.ID) FROM " + SEPARATOR + firstRegionName + " o)",
        downCast(supplierOne.get()
            .filter(p -> p.getID() == supplierOne.get().mapToInt(Portfolio::getID).max().orElse(-1))
            .mapToInt(Portfolio::getID).average().orElse(Double.NaN)));

    // Equi Join Queries
    equiJoinQueries.put("SELECT AVG(p.ID) from " + SEPARATOR + firstRegionName + " p, " + SEPARATOR
        + secondRegionName
        + " e WHERE p.ID = e.ID AND p.ID > 0",
        downCast(supplierOne.get().filter(p -> regionTwoLocalCopy.containsKey(p.getID()))
            .filter(p -> p.getID() > 0).mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    equiJoinQueries.put("SELECT AVG(p.ID) from " + SEPARATOR + firstRegionName + " p, " + SEPARATOR
        + secondRegionName
        + " e WHERE p.ID = e.ID AND p.ID > 20 AND e.ID > 40",
        downCast(supplierOne.get()
            .filter(p -> supplierTwo.get().filter(e -> e.getID() > 40)
                .collect(Collectors.toMap(Portfolio::getID, Function.identity()))
                .containsKey(p.getID()))
            .filter(p -> p.getID() > 20).mapToInt(Portfolio::getID).average().orElse(Double.NaN)));
    equiJoinQueries.put("SELECT AVG(p.ID) from " + SEPARATOR + firstRegionName + " p, " + SEPARATOR
        + secondRegionName
        + " e WHERE p.ID = e.ID AND p.ID > 0 AND p.status = 'active'",
        downCast(supplierOne.get().filter(p -> regionTwoLocalCopy.containsKey(p.getID()))
            .filter(p -> p.getID() > 0 && p.isActive()).mapToInt(Portfolio::getID).average()
            .orElse(Double.NaN)));
  }

  private void prepareStructuresWithPdx() {
    Supplier<Stream<PortfolioPdx>> supplierOne =
        () -> regionOneLocalCopy.values().stream().map(PortfolioPdx.class::cast);
    Supplier<Stream<PortfolioPdx>> supplierTwo =
        () -> regionTwoLocalCopy.values().stream().map(PortfolioPdx.class::cast);

    for (int i = 1; i <= 70; i++) {
      PortfolioPdx portfolio = new PortfolioPdx(i);
      portfolio.shortID = (short) ((short) i / 5);
      regionOneLocalCopy.put(i, portfolio);
    }

    for (int i = 50; i <= 100; i++) {
      PortfolioPdx portfolio = new PortfolioPdx(i);
      portfolio.shortID = (short) ((short) i / 5);
      regionTwoLocalCopy.put(i, portfolio);
    }

    // Simple Queries
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName + " p",
        downCast(supplierOne.get().mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName + " p WHERE p.ID > 0",
        downCast(supplierOne.get().filter(p -> p.getID() > 0).mapToInt(PortfolioPdx::getID)
            .average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName + " p WHERE p.ID > 50",
        downCast(supplierOne.get().filter(p -> p.getID() > 50).mapToInt(PortfolioPdx::getID)
            .average().orElse(Double.NaN)));
    queries.put(
        "SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
            + " p WHERE p.ID > 0 OR p.status='active'",
        downCast(supplierOne.get().filter(p -> p.getID() > 0 || p.isActive())
            .mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));
    queries.put(
        "SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
            + " p WHERE p.ID > 0 OR p.status LIKE 'ina%'",
        downCast(supplierOne.get().filter(p -> p.getID() > 0 || p.status.startsWith("ina"))
            .mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p WHERE p.ID IN SET(1, 2, 3, 4, 5)",
        downCast(supplierOne.get().filter(p -> Arrays.asList(1, 2, 3, 4, 5).contains(p.getID()))
            .mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName + " p WHERE NOT (p.ID > 5)",
        downCast(supplierOne.get().filter(p -> p.getID() <= 5).mapToInt(PortfolioPdx::getID)
            .average().orElse(Double.NaN)));

    // Distinct Queries
    queries.put(
        "SELECT AVG(DISTINCT p.ID) FROM " + SEPARATOR + firstRegionName + " p WHERE p.ID > 0",
        downCast(
            supplierOne.get().filter(p -> p.getID() > 0).filter(distinctByKey(PortfolioPdx::getID))
                .mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));
    queries.put(
        "SELECT AVG(DISTINCT p.shortID) FROM " + SEPARATOR + firstRegionName + " p WHERE p.ID > 0",
        downCast(supplierOne.get().filter(p -> p.getID() > 0).filter(distinctByKey(p -> p.shortID))
            .mapToInt(p -> p.shortID).average().orElse(Double.NaN)));

    // StructSet queries.
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p, p.positions.values pos WHERE p.ID > 0 AND pos.secId = 'IBM'",
        downCast(supplierOne.get().filter(p -> p.getID() > 0 && p.getPositions().containsKey("IBM"))
            .mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p, p.positions.values pos WHERE p.ID < 100 AND pos.secId = 'IBM'",
        downCast(
            supplierOne.get().filter(p -> p.getID() < 100 && p.getPositions().containsKey("IBM"))
                .mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));
    queries.put("SELECT DISTINCT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p, p.positions.values pos WHERE p.ID > 0 AND pos.secId = 'IBM'",
        downCast(supplierOne.get().filter(p -> p.getID() > 0 && p.getPositions().containsKey("IBM"))
            .distinct().mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p, p.positions.values pos WHERE p.ID > 0 AND p.status = 'active' AND pos.secId = 'IBM'",
        downCast(supplierOne.get()
            .filter(p -> p.getID() > 0 && p.isActive() && p.getPositions().containsKey("IBM"))
            .mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));

    // Aggregate used as as WHERE condition within inner query.
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p WHERE p.ID IN (SELECT MIN(o.ID) FROM " + SEPARATOR + firstRegionName + " o)",
        downCast(supplierOne.get()
            .filter(
                p -> p.getID() == supplierOne.get().mapToInt(PortfolioPdx::getID).min().orElse(-1))
            .mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));
    queries.put("SELECT AVG(p.ID) FROM " + SEPARATOR + firstRegionName
        + " p WHERE p.ID = ELEMENT(SELECT MAX(o.ID) FROM " + SEPARATOR + firstRegionName + " o)",
        downCast(supplierOne.get()
            .filter(
                p -> p.getID() == supplierOne.get().mapToInt(PortfolioPdx::getID).max().orElse(-1))
            .mapToInt(PortfolioPdx::getID).average().orElse(Double.NaN)));

    // Equi Join Queries
    equiJoinQueries.put("SELECT AVG(p.ID) from " + SEPARATOR + firstRegionName + " p, " + SEPARATOR
        + secondRegionName
        + " e WHERE p.ID = e.ID AND p.ID > 0",
        downCast(supplierOne.get().filter(p -> regionTwoLocalCopy.containsKey(p.getID()))
            .filter(p -> p.getID() > 0).mapToInt(PortfolioPdx::getID).average()
            .orElse(Double.NaN)));
    equiJoinQueries.put("SELECT AVG(p.ID) from " + SEPARATOR + firstRegionName + " p, " + SEPARATOR
        + secondRegionName
        + " e WHERE p.ID = e.ID AND p.ID > 20 AND e.ID > 40",
        downCast(supplierOne.get()
            .filter(p -> supplierTwo.get().filter(e -> e.getID() > 40)
                .collect(Collectors.toMap(PortfolioPdx::getID, Function.identity()))
                .containsKey(p.getID()))
            .filter(p -> p.getID() > 20).mapToInt(PortfolioPdx::getID).average()
            .orElse(Double.NaN)));
    equiJoinQueries.put("SELECT AVG(p.ID) from " + SEPARATOR + firstRegionName + " p, " + SEPARATOR
        + secondRegionName
        + " e WHERE p.ID = e.ID AND p.ID > 0 AND p.status = 'active'",
        downCast(supplierOne.get().filter(p -> regionTwoLocalCopy.containsKey(p.getID()))
            .filter(p -> p.getID() > 0 && p.isActive()).mapToInt(PortfolioPdx::getID).average()
            .orElse(Double.NaN)));
  }

  public void parameterizedSetUp(boolean usePdx) {
    if (!usePdx) {
      prepareStructures();
    } else {
      prepareStructuresWithPdx();
    }
  }

  @Test
  @Parameters({
      "LOCAL, true", "LOCAL, false", "REPLICATE, true",
      "REPLICATE, false", "PARTITION, true", "PARTITION, false"
  })
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1})")
  public void avgShouldWorkCorrectlyOnDifferentRegionTypes(RegionShortcut regionShortcut,
      boolean usePdx) throws Exception {
    parameterizedSetUp(usePdx);
    createAndPopulateRegion(firstRegionName, regionShortcut, regionOneLocalCopy);
    QueryService queryService = server.getCache().getQueryService();

    for (String queryStr : queries.keySet()) {
      Query query = queryService.newQuery(queryStr);
      SelectResults result = (SelectResults) query.execute();
      assertThat(result.size()).isEqualTo(1);
      assertThat(result.asList().get(0)).isInstanceOf(Number.class);
      assertThat(result.asList().get(0))
          .as(String.format("Query %s didn't return expected results", queryStr))
          .isEqualTo(queries.get(queryStr));
    }
  }

  @Test
  @Parameters({
      "LOCAL, true", "LOCAL, false", "REPLICATE, true",
      "REPLICATE, false", "PARTITION, true", "PARTITION, false"
  })
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1})")
  public void avgShouldWorkCorrectlyOnDifferentRegionTypesWithIndexes(RegionShortcut regionShortcut,
      boolean usePdx) throws Exception {
    parameterizedSetUp(usePdx);
    createRegion(firstRegionName, regionShortcut);
    QueryService queryService = server.getCache().getQueryService();
    queryService.createIndex("sampleIndex-1", "p.ID", SEPARATOR + firstRegionName + " p");
    queryService.createIndex("sampleIndex-2", "p.status", SEPARATOR + firstRegionName + " p");
    queryService.createIndex("sampleIndex-3", "pos.secId",
        SEPARATOR + firstRegionName + " p, p.positions.values pos");
    await().untilAsserted(() -> assertThat(queryService.getIndexes().size()).isEqualTo(3));
    populateRegion(firstRegionName, regionOneLocalCopy);

    for (String queryStr : queries.keySet()) {
      Query query = queryService.newQuery(queryStr);
      SelectResults result = (SelectResults) query.execute();
      assertThat(result.size()).isEqualTo(1);
      assertThat(result.asList().get(0)).isInstanceOf(Number.class);
      assertThat(result.asList().get(0))
          .as(String.format("Query %s didn't return expected results", queryStr))
          .isEqualTo(queries.get(queryStr));
    }
  }

  @Test
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1})")
  @Parameters({"LOCAL, true", "LOCAL, false", "REPLICATE, true", "REPLICATE, false"})
  public void avgWithEquiJoinShouldWorkCorrectlyOnDifferentRegionTypes(
      RegionShortcut regionShortcut, boolean usePdx) throws Exception {
    parameterizedSetUp(usePdx);
    createAndPopulateRegion(firstRegionName, regionShortcut, regionOneLocalCopy);
    createAndPopulateRegion(secondRegionName, regionShortcut, regionTwoLocalCopy);
    QueryService queryService = server.getCache().getQueryService();

    for (String queryStr : equiJoinQueries.keySet()) {
      Query query = queryService.newQuery(queryStr);
      SelectResults result = (SelectResults) query.execute();
      assertThat(result.size()).isEqualTo(1);
      assertThat(result.asList().get(0)).isInstanceOf(Number.class);
      assertThat(result.asList().get(0))
          .as(String.format("Query %s didn't return expected results", queryStr))
          .isEqualTo(equiJoinQueries.get(queryStr));
    }
  }

  @Test
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1})")
  @Parameters({"LOCAL, true", "LOCAL, false", "REPLICATE, true", "REPLICATE, false"})
  public void avgWithEquiJoinShouldWorkCorrectlyOnDifferentRegionTypesWithIndexes(
      RegionShortcut regionShortcut, boolean usePdx) throws Exception {
    parameterizedSetUp(usePdx);
    createRegion(firstRegionName, regionShortcut);
    createRegion(secondRegionName, regionShortcut);
    QueryService queryService = server.getCache().getQueryService();
    queryService.createIndex("sampleIndex-1", "p.ID", SEPARATOR + firstRegionName + " p");
    queryService.createIndex("sampleIndex-2", "p.status", SEPARATOR + firstRegionName + " p");
    queryService.createIndex("sampleIndex-3", "e.ID", SEPARATOR + secondRegionName + " e");
    queryService.createIndex("sampleIndex-4", "e.status", SEPARATOR + secondRegionName + " e");
    await().untilAsserted(() -> assertThat(queryService.getIndexes().size()).isEqualTo(4));
    populateRegion(firstRegionName, regionOneLocalCopy);
    populateRegion(secondRegionName, regionTwoLocalCopy);

    for (String queryStr : equiJoinQueries.keySet()) {
      Query query = queryService.newQuery(queryStr);
      SelectResults result = (SelectResults) query.execute();
      assertThat(result.size()).isEqualTo(1);
      assertThat(result.asList().get(0)).isInstanceOf(Number.class);
      assertThat(result.asList().get(0))
          .as(String.format("Query %s didn't return expected results", queryStr))
          .isEqualTo(equiJoinQueries.get(queryStr));
    }
  }
}
