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
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.data.PositionPdx;
import org.apache.geode.cache.query.internal.CompiledSelect;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
@FixMethodOrder(NAME_ASCENDING)
public class AggregateFunctionsQueryDUnitTest implements Serializable {
  private static final String regionName = "portfolio";
  private static MemberVM locator, server1, server2, server3, server4;

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());
    server3 = cluster.startServerVM(3, locator.getPort());
    server4 = cluster.startServerVM(4, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  private void createRegion(String regionType) {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=" + regionType)
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 4);
  }

  private void createIndexes() {
    gfsh.executeAndAssertThat(
        "define index --name=shortIndex --expression=shortID --region=" + regionName)
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "define index --name=statusIndex --expression=status --region=" + regionName)
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "define index --name=idIndex --expression=ID --type=key --region=" + regionName)
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "define index --name=secIdIndex --expression=\"pos.secId\" --region=\"" + SEPARATOR
            + regionName
            + " p, p.positions.values pos\"")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create defined indexes").statusIsSuccess();

    // Wait Until Indexes are Fully Created.
    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      Cache memberCache = ClusterStartupRule.getCache();
      await().untilAsserted(
          () -> assertThat(memberCache.getQueryService().getIndexes().size()).isEqualTo(4));
    }, server1, server2, server3, server4);
  }

  private void populateRegion(boolean usePdx, int entriesAmount) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    Region<Integer, Object> portfolioRegion = ClusterStartupRule.getCache().getRegion(regionName);
    assertThat(portfolioRegion).isNotNull();

    for (int i = 1; i < entriesAmount + 1; ++i) {
      Object value;

      if (usePdx) {
        PortfolioPdx pfPdx = new PortfolioPdx(i);
        pfPdx.shortID = (short) ((short) i / 5);
        value = pfPdx;
      } else {
        Portfolio pf = new Portfolio(i);
        pf.shortID = (short) ((short) i / 5);
        value = pf;
      }

      portfolioRegion.put(i, value);
    }

    // Wait Until Region is Fully Populated.
    await().untilAsserted(() -> assertThat(portfolioRegion.size()).isEqualTo(entriesAmount));
  }

  private Predicate<Object> hasStatus(boolean usePdx, boolean status) {
    return object -> {
      if (!usePdx) {
        return (status == ((Portfolio) object).isActive());
      } else {
        return (status == ((PortfolioPdx) object).isActive());
      }
    };
  }

  private ToIntFunction<Object> toPortfolioID(boolean usePdx) {
    return value -> {
      if (!usePdx) {
        return ((Portfolio) value).getID();
      } else {
        return ((PortfolioPdx) value).getID();
      }
    };
  }

  private ToIntFunction<Object> toPortfolioShortID(boolean usePdx) {
    return value -> {
      if (!usePdx) {
        return ((Portfolio) value).shortID;
      } else {
        return ((PortfolioPdx) value).shortID;
      }
    };
  }

  private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Set<Object> seen = ConcurrentHashMap.newKeySet();
    return t -> seen.add(keyExtractor.apply(t));
  }

  private long countByStatus(boolean status, boolean usePdx) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);

    return region.values()
        .stream()
        .filter(hasStatus(usePdx, status))
        .count();
  }

  private long countDistinctShortIDsByStatus(boolean status, boolean usePdx) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);

    return region.values()
        .stream()
        .filter(hasStatus(usePdx, status))
        .filter(distinctByKey(value -> {
          if (!usePdx) {
            return ((Portfolio) value).shortID;
          } else {
            return ((PortfolioPdx) value).shortID;
          }
        }))
        .count();
  }

  private double sumIDsByStatus(boolean status, boolean usePdx) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);

    return region.values()
        .stream()
        .filter(hasStatus(usePdx, status))
        .mapToInt(toPortfolioID(usePdx))
        .sum();
  }

  private double sumDistinctShortIDsByStatus(boolean status, boolean usePdx) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);

    return region.values()
        .stream()
        .filter(hasStatus(usePdx, status))
        .filter(distinctByKey(value -> {
          if (!usePdx) {
            return ((Portfolio) value).shortID;
          } else {
            return ((PortfolioPdx) value).shortID;
          }
        }))
        .mapToInt(toPortfolioShortID(usePdx))
        .sum();
  }

  private int maxIDByStatus(boolean status, boolean usePdx) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);

    return region.values()
        .stream()
        .filter(hasStatus(usePdx, status))
        .mapToInt(toPortfolioID(usePdx))
        .max().orElseThrow(IllegalArgumentException::new);
  }

  private int minIDByStatus(boolean status, boolean usePdx) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);

    return region.values()
        .stream()
        .filter(hasStatus(usePdx, status))
        .mapToInt(toPortfolioID(usePdx))
        .min().orElseThrow(IllegalArgumentException::new);
  }

  @SuppressWarnings("unused")
  private Object[] regionTypeAndOneBoolean() {
    // Pdx, Alias , Nested Queries
    return new Object[] {
        new Object[] {"REPLICATE", true},
        new Object[] {"REPLICATE", false},

        new Object[] {"PARTITION", true},
        new Object[] {"PARTITION", false},
    };
  }

  @SuppressWarnings("unused")
  private Object[] regionTypeAndTwoBooleans() {
    // Pdx, Alias , Nested Queries
    return new Object[] {
        new Object[] {"REPLICATE", true, true},
        new Object[] {"REPLICATE", true, false},
        new Object[] {"REPLICATE", false, true},
        new Object[] {"REPLICATE", false, false},

        new Object[] {"PARTITION", true, true},
        new Object[] {"PARTITION", true, false},
        new Object[] {"PARTITION", false, true},
        new Object[] {"PARTITION", false, false},
    };
  }

  @SuppressWarnings("unused")
  private Object[] regionTypeAndThreeBooleans() {
    // Pdx, Alias , Nested Queries
    return new Object[] {
        new Object[] {"REPLICATE", true, true, true},
        new Object[] {"REPLICATE", true, true, false},
        new Object[] {"REPLICATE", true, false, true},
        new Object[] {"REPLICATE", true, false, false},
        new Object[] {"REPLICATE", false, true, true},
        new Object[] {"REPLICATE", false, true, false},
        new Object[] {"REPLICATE", false, false, true},
        new Object[] {"REPLICATE", false, false, false},

        new Object[] {"PARTITION", true, true, true},
        new Object[] {"PARTITION", true, true, false},
        new Object[] {"PARTITION", true, false, true},
        new Object[] {"PARTITION", true, false, false},
        new Object[] {"PARTITION", false, true, true},
        new Object[] {"PARTITION", false, true, false},
        new Object[] {"PARTITION", false, false, true},
        new Object[] {"PARTITION", false, false, false},
    };
  }

  @Test
  @Parameters(method = "regionTypeAndTwoBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2})")
  public void groupBySingleFieldShouldBeTransformableToDistinctOrderByAndReturnCorrectly(
      String regionType, boolean usePdx, boolean useAlias) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 200), server1, server2, server3,
        server4);
    String queryString;

    if (!useAlias) {
      queryString =
          "SELECT p.shortID FROM " + SEPARATOR + regionName
              + " p WHERE p.ID >= 0 GROUP BY p.shortID ";
    } else {
      queryString = "SELECT p.shortID AS short_id FROM " + SEPARATOR + regionName
          + " p WHERE p.ID >= 0 GROUP BY short_id ";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      CompiledSelect cs = ((DefaultQuery) query).getSelect();
      assertThat(cs.isDistinct()).isTrue();
      assertThat(cs.isOrderBy()).isTrue();
      assertThat(cs.isGroupBy()).isFalse();

      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
      Map<Short, List<Object>> temp = region.values().stream().collect(
          Collectors.groupingBy(value -> (short) toPortfolioShortID(usePdx).applyAsInt(value)));
      Set<Short> expectedResults = temp.keySet();

      @SuppressWarnings("unchecked")
      SelectResults<Object> results = (SelectResults<Object>) query.execute();
      assertThat(results.asList().size()).isEqualTo(expectedResults.size());

      for (Object result : results.asList()) {
        if (!useAlias) {
          assertThat(result).isInstanceOf(Short.class);
          assertThat(expectedResults.contains(result)).isTrue();
        } else {
          Struct struct = (Struct) result;
          assertThat(struct.get("short_id")).isInstanceOf(Short.class);
          assertThat(expectedResults.contains(struct.get("short_id"))).isTrue();
        }
      }
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndOneBoolean")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1})")
  public void groupByMultipleFieldsShouldBeTransformableToDistinctOrderByAndReturnCorrectly(
      String regionType, boolean usePdx) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 150), server1, server2, server3,
        server4);
    String queryString = "SELECT p.status AS status, p.ID FROM " + SEPARATOR + regionName
        + " p WHERE p.ID > 0 GROUP BY status, p.ID ";

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      CompiledSelect cs = ((DefaultQuery) query).getSelect();
      assertThat(cs.isDistinct()).isTrue();
      assertThat(cs.isOrderBy()).isTrue();
      assertThat(cs.isGroupBy()).isFalse();

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(150);
      for (Struct struct : results.asList()) {
        Integer id = (Integer) struct.get("ID");
        assertThat(((String) struct.get("status"))).isEqualTo(id % 2 == 0 ? "active" : "inactive");
      }
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void countStartWithGroupByShouldWorkCorrectly(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 200), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT * FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)" : "*";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, COUNT(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY p.status";
    } else {
      queryString =
          "SELECT p.status AS st, COUNT(" + expression + ") AS ct FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeCount = countByStatus(true, usePdx);
      double inactiveCount = countByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(downCast(activeCount));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(downCast(inactiveCount));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void countStartWithGroupByShouldWorkCorrectlyWithIndexes(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 200), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT * FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)" : "*";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, COUNT(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY p.status";
    } else {
      queryString =
          "SELECT p.status AS st, COUNT(" + expression + ") AS ct FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeCount = countByStatus(true, usePdx);
      double inactiveCount = countByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(downCast(activeCount));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(downCast(inactiveCount));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void countDistinctWithGroupByShouldWorkCorrectly(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 200), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.shortID FROM " + SEPARATOR + regionName
            + " iter WHERE iter.ID = p.ID)"
        : "p.shortID";
    String queryString;

    if (!useAlias) {
      queryString =
          "SELECT p.status, COUNT(DISTINCT " + expression + ") FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, COUNT(DISTINCT " + expression + ") AS ct FROM " + SEPARATOR
              + regionName + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeCount = countDistinctShortIDsByStatus(true, usePdx);
      double inactiveCount = countDistinctShortIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(downCast(activeCount));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(downCast(inactiveCount));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void countDistinctWithGroupByShouldWorkCorrectlyWithIndexes(String regionType,
      boolean usePdx, boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 200), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.shortID FROM " + SEPARATOR + regionName
            + " iter WHERE iter.ID = p.ID)"
        : "p.shortID";
    String queryString;

    if (!useAlias) {
      queryString =
          "SELECT p.status, COUNT(DISTINCT " + expression + ") FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, COUNT(DISTINCT " + expression + ") AS ct FROM " + SEPARATOR
              + regionName + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeCount = countDistinctShortIDsByStatus(true, usePdx);
      double inactiveCount = countDistinctShortIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(downCast(activeCount));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(downCast(inactiveCount));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndTwoBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2})")
  public void countWithGroupByShouldBeUsableWithinOrderByAndWorkCorrectly(String regionType,
      boolean usePdx, boolean useAlias) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 600), server1, server2, server3,
        server4);
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.shortID, COUNT(*) FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY p.shortID ORDER BY COUNT(*) DESC";
    } else {
      queryString = "SELECT p.shortID AS shid, COUNT(*) AS ct FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY shid ORDER BY ct DESC";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();

      // Compute Expected Results
      List<Object[]> expectedOrderedResults = new ArrayList<>();
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
      Map<Short, Long> temp = region.values().stream().collect(Collectors.groupingBy(
          value -> (short) toPortfolioShortID(usePdx).applyAsInt(value), Collectors.counting()));
      temp.entrySet().stream().sorted(Map.Entry.<Short, Long>comparingByValue().reversed())
          .forEachOrdered(e -> expectedOrderedResults
              .add(new Object[] {e.getKey(), e.getValue().doubleValue()}));

      // Execute Query
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(expectedOrderedResults.size());
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      // Assertions
      List<Struct> queryResults = results.asList();
      for (int i = 0; i < queryResults.size(); i++) {
        Struct structResult = queryResults.get(i);
        Object[] expectedResult = expectedOrderedResults.get(i);
        assertThat(structResult.getFieldValues()[0]).isEqualTo(expectedResult[0]);
        assertThat(structResult.getFieldValues()[1])
            .isEqualTo(downCast((double) expectedResult[1]));
      }
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndTwoBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2})")
  public void countWithGroupByShouldBeUsableWithinOrderByAndWorkCorrectlyWithIndexes(
      String regionType, boolean usePdx, boolean useAlias) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 600), server1, server2, server3,
        server4);
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.shortID, COUNT(*) FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY p.shortID ORDER BY COUNT(*) DESC";
    } else {
      queryString = "SELECT p.shortID AS shid, COUNT(*) AS ct FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY shid ORDER BY ct DESC";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();

      // Compute Expected Results
      List<Object[]> expectedOrderedResults = new ArrayList<>();
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
      Map<Short, Long> temp = region.values().stream().collect(Collectors.groupingBy(
          value -> (short) toPortfolioShortID(usePdx).applyAsInt(value), Collectors.counting()));
      temp.entrySet().stream().sorted(Map.Entry.<Short, Long>comparingByValue().reversed())
          .forEachOrdered(e -> expectedOrderedResults
              .add(new Object[] {e.getKey(), e.getValue().doubleValue()}));

      // Execute Query
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(expectedOrderedResults.size());
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      // Assertions
      List<Struct> queryResults = results.asList();
      for (int i = 0; i < queryResults.size(); i++) {
        Struct structResult = queryResults.get(i);
        Object[] expectedResult = expectedOrderedResults.get(i);
        assertThat(structResult.getFieldValues()[0]).isEqualTo(expectedResult[0]);
        assertThat(structResult.getFieldValues()[1])
            .isEqualTo(downCast((double) expectedResult[1]));
      }
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void sumWithGroupByShouldWorkCorrectly(String regionType, boolean usePdx, boolean useAlias,
      boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 600), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, SUM(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, SUM(" + expression + ") AS sm FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeSum = sumIDsByStatus(true, usePdx);
      double inactiveSum = sumIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(downCast(activeSum));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(downCast(inactiveSum));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void sumWithGroupByShouldWorkCorrectlyWithIndexes(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 600), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, SUM(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, SUM(" + expression + ") AS sm FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeSum = sumIDsByStatus(true, usePdx);
      double inactiveSum = sumIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(downCast(activeSum));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(downCast(inactiveSum));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void sumDistinctWithGroupByShouldWorkCorrectly(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 600), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.shortID FROM " + SEPARATOR + regionName
            + " iter WHERE iter.ID = p.ID)"
        : "p.shortID";
    String queryString;

    if (!useAlias) {
      queryString =
          "SELECT p.status, SUM(DISTINCT " + expression + ") FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, SUM(DISTINCT " + expression + ") AS sm FROM " + SEPARATOR
              + regionName + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeDistinctShortIDsSum = sumDistinctShortIDsByStatus(true, usePdx);
      double inactiveDistinctShortIDsSum = sumDistinctShortIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1])
          .isEqualTo(downCast(activeDistinctShortIDsSum));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1])
          .isEqualTo(downCast(inactiveDistinctShortIDsSum));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void sumDistinctWithGroupByShouldWorkCorrectlyWithIndexes(String regionType,
      boolean usePdx, boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 600), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.shortID FROM " + SEPARATOR + regionName
            + " iter WHERE iter.ID = p.ID)"
        : "p.shortID";
    String queryString;

    if (!useAlias) {
      queryString =
          "SELECT p.status, SUM(DISTINCT " + expression + ") FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, SUM(DISTINCT " + expression + ") AS sm FROM " + SEPARATOR
              + regionName + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeDistinctShortIDsSum = sumDistinctShortIDsByStatus(true, usePdx);
      double inactiveDistinctShortIDsSum = sumDistinctShortIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1])
          .isEqualTo(downCast(activeDistinctShortIDsSum));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1])
          .isEqualTo(downCast(inactiveDistinctShortIDsSum));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndTwoBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2})")
  public void sumWithGroupByShouldBeUsableWithinOrderByAndWorkCorrectly(String regionType,
      boolean usePdx, boolean useAlias) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.shortID, SUM(p.ID) FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY p.shortID ORDER BY SUM(p.ID) ASC";
    } else {
      queryString = "SELECT p.shortID AS shid, SUM(p.ID) AS sm FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY shid ORDER BY sm ASC";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();

      // Compute Expected Results
      List<Object[]> expectedOrderedResults = new ArrayList<>();
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
      Map<Short, Integer> temp = region.values().stream().collect(
          Collectors.groupingBy(value -> (short) toPortfolioShortID(usePdx).applyAsInt(value),
              Collectors.summingInt(toPortfolioID(usePdx))));
      temp.entrySet().stream().sorted(Map.Entry.comparingByValue())
          .forEachOrdered(e -> expectedOrderedResults.add(new Object[] {e.getKey(), e.getValue()}));

      // Execute Query
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(expectedOrderedResults.size());
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      // Assertions
      List<Struct> queryResults = results.asList();
      for (int i = 0; i < queryResults.size(); i++) {
        Struct structResult = queryResults.get(i);
        Object[] expectedResult = expectedOrderedResults.get(i);
        assertThat(structResult.getFieldValues()[0]).isEqualTo(expectedResult[0]);
        assertThat(structResult.getFieldValues()[1]).isEqualTo(expectedResult[1]);
      }
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndTwoBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2})")
  public void sumWithGroupByShouldBeUsableWithinOrderByAndWorkCorrectlyWithIndexes(
      String regionType, boolean usePdx, boolean useAlias) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.shortID, SUM(p.ID) FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY p.shortID ORDER BY SUM(p.ID) ASC";
    } else {
      queryString = "SELECT p.shortID AS shid, SUM(p.ID) AS sm FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY shid ORDER BY sm ASC";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();

      // Compute Expected Results
      List<Object[]> expectedOrderedResults = new ArrayList<>();
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
      Map<Short, Integer> temp = region.values().stream().collect(
          Collectors.groupingBy(value -> (short) toPortfolioShortID(usePdx).applyAsInt(value),
              Collectors.summingInt(toPortfolioID(usePdx))));
      temp.entrySet().stream().sorted(Map.Entry.comparingByValue())
          .forEachOrdered(e -> expectedOrderedResults.add(new Object[] {e.getKey(), e.getValue()}));

      // Execute Query
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(expectedOrderedResults.size());
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      // Assertions
      List<Struct> queryResults = results.asList();
      for (int i = 0; i < queryResults.size(); i++) {
        Struct structResult = queryResults.get(i);
        Object[] expectedResult = expectedOrderedResults.get(i);
        assertThat(structResult.getFieldValues()[0]).isEqualTo(expectedResult[0]);
        assertThat(structResult.getFieldValues()[1]).isEqualTo(expectedResult[1]);
      }
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void avgWithGroupByShouldWorkCorrectly(String regionType, boolean usePdx, boolean useAlias,
      boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, AVG(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, AVG(" + expression + ") AS av FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeAvg = sumIDsByStatus(true, usePdx) / countByStatus(true, usePdx);
      double inactiveAvg = sumIDsByStatus(false, usePdx) / countByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(downCast(activeAvg));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(downCast(inactiveAvg));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void avgWithGroupByShouldWorkCorrectlyWithIndexes(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, AVG(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, AVG(" + expression + ") AS av FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeAvg = sumIDsByStatus(true, usePdx) / countByStatus(true, usePdx);
      double inactiveAvg = sumIDsByStatus(false, usePdx) / countByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(downCast(activeAvg));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(downCast(inactiveAvg));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void avgDistinctWithGroupByShouldWorkCorrectly(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.shortID FROM " + SEPARATOR + regionName
            + " iter WHERE iter.ID = p.ID)"
        : "p.shortID";
    String queryString;

    if (!useAlias) {
      queryString =
          "SELECT p.status, AVG(DISTINCT " + expression + ") FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, AVG(DISTINCT p.shortID) AS av FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeDistinctShortIDsAvg =
          sumDistinctShortIDsByStatus(true, usePdx) / countDistinctShortIDsByStatus(true, usePdx);
      double inactiveDistinctShortIDsAvg =
          sumDistinctShortIDsByStatus(false, usePdx) / countDistinctShortIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1])
          .isEqualTo(downCast(activeDistinctShortIDsAvg));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1])
          .isEqualTo(downCast(inactiveDistinctShortIDsAvg));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void avgDistinctWithGroupByShouldWorkCorrectlyWithIndexes(String regionType,
      boolean usePdx, boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.shortID FROM " + SEPARATOR + regionName
            + " iter WHERE iter.ID = p.ID)"
        : "p.shortID";
    String queryString;

    if (!useAlias) {
      queryString =
          "SELECT p.status, AVG(DISTINCT " + expression + ") FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, AVG(DISTINCT p.shortID) AS av FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeDistinctShortIDsAvg =
          sumDistinctShortIDsByStatus(true, usePdx) / countDistinctShortIDsByStatus(true, usePdx);
      double inactiveDistinctShortIDsAvg =
          sumDistinctShortIDsByStatus(false, usePdx) / countDistinctShortIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1])
          .isEqualTo(downCast(activeDistinctShortIDsAvg));
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1])
          .isEqualTo(downCast(inactiveDistinctShortIDsAvg));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndTwoBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2})")
  public void avgWithGroupByShouldBeUsableWithinOrderByAndWorkCorrectly(String regionType,
      boolean usePdx, boolean useAlias) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.shortID, AVG(p.ID) FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY p.shortID ORDER BY AVG(p.ID) DESC";
    } else {
      queryString = "SELECT p.shortID AS shid, AVG(p.ID) AS ag FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY shid ORDER BY ag DESC";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();

      // Compute Expected Results
      List<Object[]> expectedOrderedResults = new ArrayList<>();
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
      Map<Short, Double> temp = region.values().stream().collect(
          Collectors.groupingBy(value -> (short) toPortfolioShortID(usePdx).applyAsInt(value),
              Collectors.averagingInt(toPortfolioID(usePdx))));
      temp.entrySet().stream().sorted(Map.Entry.<Short, Double>comparingByValue().reversed())
          .forEachOrdered(e -> expectedOrderedResults.add(new Object[] {e.getKey(), e.getValue()}));

      // Execute Query
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(expectedOrderedResults.size());
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      // Assertions
      List<Struct> queryResults = results.asList();
      for (int i = 0; i < queryResults.size(); i++) {
        Struct structResult = queryResults.get(i);
        Object[] expectedResult = expectedOrderedResults.get(i);
        assertThat(structResult.getFieldValues()[0]).isEqualTo(expectedResult[0]);
        assertThat(structResult.getFieldValues()[1])
            .isEqualTo(downCast((double) expectedResult[1]));
      }
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndTwoBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2})")
  public void avgWithGroupByShouldBeUsableWithinOrderByAndWorkCorrectlyWithIndexes(
      String regionType, boolean usePdx, boolean useAlias) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.shortID, AVG(p.ID) FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY p.shortID ORDER BY AVG(p.ID) DESC";
    } else {
      queryString = "SELECT p.shortID AS shid, AVG(p.ID) AS ag FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY shid ORDER BY ag DESC";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();

      // Compute Expected Results
      List<Object[]> expectedOrderedResults = new ArrayList<>();
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
      Map<Short, Double> temp = region.values().stream().collect(
          Collectors.groupingBy(value -> (short) toPortfolioShortID(usePdx).applyAsInt(value),
              Collectors.averagingInt(toPortfolioID(usePdx))));
      temp.entrySet().stream().sorted(Map.Entry.<Short, Double>comparingByValue().reversed())
          .forEachOrdered(e -> expectedOrderedResults.add(new Object[] {e.getKey(), e.getValue()}));

      // Execute Query
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(expectedOrderedResults.size());
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      // Assertions
      List<Struct> queryResults = results.asList();
      for (int i = 0; i < queryResults.size(); i++) {
        Struct structResult = queryResults.get(i);
        Object[] expectedResult = expectedOrderedResults.get(i);
        assertThat(structResult.getFieldValues()[0]).isEqualTo(expectedResult[0]);
        assertThat(structResult.getFieldValues()[1])
            .isEqualTo(downCast((double) expectedResult[1]));
      }
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void maxWithGroupByShouldWorkCorrectly(String regionType, boolean usePdx, boolean useAlias,
      boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, MAX(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, MAX(" + expression + ") as mx FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      int maxActiveID = maxIDByStatus(true, usePdx);
      int maxInactiveID = maxIDByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(maxActiveID);
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(maxInactiveID);
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void maxWithGroupByShouldWorkCorrectlyWithIndexes(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, MAX(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, MAX(" + expression + ") as mx FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      int maxActiveID = maxIDByStatus(true, usePdx);
      int maxInactiveID = maxIDByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(maxActiveID);
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(maxInactiveID);
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void minWithGroupByShouldWorkCorrectly(String regionType, boolean usePdx, boolean useAlias,
      boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, MIN(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, MIN(" + expression + ") as mx FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      int minActiveID = minIDByStatus(true, usePdx);
      int minInactiveID = minIDByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(minActiveID);
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(minInactiveID);
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void minWithGroupByShouldWorkCorrectlyWithIndexes(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, MIN(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString =
          "SELECT p.status AS st, MIN(" + expression + ") as mx FROM " + SEPARATOR + regionName
              + " p WHERE p.ID > 0 GROUP BY st";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      int minActiveID = minIDByStatus(true, usePdx);
      int minInactiveID = minIDByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isEqualTo(minActiveID);
      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo(minInactiveID);
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void groupByMultipleFieldsWithAggregateFunctionsShouldWorkCorrectly(String regionType,
      boolean usePdx, boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, p.description, SUM(" + expression + "), COUNT(" + expression
          + "), AVG(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status, description";
    } else {
      queryString = "SELECT p.status AS st, p.description as ds, SUM(" + expression
          + ") as sm, COUNT(" + expression + ") as ag, AVG(" + expression + ") as ct FROM "
          + SEPARATOR
          + regionName + " p WHERE p.ID > 0 GROUP BY st, ds";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeCount = countByStatus(true, usePdx);
      double inactiveCount = countByStatus(false, usePdx);
      double activeSum = sumIDsByStatus(true, usePdx);
      double inactiveSum = sumIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isNull();
      assertThat(activeStruct.get().getFieldValues()[2]).isEqualTo(downCast(activeSum));
      assertThat(activeStruct.get().getFieldValues()[3]).isEqualTo(downCast(activeCount));
      assertThat(activeStruct.get().getFieldValues()[4])
          .isEqualTo(downCast(activeSum / activeCount));

      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo("XXXX");
      assertThat(inactiveStruct.get().getFieldValues()[2]).isEqualTo(downCast(inactiveSum));
      assertThat(inactiveStruct.get().getFieldValues()[3]).isEqualTo(downCast(inactiveCount));
      assertThat(inactiveStruct.get().getFieldValues()[4])
          .isEqualTo(downCast(inactiveSum / inactiveCount));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void groupByMultipleFieldsWithAggregateFunctionsShouldWorkCorrectlyWithIndexes(
      String regionType, boolean usePdx, boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT p.status, p.description, SUM(" + expression + "), COUNT(" + expression
          + "), AVG(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 GROUP BY status, description";
    } else {
      queryString = "SELECT p.status AS st, p.description as ds, SUM(" + expression
          + ") as sm, COUNT(" + expression + ") as ag, AVG(" + expression + ") as ct FROM "
          + SEPARATOR
          + regionName + " p WHERE p.ID > 0 GROUP BY st, ds";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeCount = countByStatus(true, usePdx);
      double inactiveCount = countByStatus(false, usePdx);
      double activeSum = sumIDsByStatus(true, usePdx);
      double inactiveSum = sumIDsByStatus(false, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Optional<Struct> activeStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("active")).findAny();
      assertThat(activeStruct.isPresent()).isTrue();
      assertThat(activeStruct.get().getFieldValues()[1]).isNull();
      assertThat(activeStruct.get().getFieldValues()[2]).isEqualTo(downCast(activeSum));
      assertThat(activeStruct.get().getFieldValues()[3]).isEqualTo(downCast(activeCount));
      assertThat(activeStruct.get().getFieldValues()[4])
          .isEqualTo(downCast(activeSum / activeCount));

      Optional<Struct> inactiveStruct = results.asList().stream()
          .filter(struct -> struct.getFieldValues()[0].equals("inactive")).findAny();
      assertThat(inactiveStruct.isPresent()).isTrue();
      assertThat(inactiveStruct.get().getFieldValues()[1]).isEqualTo("XXXX");
      assertThat(inactiveStruct.get().getFieldValues()[2]).isEqualTo(downCast(inactiveSum));
      assertThat(inactiveStruct.get().getFieldValues()[3]).isEqualTo(downCast(inactiveCount));
      assertThat(inactiveStruct.get().getFieldValues()[4])
          .isEqualTo(downCast(inactiveSum / inactiveCount));
    }, server1, server2, server3, server4);
  }

  @Test
  @SuppressWarnings("unchecked")
  @Parameters(method = "regionTypeAndTwoBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2})")
  public void groupByWithMultipleAggregateFunctionAndsLimitClauseShouldWorkCorrectly(
      String regionType, boolean usePdx, boolean useAlias) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 200), server1, server2, server3,
        server4);
    String queryString;

    if (!useAlias) {
      queryString =
          "SELECT pos.secId, COUNT(pos.mktValue), MAX(pos.mktValue), MIN(pos.mktValue), SUM(pos.mktValue), AVG(pos.mktValue) FROM "
              + SEPARATOR
              + regionName + " p, p.positions.values pos GROUP BY pos.secId LIMIT 7";
    } else {
      queryString =
          "SELECT pos.secId AS posSec, COUNT(pos.mktValue) as ct, MAX(pos.mktValue) as mx, MIN(pos.mktValue) as mn, SUM(pos.mktValue) as sm, AVG(pos.mktValue) as ag FROM "
              + SEPARATOR
              + regionName + " p, p.positions.values pos GROUP BY posSec LIMIT 7";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);

      // Pre-Populate Map of Expected Values
      Map<String, List<Double>> mktValuesPerSecId = new HashMap<>();
      Arrays.stream(Portfolio.secIds)
          .forEach(secId -> mktValuesPerSecId.put(secId, new ArrayList<>()));
      region.values().forEach(value -> {
        if (!usePdx) {
          Portfolio portfolio = ((Portfolio) value);
          ((Map<String, Position>) portfolio.positions).values()
              .forEach(position -> mktValuesPerSecId.get(position.secId).add(position.mktValue));
        } else {
          PortfolioPdx portfolioPdx = ((PortfolioPdx) value);
          ((Map<String, PositionPdx>) portfolioPdx.positions).values().forEach(
              position -> mktValuesPerSecId.get(position.secId).add(position.getMktValue()));
        }
      });

      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(7);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      results.forEach(struct -> {
        String posSecId = (String) struct.getFieldValues()[0];
        assertThat(mktValuesPerSecId.containsKey(posSecId)).isTrue();
        List<Double> valuesPerSecId = mktValuesPerSecId.get(posSecId);

        assertThat(struct.getFieldValues()[1]).isEqualTo(valuesPerSecId.size());
        assertThat(struct.getFieldValues()[2]).isEqualTo(Collections.max(valuesPerSecId));
        assertThat(struct.getFieldValues()[3]).isEqualTo(Collections.min(valuesPerSecId));
        assertThat(struct.getFieldValues()[4])
            .isEqualTo(downCast(valuesPerSecId.stream().mapToDouble(Double::doubleValue).sum()));
        assertThat(struct.getFieldValues()[5]).isEqualTo(downCast(
            valuesPerSecId.stream().mapToDouble(Double::doubleValue).average().orElse(0d)));
      });
    }, server1, server2, server3, server4);
  }

  @Test
  @SuppressWarnings("unchecked")
  @Parameters(method = "regionTypeAndTwoBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2})")
  public void groupByWithMultipleAggregateFunctionAndsLimitClauseShouldWorkCorrectlyWithIndexes(
      String regionType, boolean usePdx, boolean useAlias) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 200), server1, server2, server3,
        server4);
    String queryString;

    if (!useAlias) {
      queryString =
          "SELECT pos.secId, COUNT(pos.mktValue), MAX(pos.mktValue), MIN(pos.mktValue), SUM(pos.mktValue), AVG(pos.mktValue) FROM "
              + SEPARATOR
              + regionName + " p, p.positions.values pos GROUP BY pos.secId LIMIT 7";
    } else {
      queryString =
          "SELECT pos.secId AS posSec, COUNT(pos.mktValue) as ct, MAX(pos.mktValue) as mx, MIN(pos.mktValue) as mn, SUM(pos.mktValue) as sm, AVG(pos.mktValue) as ag FROM "
              + SEPARATOR
              + regionName + " p, p.positions.values pos GROUP BY posSec LIMIT 7";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);

      // Pre-Populate Map of Expected Values
      Map<String, List<Double>> mktValuesPerSecId = new HashMap<>();
      Arrays.stream(Portfolio.secIds)
          .forEach(secId -> mktValuesPerSecId.put(secId, new ArrayList<>()));
      region.values().forEach(value -> {
        if (!usePdx) {
          Portfolio portfolio = ((Portfolio) value);
          ((Map<String, Position>) portfolio.positions).values()
              .forEach(position -> mktValuesPerSecId.get(position.secId).add(position.mktValue));
        } else {
          PortfolioPdx portfolioPdx = ((PortfolioPdx) value);
          ((Map<String, PositionPdx>) portfolioPdx.positions).values().forEach(
              position -> mktValuesPerSecId.get(position.secId).add(position.getMktValue()));
        }
      });

      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(7);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      results.forEach(struct -> {
        String posSecId = (String) struct.getFieldValues()[0];
        assertThat(mktValuesPerSecId.containsKey(posSecId)).isTrue();
        List<Double> valuesPerSecId = mktValuesPerSecId.get(posSecId);

        assertThat(struct.getFieldValues()[1]).isEqualTo(valuesPerSecId.size());
        assertThat(struct.getFieldValues()[2]).isEqualTo(Collections.max(valuesPerSecId));
        assertThat(struct.getFieldValues()[3]).isEqualTo(Collections.min(valuesPerSecId));
        assertThat(struct.getFieldValues()[4])
            .isEqualTo(downCast(valuesPerSecId.stream().mapToDouble(Double::doubleValue).sum()));
        assertThat(struct.getFieldValues()[5]).isEqualTo(downCast(
            valuesPerSecId.stream().mapToDouble(Double::doubleValue).average().orElse(0d)));
      });
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void aggregateFunctionsWithoutGroupByShouldWorkCorrectly(String regionType, boolean usePdx,
      boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT AVG(" + expression + "), SUM(" + expression + "), MIN(" + expression
          + "), MAX(" + expression + "), COUNT(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0";
    } else {
      queryString = "SELECT AVG(" + expression + ") as ag, SUM(" + expression + ") as sm, MIN("
          + expression + ") as mn, MAX(" + expression + ") as mx, COUNT(" + expression
          + ") as ct FROM " + SEPARATOR + regionName + " p WHERE p.ID > 0";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
      int sumIDs = region.values().stream().mapToInt(toPortfolioID(usePdx)).sum();
      int minID = region.values().stream().mapToInt(toPortfolioID(usePdx)).min().orElse(-1);
      int maxID = region.values().stream().mapToInt(toPortfolioID(usePdx)).max().orElse(-1);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(1);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Struct structResult = results.asList().get(0);
      assertThat(structResult).isNotNull();
      assertThat(structResult.getFieldValues()[0])
          .isEqualTo(downCast(((double) sumIDs / region.size())));
      assertThat(structResult.getFieldValues()[1]).isEqualTo(sumIDs);
      assertThat(structResult.getFieldValues()[2]).isEqualTo(minID);
      assertThat(structResult.getFieldValues()[3]).isEqualTo(maxID);
      assertThat(structResult.getFieldValues()[4]).isEqualTo(300);
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void aggregateFunctionsWithoutGroupByShouldWorkCorrectlyWithIndexes(String regionType,
      boolean usePdx, boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.ID FROM " + SEPARATOR + regionName + " iter WHERE iter.ID = p.ID)"
        : "p.ID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT AVG(" + expression + "), SUM(" + expression + "), MIN(" + expression
          + "), MAX(" + expression + "), COUNT(" + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0";
    } else {
      queryString = "SELECT AVG(" + expression + ") as ag, SUM(" + expression + ") as sm, MIN("
          + expression + ") as mn, MAX(" + expression + ") as mx, COUNT(" + expression
          + ") as ct FROM " + SEPARATOR + regionName + " p WHERE p.ID > 0";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      Region<String, Object> region = ClusterStartupRule.getCache().getRegion(regionName);
      int sumIDs = region.values().stream().mapToInt(toPortfolioID(usePdx)).sum();
      int minID = region.values().stream().mapToInt(toPortfolioID(usePdx)).min().orElse(-1);
      int maxID = region.values().stream().mapToInt(toPortfolioID(usePdx)).max().orElse(-1);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(1);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Struct structResult = results.asList().get(0);
      assertThat(structResult).isNotNull();
      assertThat(structResult.getFieldValues()[0])
          .isEqualTo(downCast(((double) sumIDs / region.size())));
      assertThat(structResult.getFieldValues()[1]).isEqualTo(sumIDs);
      assertThat(structResult.getFieldValues()[2]).isEqualTo(minID);
      assertThat(structResult.getFieldValues()[3]).isEqualTo(maxID);
      assertThat(structResult.getFieldValues()[4]).isEqualTo(300);
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void aggregateFunctionsWithDistinctAndWithoutGroupByShouldWorkCorrectly(String regionType,
      boolean usePdx, boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.shortID FROM " + SEPARATOR + regionName
            + " iter WHERE iter.ID = p.ID)"
        : "p.shortID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT AVG(DISTINCT " + expression + "), SUM(DISTINCT " + expression
          + "), COUNT(DISTINCT " + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 AND p.isActive() = true";
    } else {
      queryString = "SELECT AVG(DISTINCT " + expression + ") as ag, SUM(DISTINCT " + expression
          + ") as sm, COUNT(DISTINCT " + expression + ") as ct FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 AND p.isActive() = true";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeDistinctShortIDsSum = sumDistinctShortIDsByStatus(true, usePdx);
      double activeDistinctShortIDsAvg =
          activeDistinctShortIDsSum / countDistinctShortIDsByStatus(true, usePdx);
      double activeDistinctShortIDsCount = countDistinctShortIDsByStatus(true, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(1);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Struct structResult = results.asList().get(0);
      assertThat(structResult).isNotNull();
      assertThat(structResult.getFieldValues()[0]).isEqualTo(downCast(activeDistinctShortIDsAvg));
      assertThat(structResult.getFieldValues()[1]).isEqualTo(downCast(activeDistinctShortIDsSum));
      assertThat(structResult.getFieldValues()[2]).isEqualTo(downCast(activeDistinctShortIDsCount));
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters(method = "regionTypeAndThreeBooleans")
  @TestCaseName("[{index}] {method}(RegionType:{0},PDX:{1},Alias:{2},NestedQuery:{3})")
  public void aggregateFunctionsWithDistinctAndWithoutGroupByShouldWorkCorrectlyWithIndexes(
      String regionType, boolean usePdx, boolean useAlias, boolean useNestedQuery) {
    createRegion(regionType);
    createIndexes();
    VMProvider.invokeInRandomMember(() -> populateRegion(usePdx, 300), server1, server2, server3,
        server4);
    String expression = useNestedQuery
        ? "ELEMENT(SELECT iter.shortID FROM " + SEPARATOR + regionName
            + " iter WHERE iter.ID = p.ID)"
        : "p.shortID";
    String queryString;

    if (!useAlias) {
      queryString = "SELECT AVG(DISTINCT " + expression + "), SUM(DISTINCT " + expression
          + "), COUNT(DISTINCT " + expression + ") FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 AND p.isActive() = true";
    } else {
      queryString = "SELECT AVG(DISTINCT " + expression + ") as ag, SUM(DISTINCT " + expression
          + ") as sm, COUNT(DISTINCT " + expression + ") as ct FROM " + SEPARATOR + regionName
          + " p WHERE p.ID > 0 AND p.isActive() = true";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      double activeDistinctShortIDsSum = sumDistinctShortIDsByStatus(true, usePdx);
      double activeDistinctShortIDsAvg =
          activeDistinctShortIDsSum / countDistinctShortIDsByStatus(true, usePdx);
      double activeDistinctShortIDsCount = countDistinctShortIDsByStatus(true, usePdx);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(1);
      assertThat(results.getCollectionType().getElementType().isStructType()).isTrue();

      Struct structResult = results.asList().get(0);
      assertThat(structResult).isNotNull();
      assertThat(structResult.getFieldValues()[0]).isEqualTo(downCast(activeDistinctShortIDsAvg));
      assertThat(structResult.getFieldValues()[1]).isEqualTo(downCast(activeDistinctShortIDsSum));
      assertThat(structResult.getFieldValues()[2]).isEqualTo(downCast(activeDistinctShortIDsCount));
    }, server1, server2, server3, server4);
  }

  @After
  public void tearDown() {
    // Always destroy everything to avoid race conditions with subsequent tests.
    gfsh.executeAndAssertThat("clear defined indexes").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy region --name=" + regionName).statusIsSuccess();
    gfsh.executeAndAssertThat("destroy index --name=shortIndex --if-exists").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy index --name=statusIndex --if-exists").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy index --name=idIndex --if-exists").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy index --name=secIdIndex --if-exists").statusIsSuccess();

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      Cache memberCache = ClusterStartupRule.getCache();
      await().untilAsserted(() -> assertThat(memberCache.getRegion(regionName)).isNull());
      await().untilAsserted(
          () -> assertThat(memberCache.getQueryService().getIndexes().isEmpty()).isTrue());
    }, server1, server2, server3, server4);
  }
}
