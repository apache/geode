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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Random;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.cache.query.internal.CompiledSelect;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

@RunWith(JUnitParamsRunner.class)
public class GroupByQueryDUnitTestBase implements Serializable {
  private static final String regionName = "portfolio";
  protected static MemberVM locator, server1, server2, server3, server4;

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

  private static void invokeOnRandomMember(SerializableRunnableIF runnableIF, VMProvider... members) {
    if (ArrayUtils.isEmpty(members)) {
      throw new IllegalArgumentException("Array of members must not be null nor empty.");
    }

    int randomMemberIndex = new Random().nextInt(members.length);
    members[randomMemberIndex].invoke(runnableIF);
  }

  private void populateRegion(int entriesAmount, boolean usePdx) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    Region<String, Object> portfolioRegion = ClusterStartupRule.getCache().getRegion(regionName);
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

      portfolioRegion.put("key-" + i, value);
    }
  }

  @Test
  @Parameters({"REPLICATE, false", "REPLICATE, true", "PARTITION, false", "PARTITION, true"})
  @TestCaseName("[{index}] {method}: RegionType -> {0}, UsePdx -> {1}")
  public void groupByShouldBeTransformableToDistinctOrderByWithSingleFieldAndReturnCorrectValues(String regionType, boolean usePdx) {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=" + regionType).statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + regionName, 4);
    invokeOnRandomMember(() -> populateRegion(200, usePdx), server1, server2, server3, server4);

    String queryString = "SELECT p.shortID FROM /" + regionName + " p WHERE p.ID >= 0 GROUP BY p.shortID ";
    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);
      CompiledSelect cs = ((DefaultQuery) query).getSelect();
      assertThat(cs.isDistinct()).isTrue();
      assertThat(cs.isOrderBy()).isTrue();
      assertThat(cs.isGroupBy()).isFalse();

      @SuppressWarnings("unchecked")
      SelectResults<Short> results = (SelectResults<Short>) query.execute();
      int counter = 0;
      for (Short shortID : results.asList()) {
        assertThat(shortID.intValue()).isEqualTo(counter++);
      }

      assertThat(counter - 1).isEqualTo(40);
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters({"REPLICATE, false", "REPLICATE, true", "PARTITION, false", "PARTITION, true"})
  @TestCaseName("[{index}] {method}: RegionType -> {0}, UsePdx -> {1}")
  public void groupByShouldBeTransformableToDistinctOrderByWithSingleFieldAsAliasAndReturnCorrectValues(String regionType, boolean usePdx) {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=" + regionType).statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + regionName, 4);
    invokeOnRandomMember(() -> populateRegion(200, usePdx), server1, server2, server3, server4);

    String queryString = "SELECT p.shortID AS short_id FROM /" + regionName + " p WHERE p.ID >= 0 GROUP BY short_id ";
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
      int counter = 0;
      for (Struct struct : results.asList()) {
        assertThat(((Short) struct.get("short_id")).intValue()).isEqualTo(counter++);
      }

      assertThat(counter - 1).isEqualTo(40);
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters({"REPLICATE, false", "REPLICATE, true", "PARTITION, false", "PARTITION, true"})
  @TestCaseName("[{index}] {method}: RegionType -> {0}, UsePdx -> {1}")
  public void groupByShouldBeTransformableToDistinctOrderByWithMultipleFieldsAndAliasesAndReturnCorrectValues(String regionType, boolean usePdx) {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=" + regionType).statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + regionName, 4);
    invokeOnRandomMember(() -> populateRegion(100, usePdx), server1, server2, server3, server4);

    String queryString = "SELECT p.status AS status, p.ID FROM /" + regionName + " p WHERE p.ID > 0 GROUP BY status, p.ID ";
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
      assertThat(results.size()).isEqualTo(100);
      for (Struct struct : results.asList()) {
        Integer id = (Integer) struct.get("ID");
        assertThat(((String) struct.get("status"))).isEqualTo(id % 2 == 0 ? "active" : "inactive");
      }
    }, server1, server2, server3, server4);
  }

  @Test
  @Parameters({ "REPLICATE, false, true", "REPLICATE, false, false",
                "REPLICATE, true, false", "REPLICATE, true, true",
                "PARTITION, false, false", "PARTITION, false, true",
                "PARTITION, true, false", "PARTITION, true, true"})
  @TestCaseName("[{index}] {method}: RegionType -> {0}, UsePdx -> {1}, UseAliases -> {2}")
  public void groupByUsedWithCountStarShouldWorkProperly(String regionType, boolean usePdx, boolean useAlias) {
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=" + regionType).statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + regionName, 4);
    invokeOnRandomMember(() -> populateRegion(200, usePdx), server1, server2, server3, server4);

    String queryString;
    if (useAlias) {
      queryString = "SELECT p.status AS status, COUNT(*) AS result FROM /" + regionName + " p WHERE p.ID > 0 GROUP BY status";
    } else {
      queryString = "SELECT p.status, COUNT(*) FROM /" + regionName + " p WHERE p.ID > 0 GROUP BY p.status";
    }

    VMProvider.invokeInEveryMember(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString);

      @SuppressWarnings("unchecked")
      SelectResults<Struct> results = (SelectResults<Struct>) query.execute();
      assertThat(results.size()).isEqualTo(2);
      Struct activeStruct = results.asList().get(0);
      assertThat(activeStruct.getFieldValues()[0]).isEqualTo("active");
      assertThat(activeStruct.getFieldValues()[1]).isEqualTo(100);
      Struct inactiveStruct = results.asList().get(1);
      assertThat(inactiveStruct.getFieldValues()[0]).isEqualTo("active");
      assertThat(inactiveStruct.getFieldValues()[1]).isEqualTo(100);
    }, server1, server2, server3, server4);
  }

  @After
  public void tearDown() {
    gfsh.executeAndAssertThat("destroy region --name=" + regionName).statusIsSuccess();
  }
}
