/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({GfshTest.class})
public class ShowMetricsCommandIntegrationTest {
  private static final Logger logger = LogService.getLogger();
  private static final String REGION_NAME = "test-region";
  private static final String MEMBER_NAME = "testServer";

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withRegion(RegionShortcut.REPLICATE, REGION_NAME)
          .withName(MEMBER_NAME).withJMXManager().withAutoStart();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule(server::getJmxPort, PortType.jmxManager);

  @Test
  public void everyCategoryHasAUseCase() {
    Set<ShowMetricsCommand.Category> categoriesUsed = new HashSet<>();
    categoriesUsed.addAll(ShowMetricsCommand.REGION_METRIC_CATEGORIES);
    categoriesUsed.addAll(ShowMetricsCommand.MEMBER_METRIC_CATEGORIES);
    categoriesUsed.addAll(ShowMetricsCommand.MEMBER_WITH_PORT_METRIC_CATEGORIES);
    categoriesUsed.addAll(ShowMetricsCommand.SYSTEM_METRIC_CATEGORIES);
    categoriesUsed.addAll(ShowMetricsCommand.SYSTEM_REGION_METRIC_CATEGORIES);

    Set<ShowMetricsCommand.Category> categoriesSpecified =
        Arrays.stream(ShowMetricsCommand.Category.values()).collect(Collectors.toSet());

    assertThat(categoriesSpecified)
        .containsExactlyInAnyOrder(categoriesUsed.toArray(new ShowMetricsCommand.Category[0]));
  }

  @Test
  public void commandFailsWhenNotConnected() throws Exception {
    gfsh.disconnect();
    gfsh.executeAndAssertThat("show metrics")
        .containsOutput("was found but is not currently available");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void getRegionMetricsShowsExactlyDefaultCategories() {
    // Use --region and --member to get RegionMetricsFromMember
    String cmd = "show metrics --region=/" + REGION_NAME + " --member=" + MEMBER_NAME;
    List<String> expectedCategories =
        ShowMetricsInterceptor.getValidCategoriesAsStrings(true, true, false);
    // Blank lines are permitted for grouping.
    expectedCategories.add("");

    gfsh.executeAndAssertThat(cmd).tableHasColumnOnlyWithValues("Category",
        expectedCategories.toArray(new String[0]));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void getSystemRegionMetricsShowsExactlyDefaultCategories() {
    // Use --region alone to get SystemRegionMetrics
    String cmd = "show metrics --region=/" + REGION_NAME;
    List<String> expectedCategories =
        ShowMetricsInterceptor.getValidCategoriesAsStrings(true, false, false);
    // Blank lines are permitted for grouping.
    expectedCategories.add("");
    logger.info("Expecting categories: " + String.join(", ", expectedCategories));

    gfsh.executeAndAssertThat(cmd).tableHasColumnOnlyWithValues("Category",
        expectedCategories.toArray(new String[0]));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void getMemberMetricsShowsExactlyDefaultCategories() {
    // Use --member to get member metrics
    String cmd = "show metrics --member=" + MEMBER_NAME;
    List<String> expectedCategories =
        ShowMetricsInterceptor.getValidCategoriesAsStrings(false, true, false);
    // Blank lines are permitted for grouping.
    expectedCategories.add("");
    logger.info("Expecting categories: " + String.join(", ", expectedCategories));

    gfsh.executeAndAssertThat(cmd).tableHasColumnOnlyWithValues("Category",
        expectedCategories.toArray(new String[0]));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void getMemberWithPortMetricsShowsExactlyDefaultCategories() {
    // Use --member and --port to get member metrics with port info
    String cmd = "show metrics --member=" + MEMBER_NAME + " --port=" + server.getPort();
    List<String> expectedCategories =
        ShowMetricsInterceptor.getValidCategoriesAsStrings(false, true, true);
    // Blank lines are permitted for grouping.
    expectedCategories.add("");
    logger.info("Expecting categories: " + String.join(", ", expectedCategories));

    gfsh.executeAndAssertThat(cmd).tableHasColumnOnlyWithValues("Category",
        expectedCategories.toArray(new String[0]));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void getSystemMetricsShowsExactlyDefaultCategories() {
    // No specified options yield system-wide metrics
    String cmd = "show metrics";
    List<String> expectedCategories =
        ShowMetricsInterceptor.getValidCategoriesAsStrings(false, false, false);
    // Blank lines are permitted for grouping.
    expectedCategories.add("");
    logger.info("Expecting categories: " + String.join(", ", expectedCategories));

    gfsh.executeAndAssertThat(cmd).tableHasColumnOnlyWithValues("Category",
        expectedCategories.toArray(new String[0]));
  }

  @Test
  public void invalidCategoryGetsReported() {
    String cmd =
        "show metrics --categories=\"cluster,cache,some_invalid_category,another_invalid_category\"";

    gfsh.executeAndAssertThat(cmd).containsOutput("Invalid Categories")
        .containsOutput("some_invalid_category").containsOutput("another_invalid_category")
        .doesNotContainOutput("cache").doesNotContainOutput("cluster");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void categoryOptionAbridgesOutput() {
    String cmd = "show metrics --categories=\"cluster,cache\"";
    List<String> expectedCategories = Arrays.asList("cluster", "cache", "");
    logger.info("Expecting categories: " + String.join(", ", expectedCategories));

    gfsh.executeAndAssertThat(cmd).tableHasColumnOnlyWithValues("Category",
        expectedCategories.toArray(new String[0]));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void getRegionMetricsForPartitionedRegionWithStatistics() {
    String cmd = "create region --name=region2 --type=PARTITION --enable-statistics";
    gfsh.executeAndAssertThat(cmd).statusIsSuccess();
    String cmd2 = "show metrics --member=" + MEMBER_NAME + " --region=region2";
    gfsh.executeAndAssertThat(cmd2).statusIsSuccess().tableHasRowWithValues("Category", "Metric",
        "Value", "", "missCount", "0");
  }
}
