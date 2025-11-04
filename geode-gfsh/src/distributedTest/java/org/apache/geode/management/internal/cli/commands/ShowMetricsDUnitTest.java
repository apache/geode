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


import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

/**
 * Tests for the "show metrics" gfsh command.
 *
 * <p>
 * Note on Spring Shell 3.x Migration (GEODE-10466):
 * This test required updates due to the removal of automatic parameter conversion that existed
 * in Spring Shell 1.x. Previously, the {@code RegionPathConverter} automatically prefixed region
 * names with "/" (e.g., "REGION1" became "/REGION1"). With Spring Shell 3.x, the
 * {@code @CliOption} annotation was replaced with {@code @ShellOption}, which doesn't support
 * the {@code optionContext = ConverterHint.REGION_PATH} parameter that triggered this automatic
 * conversion. The {@code RegionPathConverter} class was removed as part of the migration.
 *
 * <p>
 * As a result, tests must now:
 * <ul>
 * <li>Explicitly provide region paths with SEPARATOR prefix (e.g., "/REGION1") instead of
 * just region names (e.g., "REGION1")</li>
 * <li>Enable region statistics to ensure RegionMXBean exposes complete metrics</li>
 * <li>Wait for MBean federation to complete before executing commands that query
 * member-specific MBeans</li>
 * </ul>
 *
 * <p>
 * These changes ensure compatibility with the Spring Shell 3.x command framework while
 * maintaining correct behavior for the "show metrics" command.
 */
public class ShowMetricsDUnitTest {

  private MemberVM locator, server;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server = lsRule.startServerVM(1, locator.getPort());
    int serverPort = server.getPort();
    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<Integer, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.REPLICATE);
      // Enable statistics on the region to ensure RegionMXBean exposes complete metrics.
      // Without statistics enabled, the RegionMXBean may not be fully initialized or may
      // not expose certain metric values that the "show metrics" command expects to retrieve.
      // This is consistent with ShowMetricsCommandIntegrationTest which uses --enable-statistics.
      dataRegionFactory.setStatisticsEnabled(true);
      dataRegionFactory.create("REGION1");

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();
      await().until(() -> isBeanReady(cache, 5, "", member, serverPort));
    });

    locator.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      // Wait for all of the relevant beans to be ready
      await().until(() -> isBeanReady(cache, 1, "", null, 0));
      await().until(() -> isBeanReady(cache, 2, "REGION1", null, 0));

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();
      await().until(() -> isBeanReady(cache, 3, "", member, 0));
    });

    // Critical: Wait for the RegionMXBean for REGION1 on the server to be fully federated
    // to the locator's management service before executing gfsh commands.
    //
    // The testShowMetricsRegionFromMember test queries region metrics from a specific member,
    // which requires the member's RegionMXBean to be:
    // 1. Created on the server (happens when region is created with statistics enabled)
    // 2. Federated to the locator's ManagementService (happens asynchronously)
    // 3. Available via ManagementService.getMBeanInstance() (ready for JMX queries)
    //
    // Without this wait, the "show metrics --member=X --region=Y" command may execute before
    // federation completes, resulting in "Region MBean for REGION1 on member server-1 not found".
    //
    // waitUntilRegionIsReadyOnExactlyThisManyServers ensures the DistributedRegionMXBean shows
    // the region is present on exactly 1 server, which guarantees the member-specific RegionMXBean
    // is also available for queries.
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "REGION1", 1);

    gfsh.connect(locator);
  }

  private static boolean isBeanReady(Cache cache, int beanType, String regionName,
      DistributedMember distributedMember, int cacheServerPort) {
    ManagementService mgmtService = ManagementService.getManagementService(cache);
    Object bean = null;

    switch (beanType) {
      case 1:
        bean = mgmtService.getDistributedSystemMXBean();
        break;
      case 2:
        bean = mgmtService.getDistributedRegionMXBean(SEPARATOR + regionName);
        break;
      case 3:
        ObjectName memberMBeanName = mgmtService.getMemberMBeanName(distributedMember);
        bean = mgmtService.getMBeanInstance(memberMBeanName, MemberMXBean.class);
        break;
      case 4:
        ObjectName regionMBeanName =
            mgmtService.getRegionMBeanName(distributedMember, SEPARATOR + regionName);
        bean = mgmtService.getMBeanInstance(regionMBeanName, RegionMXBean.class);
        break;
      case 5:
        ObjectName csMxBeanName =
            mgmtService.getCacheServerMBeanName(cacheServerPort, distributedMember);
        bean = mgmtService.getMBeanInstance(csMxBeanName, CacheServerMXBean.class);
        break;
    }

    return bean != null;
  }

  @Test
  public void testShowMetricsDefault() throws Exception {
    gfsh.executeAndAssertThat("show metrics").statusIsSuccess();
  }

  @Test
  public void testShowMetricsRegion() throws Exception {
    // Region path must include SEPARATOR prefix due to Spring Shell 3.x migration.
    // The RegionPathConverter that automatically added "/" was removed.
    // See class-level Javadoc for details on Spring Shell 3.x migration impact.
    gfsh.executeAndAssertThat("show metrics --region=" + SEPARATOR + "REGION1").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Cluster-wide Region Metrics");
  }

  @Test
  public void testShowMetricsMember() throws Exception {
    gfsh.executeAndAssertThat(
        "show metrics --member=" + server.getName() + " --port=" + server.getPort())
        .statusIsSuccess().containsOutput("Member Metrics").containsOutput("cacheserver");
  }

  @Test
  public void testShowMetricsMemberWithFileOutput() throws Exception {
    File output = tempFolder.newFile("memberMetricReport.csv");
    output.delete();

    gfsh.executeAndAssertThat("show metrics --member=" + server.getName() + " --port="
        + server.getPort() + " --file=" + output.getAbsolutePath()).statusIsSuccess()
        .containsOutput("Member Metrics").containsOutput("cacheserver")
        .containsOutput("File saved to " + output.getAbsolutePath());

    assertThat(output).exists();
  }

  @Test
  public void testShowMetricsRegionFromMember() throws Exception {
    // Important: The region parameter must include the SEPARATOR (/) prefix to form a valid
    // region path, not just a region name. The ShowMetricsCommand.getRegionMetricsFromMember()
    // method calls ManagementService.getRegionMBeanName(member, regionPath), which expects
    // a full region path like "/REGION1" rather than just "REGION1".
    //
    // Spring Shell 3.x Migration Note:
    // In the develop branch (Spring Shell 1.x), the RegionPathConverter automatically added
    // the "/" prefix via its convertFromText() method when processing @CliOption parameters
    // with optionContext = ConverterHint.REGION_PATH. This allowed tests to pass "REGION1"
    // and have it automatically converted to "/REGION1".
    //
    // With Spring Shell 3.x, @CliOption was replaced with @ShellOption which doesn't support
    // optionContext, and RegionPathConverter was removed. Tests must now explicitly provide
    // the full region path with SEPARATOR prefix.
    //
    // This is consistent with:
    // - ShowMetricsCommandIntegrationTest which uses SEPARATOR + "region2"
    // - ManagementService.getRegionMBeanName() API documentation which specifies "regionPath"
    // - The expected output format which includes "region:/REGION1"
    gfsh.executeAndAssertThat(
        "show metrics --member=" + server.getName() + " --region=" + SEPARATOR + "REGION1")
        .statusIsSuccess();
    assertThat(gfsh.getGfshOutput())
        .contains("Metrics for region:" + SEPARATOR + "REGION1 On Member server-1");
  }
}
