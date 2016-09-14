/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.lucene.internal.configuration;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_CONFIGURATION_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.DEPLOY_WORKING_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.lucene.analysis.Analyzer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexCommands;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.commands.CliCommandTestBase;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.LocatorServerConfigurationRule;
import org.apache.geode.test.junit.categories.DistributedTest;


@Category(DistributedTest.class)
public class LuceneClusterConfigurationDUnitTest extends CliCommandTestBase {

  private String groupName = "Lucene";

  @Rule
  public LocatorServerConfigurationRule ls = new LocatorServerConfigurationRule(
      this);

  @Test
  public void indexGetsCreatedUsingClusterConfiguration()
      throws Exception {
    VM locator = startLocatorWithClusterConfigurationEnabled();
    VM vm1 = startNodeUsingClusterConfiguration(1, false);

    // Connect Gfsh to locator.
    createAndConnectGfshToLocator();

    // Create lucene index.
    createLuceneIndexUsingGfsh(false);

    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

    // Start vm2. This should have lucene index created using cluster
    // configuration.
    VM vm2 = startNodeUsingClusterConfiguration(2, false);
    vm2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] { "field1", "field2", "field3" }, index);
    });
  }

  @Test
  public void indexWithAnalyzerGetsCreatedUsingClusterConfiguration()
      throws Exception {
    VM locator = startLocatorWithClusterConfigurationEnabled();
    VM vm1 = startNodeUsingClusterConfiguration(1, false);

    // Connect Gfsh to locator.
    createAndConnectGfshToLocator();

    // Create lucene index.
    // createLuceneIndexUsingGfsh(false);
    createLuceneIndexWithAnalyzerUsingGfsh(false);

    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, null);

    // Start vm2. This should have lucene index created using cluster
    // configuration.
    VM vm2 = startNodeUsingClusterConfiguration(2, false);
    vm2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      String[] fields = new String[] { "field1", "field2", "field3" };
      validateIndexFields(fields, index);
      // Add this check back when we complete xml generation for analyzer.
      this.validateIndexFieldAnalyzer(fields, new String[] {
          "org.apache.lucene.analysis.standard.StandardAnalyzer",
          "org.apache.lucene.analysis.standard.StandardAnalyzer",
          "org.apache.lucene.analysis.standard.StandardAnalyzer" }, index);
    });
  }

  @Test
  public void indexGetsCreatedOnGroupOfNodes() throws Exception {
    VM locator = startLocatorWithClusterConfigurationEnabled();

    // Start vm1, vm2 in group
    VM vm1 = startNodeUsingClusterConfiguration(1, true);
    VM vm2 = startNodeUsingClusterConfiguration(2, true);

    // Start vm3 outside the group. The Lucene index should not be present here.
    VM vm3 = startNodeUsingClusterConfiguration(3, true);

    // Connect Gfsh to locator.
    createAndConnectGfshToLocator();

    // Create lucene index on group.
    createLuceneIndexUsingGfsh(true);

    // Create region.
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, groupName);

    // VM2 should have lucene index created using gfsh execution.
    vm2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] { "field1", "field2", "field3" }, index);
    });

    // The Lucene index is present in vm3.
    vm3.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
    });
  }

  @Test
  public void indexNotCreatedOnNodeOutSideTheGroup()
      throws Exception {
    VM locator = startLocatorWithClusterConfigurationEnabled();

    // Start vm1, vm2 in group
    VM vm1 = startNodeUsingClusterConfiguration(1, true);
    VM vm2 = startNodeUsingClusterConfiguration(2, true);

    // Start vm3 outside the group. The Lucene index should not be present here.
    VM vm3 = startNodeUsingClusterConfiguration(3, false);

    // Connect Gfsh to locator.
    createAndConnectGfshToLocator();

    // Create lucene index on group.
    createLuceneIndexUsingGfsh(true);

    // Create region.
    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, groupName);

    // VM2 should have lucene index created using gfsh execution
    vm2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] { "field1", "field2", "field3" }, index);
    });

    // The Lucene index should not be present in vm3.
    vm3.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNull(index);
    });
  }

  @Test
  public void indexAreCreatedInValidGroupOfNodesJoiningLater()
      throws Exception {
    VM locator = startLocatorWithClusterConfigurationEnabled();

    // Start vm1 in group
    VM vm1 = startNodeUsingClusterConfiguration(1, true);
    // Connect Gfsh to locator.
    createAndConnectGfshToLocator();

    // Create lucene index on group.
    createLuceneIndexUsingGfsh(true);

    createRegionUsingGfsh(REGION_NAME, RegionShortcut.PARTITION, groupName);

    // Start vm2 in group
    VM vm2 = startNodeUsingClusterConfiguration(2, true);

    // Start vm3 outside the group. The Lucene index should not be present here.
    VM vm3 = startNodeUsingClusterConfiguration(3, false);

    // VM2 should have lucene index created using gfsh execution
    vm2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNotNull(index);
      validateIndexFields(new String[] { "field1", "field2", "field3" }, index);
    });

    // The Lucene index should not be present in vm3.
    vm3.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertNull(index);
    });
  }

  private void createAndConnectGfshToLocator() {
    HeadlessGfsh gfsh = getDefaultShell();
    connect(jmxHost, jmxPort, httpPort, gfsh);
  }

  private VM startNodeUsingClusterConfiguration(int vmIndex, boolean addGroup)
      throws Exception {
    File dir = this.temporaryFolder.newFolder();
    Properties nodeProperties = new Properties();
    nodeProperties.setProperty(USE_CLUSTER_CONFIGURATION, "true");
    nodeProperties.setProperty(DEPLOY_WORKING_DIR,
 dir.getCanonicalPath());
    if (addGroup) {
      nodeProperties.setProperty(GROUPS, groupName);
    }
    return ls.getNodeVM(vmIndex, nodeProperties);
  }

  private VM startLocatorWithClusterConfigurationEnabled() throws Exception {
    try {
      jmxHost = InetAddress.getLocalHost().getHostName();
    }
    catch (UnknownHostException ignore) {
      jmxHost = "localhost";
    }

    File dir = this.temporaryFolder.newFolder();

    final int[] ports = getRandomAvailableTCPPorts(2);
    jmxPort = ports[0];
    httpPort = ports[1];

    Properties locatorProps = new Properties();
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    locatorProps.setProperty(JMX_MANAGER, "true");
    locatorProps.setProperty(JMX_MANAGER_START, "true");
    locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS, String.valueOf(jmxHost));
    locatorProps.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    locatorProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));
    locatorProps.setProperty(CLUSTER_CONFIGURATION_DIR,
 dir.getCanonicalPath());
    return ls.getLocatorVM(locatorProps);
  }

  private void createLuceneIndexUsingGfsh(boolean addGroup) throws Exception {
    // Execute Gfsh command to create lucene index.
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());
    CommandStringBuilder csb = new CommandStringBuilder(
        LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    if (addGroup) {
      csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__GROUP, groupName);
    }
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD,
        "field1,field2,field3");
    executeCommand(csb.toString());
  }

  private void createLuceneIndexWithAnalyzerUsingGfsh(boolean addGroup)
      throws Exception {
    // Gfsh command to create lucene index.
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());
    CommandStringBuilder csb = new CommandStringBuilder(
        LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD,
        "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
        "org.apache.lucene.analysis.standard.StandardAnalyzer,"
            + "org.apache.lucene.analysis.standard.StandardAnalyzer,"
            + "org.apache.lucene.analysis.standard.StandardAnalyzer");

    if (addGroup) {
      csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__GROUP, groupName);
    }
    // Execute Gfsh command.
    executeCommand(csb.toString());
  }

  private void createRegionUsingGfsh(String regionName,
      RegionShortcut regionShortCut, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(
        CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
        regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.CREATE_REGION__GROUP, group);
    executeAndVerifyCommand(csb.toString());
  }

  private void executeAndVerifyCommand(String commandString) {
    CommandResult cmdResult = executeCommand(commandString);
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
        "Command : " + commandString);
    org.apache.geode.test.dunit.LogWriterUtils.getLogWriter().info(
        "Command Result : " + commandResultToString(cmdResult));
    assertEquals(Status.OK, cmdResult.getStatus());
  }

  private void validateIndexFields(String[] indexFields, LuceneIndex index) {
    String[] indexFieldNames = index.getFieldNames();
    Arrays.sort(indexFieldNames);
    assertArrayEquals(indexFields, indexFieldNames);
  }

  private void validateIndexFieldAnalyzer(String[] fields, String[] analyzers,
      LuceneIndex index) {
    Map<String, Analyzer> indexfieldAnalyzers = index.getFieldAnalyzers();
    for (int i = 0; i < fields.length; i++) {
      Analyzer a = indexfieldAnalyzers.get(fields[i]);
      System.out.println("#### Analyzer name :" + a.getClass().getName());
      assertEquals(analyzers[i], a.getClass().getName());
    }
  }

}
