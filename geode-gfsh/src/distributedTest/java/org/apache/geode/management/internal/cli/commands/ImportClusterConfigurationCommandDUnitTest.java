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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.internal.DistributionConfig.GROUPS_NAME;
import static org.apache.geode.management.internal.i18n.CliStrings.IMPORT_SHARED_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ImportClusterConfigurationCommandDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private File xmlFile;
  private MemberVM locator;
  private String commandWithFile;

  private static final String CLUSTER_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
          + "<cache xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" copy-on-read=\"false\" is-server=\"false\" lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\">\n"
          + "<region name=\"regionForCluster\">\n"
          + "    <region-attributes data-policy=\"replicate\" scope=\"distributed-ack\"/>\n"
          + "  </region>\n" + "</cache>\n";

  private static final String GROUP_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
          + "<cache xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" copy-on-read=\"false\" is-server=\"false\" lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\">\n"
          + "<region name=\"regionForGroupA\">\n"
          + "    <region-attributes data-policy=\"replicate\" scope=\"distributed-ack\"/>\n"
          + "  </region>\n" + "</cache>\n";

  @Before
  public void setUp() throws Exception {
    xmlFile = tempFolder.newFile("my.xml");
    FileUtils.write(xmlFile, CLUSTER_XML, Charset.defaultCharset());
    commandWithFile = IMPORT_SHARED_CONFIG + " --xml-file=" + xmlFile.getAbsolutePath() + " ";
    locator = cluster.startLocatorVM(0);
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void noServerToBeginWith() throws IOException {
    gfsh.executeAndAssertThat(commandWithFile).statusIsSuccess()
        .containsOutput("Successfully set the 'cluster' configuration to the content of");

    MemberVM server1 = cluster.startServerVM(1, locator.getPort());
    server1.invoke(() -> {
      Region<?, ?> region = ClusterStartupRule.getCache().getRegion("regionForCluster");
      assertThat(region).isNotNull();
      assertThat(region.getAttributes().getDataPolicy()).isEqualTo(DataPolicy.REPLICATE);
    });

    // you can not configure the servers because they are not vanilla servers
    gfsh.executeAndAssertThat(commandWithFile).statusIsError()
        .containsOutput("Can not configure servers that are already configured");

    // you can only stage with existing servers
    gfsh.executeAndAssertThat(commandWithFile + " --action=STAGE").statusIsSuccess()
        .containsOutput("Existing servers are not affected with this configuration change");

    FileUtils.write(xmlFile, GROUP_XML, Charset.defaultCharset());
    // you can set cluster configuration for another group
    gfsh.executeAndAssertThat(commandWithFile + " --group=groupA").statusIsSuccess()
        .containsOutput("Successfully set the 'groupA' configuration to the content of");

    // when start up another server in groupA, it should get both regions
    Properties properties = new Properties();
    properties.setProperty(GROUPS_NAME, "groupA");
    MemberVM server2 = cluster.startServerVM(2, properties, locator.getPort());
    server2.invoke(() -> {
      Region<?, ?> region1 = ClusterStartupRule.getCache().getRegion("regionForCluster");
      Region<?, ?> region2 = ClusterStartupRule.getCache().getRegion("regionForGroupA");
      assertThat(region1).isNotNull();
      assertThat(region2).isNotNull();
    });

    // server1 is not affected
    server1.invoke(() -> {
      Region<?, ?> region1 = ClusterStartupRule.getCache().getRegion("regionForCluster");
      Region<?, ?> region2 = ClusterStartupRule.getCache().getRegion("regionForGroupA");
      assertThat(region1).isNotNull();
      assertThat(region2).isNull();
    });
  }

  @Test
  public void canNotConfigureIfServersAreNotEmpty() {
    // start a server, and create a standalone region on that server
    MemberVM server = cluster.startServerVM(1, locator.getPort());
    server.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("regionA");
    });

    gfsh.executeAndAssertThat(commandWithFile).statusIsError()
        .containsOutput("Can not configure servers with existing regions: regionA");
  }

  @Test
  public void configureVanillaServers() {
    Properties properties = new Properties();
    properties.setProperty(GROUPS_NAME, "groupA");
    @SuppressWarnings("unused")
    MemberVM serverA = cluster.startServerVM(1, properties, locator.getPort());
    gfsh.executeAndAssertThat(commandWithFile + " --group=groupA").statusIsSuccess()
        .containsOutput("Successfully set the 'groupA' configuration to the content of")
        .containsOutput("Configure the servers in 'groupA' group").containsOutput("server-1")
        .containsOutput("Cache successfully reloaded.");

    // start another server that belongs to both groupA and groupB
    properties.setProperty(GROUPS_NAME, "groupA,groupB");
    @SuppressWarnings("unused")
    MemberVM serverB = cluster.startServerVM(2, properties, locator.getPort());

    // try to set the cluster configuration of groupB, in this case, we can't bounce serverB because
    // it's already configured by groupA
    gfsh.executeAndAssertThat(commandWithFile + " --group=groupB").statusIsError()
        .containsOutput("Can not configure servers that are already configured.");
  }
}
