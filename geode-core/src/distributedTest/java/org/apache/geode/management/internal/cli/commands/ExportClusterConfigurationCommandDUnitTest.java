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
import static org.apache.geode.management.internal.cli.i18n.CliStrings.EXPORT_SHARED_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ExportClusterConfigurationCommandDUnitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();


  private static File xmlFile;
  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() throws Exception {
    xmlFile = tempFolder.newFile("my.xml");
    locator = cluster.startLocatorVM(0);
    Properties properties = new Properties();
    properties.setProperty(GROUPS_NAME, "groupB");
    cluster.startServerVM(1, properties, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=regionA --type=REPLICATE").statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=regionB --type=REPLICATE --group=groupB")
        .statusIsSuccess();
  }

  @Test
  public void getClusterConfig() {
    gfsh.executeAndAssertThat(EXPORT_SHARED_CONFIG).statusIsSuccess()
        .containsOutput("<region name=\"regionA\">").containsOutput("cluster.xml")
        .doesNotContainOutput("<region name=\"regionB\">");
  }


  @Test
  public void getClusterConfigInGroup() {
    gfsh.executeAndAssertThat(EXPORT_SHARED_CONFIG + " --group=groupB")
        .containsOutput("<region name=\"regionB\">")
        .doesNotContainOutput("<region name=\"regionA\">");
  }

  @Test
  public void getClusterConfigWithFile() throws IOException {
    gfsh.executeAndAssertThat(EXPORT_SHARED_CONFIG + " --xml-file=" + xmlFile.getAbsolutePath())
        .statusIsSuccess().containsOutput("cluster.xml")
        .containsOutput("xml content exported to " + xmlFile.getAbsolutePath());

    assertThat(xmlFile).exists();
    String content = FileUtils.readFileToString(xmlFile, Charset.defaultCharset());
    assertThat(content).startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>")
        .contains("<region name=\"regionA\">");
  }
}
