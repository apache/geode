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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class, LoggingTest.class})
public class ExportLogsWithMemberGroupDistributedTest {

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule().withLogFile();

  @ClassRule
  public static GfshCommandRule connector = new GfshCommandRule();

  protected static MemberVM locator;
  protected static int jmxPort;
  private static int httpPort;
  private static Set<String> expectedZipEntries = new HashSet<>();

  @BeforeClass
  public static void beforeClass() {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    httpPort = ports[0];
    jmxPort = ports[1];
    Properties locatorProperties = new Properties();
    locatorProperties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    locatorProperties.setProperty(HTTP_SERVICE_PORT, httpPort + "");
    locatorProperties.setProperty(JMX_MANAGER_PORT, jmxPort + "");

    // start the locator in vm0 and then connect to it over http
    locator = lsRule.startLocatorVM(0, locatorProperties);

    Properties serverProperties = new Properties();
    serverProperties.setProperty(GROUPS, "group1");
    lsRule.startServerVM(1, serverProperties, locator.getPort());
    lsRule.startServerVM(2, serverProperties, locator.getPort());

    serverProperties.setProperty(GROUPS, "group2");
    lsRule.startServerVM(3, serverProperties, locator.getPort());

    expectedZipEntries = Sets.newHashSet("locator-0/locator-0.log", "server-1/server-1.log",
        "server-1/statistics.gfs");
  }

  @Test
  public void testExportLogsWithMemberName() throws Exception {
    connectIfNeeded();
    connector.executeAndAssertThat("export logs --member=server-1").statusIsSuccess();
    String zipPath = getZipPathFromCommandResult(connector.getGfshOutput());
    Set<String> actualZipEntries = getZipEntries(zipPath);

    Set<String> expectedFiles = Sets.newHashSet(Paths.get("server-1", "server-1.log").toString());
    assertThat(actualZipEntries).isEqualTo(expectedFiles);
  }

  @Test
  public void testExportLogsWithGroupName() throws Exception {
    connectIfNeeded();
    connector.executeAndAssertThat("export logs --group=group1").statusIsSuccess();
    String zipPath = getZipPathFromCommandResult(connector.getGfshOutput());
    Set<String> actualZipEntries = getZipEntries(zipPath);

    Set<String> expectedFiles = Sets.newHashSet(Paths.get("server-1", "server-1.log").toString(),
        Paths.get("server-2", "server-2.log").toString());
    assertThat(actualZipEntries).isEqualTo(expectedFiles);

    connector.executeAndAssertThat("export logs --group=group2").statusIsSuccess();
    zipPath = getZipPathFromCommandResult(connector.getGfshOutput());
    actualZipEntries = getZipEntries(zipPath);

    expectedFiles = Sets.newHashSet(Paths.get("server-3", "server-3.log").toString());
    assertThat(actualZipEntries).isEqualTo(expectedFiles);
  }

  @Test
  public void testExportLogsWithGroupAndMemberName() throws Exception {
    connectIfNeeded();
    CommandResult results =
        connector.executeCommand("export logs --group=group1 --member=server-1");
    assertThat(results.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(connector.getGfshOutput()).contains("Can't specify both group and member");
  }

  private void connectIfNeeded() throws Exception {
    if (!connector.isConnected()) {
      connector.connect(locator);
    }
  }

  private String getZipPathFromCommandResult(String message) {
    return message.replaceAll("Logs exported to the connected member's file system: ", "").trim();
  }

  private static Set<String> getZipEntries(String zipFilePath) throws IOException {
    return new ZipFile(zipFilePath).stream().map(ZipEntry::getName)
        .filter(x -> !x.endsWith("views.log")).collect(Collectors.toSet());
  }
}
