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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Category(DistributedTest.class)
public class ExportLogsOverHttpDUnitTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @ClassRule
  public static GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  private static int jmxPort, httpPort;
  private static Properties locatorProperties;
  private static Set<String> expectedZipEntries = new HashSet<>();

  @BeforeClass
  public static void beforeClass() throws Exception {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    httpPort = ports[0];
    jmxPort = ports[1];
    locatorProperties = new Properties();
    locatorProperties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    locatorProperties.setProperty(HTTP_SERVICE_PORT, httpPort + "");
    locatorProperties.setProperty(JMX_MANAGER_PORT, jmxPort + "");

    // start the locator in vm0 and then connect to it over http
    Locator locator = lsRule.startLocatorVM(0, locatorProperties);
    lsRule.startServerVM(1, locator.getPort());
    gfshConnector.connectAndVerify(httpPort, GfshShellConnectionRule.PortType.http);

    expectedZipEntries = Sets.newHashSet("locator-0/locator-0.log", "server-1/server-1.log");
  }

  @Test
  public void testExportWithNoDir() throws Exception {
    // export the logs
    gfshConnector.executeCommand("export logs");
    // verify that the message contains a path to the user.dir
    String message = gfshConnector.getGfshOutput();
    assertThat(message).contains("Logs exported to: ");
    assertThat(message).contains(System.getProperty("user.dir"));

    String zipPath = getZipPathFromCommandResult(message);
    verifyZipContent(zipPath);
  }

  @Test
  public void testExportWithDir() throws Exception {
    File dir = temporaryFolder.newFolder();
    // export the logs
    gfshConnector.executeCommand("export logs --dir=" + dir.getAbsolutePath());
    // verify that the message contains a path to the user.dir
    String message = gfshConnector.getGfshOutput();
    assertThat(message).contains("Logs exported to: ");
    assertThat(message).contains(dir.getAbsolutePath());

    String zipPath = getZipPathFromCommandResult(message);
    verifyZipContent(zipPath);
  }

  private void verifyZipContent(String zipPath) throws Exception {
    Set<String> actualZipEnries =
        new ZipFile(zipPath).stream().map(ZipEntry::getName).collect(Collectors.toSet());

    assertThat(actualZipEnries).isEqualTo(expectedZipEntries);
  }

  private String getZipPathFromCommandResult(String message) {
    return message.replaceAll("Logs exported to: ", "").trim();
  }
}
