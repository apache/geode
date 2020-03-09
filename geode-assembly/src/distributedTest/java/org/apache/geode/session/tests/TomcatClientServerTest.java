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
package org.apache.geode.session.tests;


import java.io.File;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.serialization.Version;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

/**
 * Setup class for Tomcat Client Server tests
 *
 * Sets up the server needed for the client container to connect to
 */
public abstract class TomcatClientServerTest extends TomcatTest {
  private final ArrayList<String> serverList = new ArrayList<>();
  private final int numberOfServers = 2;

  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  /**
   * Starts a server for the client Tomcat container to connect to using the GFSH command line
   * before each test
   */
  @Before
  public void startServer() throws Exception {
    for (int i = 0; i < numberOfServers; i++) {
      serverList.add(startAServer(i));
    }

    afterStartServers();

    if (isCachingClient()) {
      deployJars();
    }
  }

  private void deployJars() throws Exception {
    String currentVersion = Version.CURRENT.getName();

    String geodeHome = requiresGeodeHome.getGeodeHome().getAbsolutePath();
    int index = geodeHome.indexOf("geode-assembly");
    geodeHome = geodeHome.substring(0, index);
    String fileName = geodeHome + "extensions/session-testing-war/build/libs/session-testing-war-"
        + currentVersion + "-SNAPSHOT.jar";
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.DEPLOY);
    command.addOption(CliStrings.JAR, fileName);
    gfsh.connectAndVerify(locatorVM.getPort(), GfshCommandRule.PortType.locator);
    gfsh.executeAndAssertThat(command.toString()).statusIsSuccess();
  }

  public void afterStartServers() throws Exception {}

  private String startAServer(int serverNumber) {
    // List of all the jars for tomcat to put on the server classpath
    String libDirJars = install.getHome() + "/lib/*";
    String binDirJars = install.getHome() + "/bin/*";

    // Set server name based on the test about to be run
    String serverName =
        getClass().getSimpleName() + "_" + testName.getMethodName() + "_" + serverNumber;

    // Create command string for starting server
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_SERVER);
    command.addOption(CliStrings.START_SERVER__NAME, serverName);
    command.addOption(CliStrings.START_SERVER__SERVER_PORT, "0");
    // Add Tomcat jars to server classpath
    command.addOption(CliStrings.START_SERVER__CLASSPATH,
        binDirJars + File.pathSeparator + libDirJars);
    command.addOption(CliStrings.START_SERVER__LOCATORS,
        locatorVM.invoke(() -> ClusterStartupRule.getLocator().asString()));
    // statistic file
    command.addOption(CliStrings.START_SERVER__STATISTIC_ARCHIVE_FILE, "statArchive.gfs");

    command.addOption(CliStrings.START_SERVER__J, "-Dgemfire.member-timeout=60000");
    command.addOption(CliStrings.START_SERVER__J, "-Dgemfire.statistic-sampling-enabled=true");
    command.addOption(CliStrings.START_SERVER__J, "-XX:+HeapDumpOnOutOfMemoryError");
    command.addOption(CliStrings.START_SERVER__J, "-XX:+JavaMonitorsInStackTrace");

    // Start server
    gfsh.executeAndAssertThat(command.toString()).statusIsSuccess();

    return serverName;
  }

  /**
   * Stops the server for the client Tomcat container is has been connecting to
   */
  @After
  public void stopServer() {
    for (int i = 0; i < numberOfServers; i++) {
      try {
        stopAServer(serverList.get(i));
      } catch (Exception ignore) {
      }
    }
  }

  private void stopAServer(String serverName) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STOP_SERVER);
    command.addOption(CliStrings.STOP_SERVER__DIR, serverName);
    gfsh.executeAndAssertThat(command.toString()).statusIsSuccess();
  }
}
