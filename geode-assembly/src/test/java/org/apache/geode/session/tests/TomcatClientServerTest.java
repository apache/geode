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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.DUnitEnv;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;

/**
 * Setup class for Tomcat Client Server tests
 *
 * Sets up the server needed for the client container to connect to
 */
public abstract class TomcatClientServerTest extends CargoTestBase {
  private String serverName;

  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public transient GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Rule
  public transient LocatorServerStartupRule locatorStartup = new LocatorServerStartupRule();

  /**
   * Starts a server for the client Tomcat container to connect to using the GFSH command line
   * before each test
   */
  @Before
  public void startServer() throws Exception {
    TomcatInstall install = (TomcatInstall) getInstall();
    // List of all the jars for tomcat to put on the server classpath
    String libDirJars = install.getHome() + "/lib/*";
    String binDirJars = install.getHome() + "/bin/*";

    // Set server name based on the test about to be run
    serverName = getClass().getSimpleName().concat("_").concat(getTestMethodName());

    // Create command string for starting server
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_SERVER);
    command.addOption(CliStrings.START_SERVER__NAME, serverName);
    command.addOption(CliStrings.START_SERVER__SERVER_PORT, "0");
    // Add Tomcat jars to server classpath
    command.addOption(CliStrings.START_SERVER__CLASSPATH,
        binDirJars + File.pathSeparator + libDirJars);
    command.addOption(CliStrings.START_SERVER__LOCATORS, DUnitEnv.get().getLocatorString());

    // Start server
    gfsh.executeAndVerifyCommand(command.toString());
  }

  /**
   * Stops the server for the client Tomcat container is has been connecting to
   */
  @After
  public void stopServer() throws Exception {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STOP_SERVER);
    command.addOption(CliStrings.STOP_SERVER__DIR, serverName);
    gfsh.executeAndVerifyCommand(command.toString());
  }
}
