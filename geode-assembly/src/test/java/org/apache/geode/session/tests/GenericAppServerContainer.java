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

import static org.apache.geode.session.tests.ContainerInstall.GEODE_BUILD_HOME;
import static org.apache.geode.session.tests.ContainerInstall.TMP_DIR;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;

/**
 * Container for a generic app server
 *
 * Extends {@link ServerContainer} to form a basic container which sets up a GenericAppServer
 * container. Currently being used solely for Jetty 9 containers.
 *
 * The container modifies a copy of the session testing war using the modify_war_file script in
 * order to properly implement geode session replication for generic application servers. That means
 * that tests using this container will only run on linux.
 *
 * In theory, adding support for additional containers should just be a matter of changing the
 * {@link GenericAppServerInstall} to support other installations, since this container does not
 * depend upon the type of appserver in any way.
 */
public class GenericAppServerContainer extends ServerContainer {
  private final File modifyWarScript;
  private final File modifyWarScriptLog;

  private static final String DEFAULT_GENERIC_APPSERVER_WAR_DIR = TMP_DIR + "/cargo_wars/";

  /**
   * Setup the generic appserver container
   *
   * Sets up a configuration for the container using the specified installation and configuration
   * home. Finds the script needed to modify the war file, sets up the new WAR file to modify by
   * creating a temporary WAR file to use, deploys the war to the Cargo container, and sets various
   * container properties (i.e. locator, local cache, etc.)
   */
  public GenericAppServerContainer(GenericAppServerInstall install, File containerConfigHome,
      String containerDescriptors) throws IOException {
    super(install, containerConfigHome, containerDescriptors);

    // Setup modify war script file so that it is executable and easily findable
    modifyWarScript = new File(install.getModulePath() + "/bin/modify_war");
    modifyWarScript.setExecutable(true);

    // Setup modify_war script logging file
    modifyWarScriptLog = new File(logDir + "/warScript.log");
    modifyWarScriptLog.createNewFile();

    // Ignore tests that are running on windows, since they can't run the modify war script
    Assume.assumeFalse(System.getProperty("os.name").toLowerCase().contains("win"));

    // Create temp war file to use
    File warDir = new File(DEFAULT_GENERIC_APPSERVER_WAR_DIR);
    warDir.mkdirs();
    setWarFile(File.createTempFile(description, ".war", warDir));

    // Deploy war file to container configuration
    deployWar();
    // Setup the default installations locators
    setLocator(install.getDefaultLocatorAddress(), install.getDefaultLocatorPort());

    // Make sure that local caches are disabled by default
    setCacheProperty("enable_local_cache",
        Boolean.toString(install.getConnectionType().enableLocalCache()));
  }

  /**
   * Builds the command needed to run the {@link #modifyWarScript}
   *
   * The command is built as an array list of strings with each element representing a string
   * separated by a space on the command line. For example, the list {'-t', 'geode'} would represent
   * 'modify_war -t geode' on the command line.
   *
   * The command built points towards the {@link ContainerInstall#getWarFilePath()} as the starting
   * file and the {@link #warFile} as the output file. Cache and system properties are specified
   * when modifying the WAR, so the elements contained within the {@link #cacheProperties} and
   * {@link #systemProperties} maps are also added to the command built.
   */
  private List<String> buildCommand() throws IOException {
    ContainerInstall install = getInstall();

    // Start command list
    List<String> command = new ArrayList<>();
    // Path to the modify war script to run
    command.add(modifyWarScript.getAbsolutePath());
    // Path to the WAR file to modify
    command.add("-w");
    command.add(install.getWarFilePath());
    // Get connection type for the WAR (peer-to-peer or client-server)
    command.add("-t");
    command.add(install.getConnectionType().getName());
    // Path to the modified version of the origin WAR file
    command.add("-o");
    command.add(getWarFile().getAbsolutePath());
    // Add all the cache properties setup to the WAR file
    for (String property : cacheProperties.keySet()) {
      command.add("-p");
      command.add("gemfire.cache." + property + "=" + getCacheProperty(property));
    }
    // Add all the system properties to the WAR file
    for (String property : systemProperties.keySet()) {
      command.add("-p");
      command.add("gemfire.property." + property + "=" + getSystemProperty(property));
    }

    return command;
  }

  /**
   * Modifies the {@link ContainerInstall#getWarFilePath()} for container use, by simulating a
   * command line execution of the modify_war_file script using the commands built from
   * {@link #buildCommand()}
   *
   * The modified WAR file is sent to {@link #warFile}.
   *
   * @throws IOException If the command executed returns with a non-zero exit code.
   */
  private void modifyWarFile() throws IOException, InterruptedException {
    // Build the environment to run the command
    ProcessBuilder builder = new ProcessBuilder();
    builder.environment().put("GEODE", GEODE_BUILD_HOME);
    builder.inheritIO();
    // Setup the environment builder with the command
    builder.command(buildCommand());
    // Redirect the command line logging to a file
    builder.redirectError(modifyWarScriptLog);
    builder.redirectOutput(modifyWarScriptLog);
    logger.info("Running command: " + String.join(" ", builder.command()));

    // Run the command
    Process process = builder.start();

    // Wait for the command to finish
    int exitCode = process.waitFor();
    // Throw error if bad exit
    if (exitCode != 0) {
      throw new IOException("Unable to run modify_war script: " + builder.command());
    }
  }

  /**
   * Update the container's settings by calling {@link #modifyWarFile()} method
   */
  @Override
  public void writeSettings() throws Exception {
    modifyWarFile();
  }
}
