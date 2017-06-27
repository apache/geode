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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.codehaus.cargo.container.deployable.WAR;
import org.junit.Assume;

/**
 * Container install for a generic app server
 *
 * Extends {@link ContainerInstall} to form a basic installer which downloads and sets up a
 * container installation. Currently being used solely for Jetty 9 installation.
 *
 * This install modifies the session testing war using the modify_war_file script, so that it uses
 * the geode session replication for generic application servers. That also means that tests using
 * this install will only run on linux.
 *
 * In theory, adding support for additional containers should just be a matter of adding new
 * elements to the {@link Server} enumeration, since this install does not modify the container in
 * any way.
 */
public class GenericAppServerInstall extends ContainerInstall {

  /**
   * Get the download URL of a generic app server using hardcoded keywords
   *
   * Currently the only supported keyword instance is JETTY9
   */
  public enum Server {
    JETTY9(
        "http://central.maven.org/maven2/org/eclipse/jetty/jetty-distribution/9.4.5.v20170502/jetty-distribution-9.4.5.v20170502.zip",
        "jetty9x");

    private String downloadURL;
    private String containerId;

    Server(String downloadURL, String containerId) {
      this.downloadURL = downloadURL;
      this.containerId = containerId;
    }

    public String getDownloadURL() {
      return downloadURL;
    }

    public String getContainerId() {
      return containerId;
    }
  }

  /**
   * Represent the type of cache being used in this install
   *
   * Supports PEER_TO_PEER or CLIENT_SERVER. Also contains several useful helper functions
   * containing hardcoded values needed for the two different types of caches.
   */
  public enum CacheType {
    PEER_TO_PEER("peer-to-peer", "cache-peer.xml"),
    CLIENT_SERVER("client-server", "cache-client.xml");

    private final String commandLineTypeString;
    private final String XMLTypeFile;

    CacheType(String commandLineTypeString, String XMLTypeFile) {
      this.commandLineTypeString = commandLineTypeString;
      this.XMLTypeFile = XMLTypeFile;
    }

    public String getCommandLineTypeString() {
      return commandLineTypeString;
    }

    public String getXMLTypeFile() {
      return XMLTypeFile;
    }
  }

  private File warFile;
  private CacheType cacheType;
  private Server server;

  private final String appServerModulePath;

  public GenericAppServerInstall(Server server) throws IOException, InterruptedException {
    this(server, CacheType.PEER_TO_PEER, DEFAULT_INSTALL_DIR);
  }

  public GenericAppServerInstall(Server server, String installDir)
      throws IOException, InterruptedException {
    this(server, CacheType.PEER_TO_PEER, installDir);
  }

  public GenericAppServerInstall(Server server, CacheType cacheType)
      throws IOException, InterruptedException {
    this(server, cacheType, DEFAULT_INSTALL_DIR);
  }

  /**
   * Download and setup container installation
   *
   * Finds the path to (and extracts) the appserver module located within GEODE_BUILD_HOME
   * directory. If cache is Client Server then also builds WAR file.
   */
  public GenericAppServerInstall(Server server, CacheType cacheType, String installDir)
      throws IOException, InterruptedException {
    super(installDir, server.getDownloadURL());

    // Ignore tests that are running on windows, since they can't run the modify war script
    Assume.assumeFalse(System.getProperty("os.name").toLowerCase().contains("win"));
    this.server = server;
    this.cacheType = cacheType;

    appServerModulePath = findAndExtractModule(GEODE_BUILD_HOME, "appserver");
    // Default properties
    setCacheProperty("enable_local_cache", "false");

    warFile = File.createTempFile("session-testing", ".war", new File("/tmp"));
    warFile.deleteOnExit();
  }

  /**
   * Sets the XML file to use for cache settings
   *
   * Calls {@link ContainerInstall#setCacheXMLFile(String, String)} with the default original cache
   * file located in the conf folder of the {@link #appServerModulePath} and the given
   * newXMLFilePath.
   */
  @Override
  public void setupCacheXMLFile(String newXMLFilePath) throws IOException {
    super.setCacheXMLFile(appServerModulePath + "/conf/" + cacheType.getXMLTypeFile(),
        newXMLFilePath);
  }

  /**
   * Build the command list used to modify/build the WAR file
   */
  private List<String> buildCommand() throws IOException {
    String unmodifiedWar = findSessionTestingWar();
    String modifyWarScript = appServerModulePath + "/bin/modify_war";
    new File(modifyWarScript).setExecutable(true);

    List<String> command = new ArrayList<>();
    command.add(modifyWarScript);
    command.add("-w");
    command.add(unmodifiedWar);
    command.add("-t");
    command.add(cacheType.getCommandLineTypeString());
    command.add("-o");
    command.add(warFile.getAbsolutePath());
    for (String property : cacheProperties.keySet()) {
      command.add("-p");
      command.add("gemfire.cache." + property + "=" + getCacheProperty(property));
    }
    for (String property : systemProperties.keySet()) {
      command.add("-p");
      command.add("gemfire.property." + property + "=" + getSystemProperty(property));
    }

    return command;
  }

  /**
   * Modifies the WAR file for container use, by simulating a command line execution of the
   * modify_war_file script using the commands built from {@link #buildCommand()}
   */
  private void modifyWarFile() throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    builder.environment().put("GEODE", GEODE_BUILD_HOME);
    builder.inheritIO();

    builder.command(buildCommand());
    logger.info("Running command: " + String.join(" ", builder.command()));

    Process process = builder.start();

    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IOException("Unable to run modify_war script: " + builder.command());
    }
  }

  /**
   * AppServer specific property updater
   *
   * Overrides {@link ContainerInstall#writeProperties}. Since most properties for an app server can
   * be specified through flags when running the modify_war script this runs
   * {@link #modifyWarFile()}.
   */
  @Override
  public void writeProperties() throws Exception {
    modifyWarFile();
  }

  /**
   * @see ContainerInstall#getContainerId()
   */
  @Override
  public String getContainerId() {
    return server.getContainerId();
  }

  /**
   * @see ContainerInstall#getContainerDescription()
   */
  @Override
  public String getContainerDescription() {
    return server.name() + "_" + cacheType.name();
  }

  /**
   * Sets the locator for this container
   *
   * If the cache is P2P the WAR file must be regenerated to take a new locator. Otherwise (if
   * Client Server) the cache xml file will be edited.
   */
  @Override
  public void setLocator(String address, int port) throws Exception {
    if (cacheType == CacheType.PEER_TO_PEER) {
      setSystemProperty("locators", address + "[" + port + "]");
    } else {
      HashMap<String, String> attributes = new HashMap<>();
      attributes.put("host", address);
      attributes.put("port", Integer.toString(port));

      editXMLFile(appServerModulePath + "/conf/" + cacheType.getXMLTypeFile(), "locator", "pool",
          attributes, true);
    }

    logger.info("Set locator for AppServer install to " + address + "[" + port + "]");
  }

  /**
   * @see ContainerInstall#getDeployableWAR()
   */
  @Override
  public WAR getDeployableWAR() {
    return new WAR(warFile.getAbsolutePath());
  }
}
