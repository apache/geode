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

import static org.apache.geode.session.tests.ContainerInstall.TMP_DIR;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.Logger;
import org.codehaus.cargo.container.ContainerType;
import org.codehaus.cargo.container.InstalledLocalContainer;
import org.codehaus.cargo.container.State;
import org.codehaus.cargo.container.configuration.ConfigurationType;
import org.codehaus.cargo.container.configuration.LocalConfiguration;
import org.codehaus.cargo.container.deployable.WAR;
import org.codehaus.cargo.container.property.GeneralPropertySet;
import org.codehaus.cargo.container.property.LoggingLevel;
import org.codehaus.cargo.container.property.ServletPropertySet;
import org.codehaus.cargo.container.tomcat.TomcatPropertySet;
import org.codehaus.cargo.generic.DefaultContainerFactory;
import org.codehaus.cargo.generic.configuration.DefaultConfigurationFactory;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.logging.LogService;

/**
 * Base class for handling the setup and configuration of cargo containers
 *
 * This class contains common logic for setting up and configuring cargo containers for J2EE
 * container installations. Also includes some common methods for applying geode session replication
 * configuration to those containers.
 *
 * Subclasses provide setup and configuration of specific containers.
 */
public abstract class ServerContainer {
  private final File containerConfigHome;
  private InstalledLocalContainer container;
  private ContainerInstall install;

  private String locatorAddress;
  private int locatorPort;
  private File warFile;

  public String description;
  public File gemfireLogFile;
  public File cacheXMLFile;
  public File logDir;

  public String loggingLevel;

  public HashMap<String, String> cacheProperties;
  public HashMap<String, String> systemProperties;

  public final String DEFAULT_CONF_DIR;

  public static final String DEFAULT_LOGGING_LEVEL = LoggingLevel.LOW.getLevel();
  public static final String DEFAULT_LOG_DIR = "cargo_logs/";
  public static final String DEFAULT_CONFIG_DIR = TMP_DIR + "/cargo_configs/";

  public static final Logger logger = LogService.getLogger();

  /**
   * Sets up the container using the given installation
   *
   * Sets up a bunch of logging files, default locations, and container properties.
   *
   * Creates a whole new cargo configuration and cargo container for the {@link #container}
   * variable.
   *
   * @param containerConfigHome The folder that the container configuration folder should be setup
   *        in
   * @param containerDescriptors A string of extra descriptors for the container used in the
   *        containers {@link #description}
   */
  public ServerContainer(ContainerInstall install, File containerConfigHome,
      String containerDescriptors) throws IOException {
    this.install = install;
    // Get a container description for logging and output
    description = generateUniqueContainerDescription(containerDescriptors);
    // Setup logging
    loggingLevel = DEFAULT_LOGGING_LEVEL;
    logDir = new File(DEFAULT_LOG_DIR + description);
    logDir.mkdirs();

    logger.info("Creating new container " + description);

    DEFAULT_CONF_DIR = install.getHome() + "/conf/";
    // Use the default configuration home path if not passed a config home
    this.containerConfigHome = containerConfigHome == null
        ? new File(DEFAULT_CONFIG_DIR + description) : containerConfigHome;

    // Init the property lists
    cacheProperties = new HashMap<>();
    systemProperties = new HashMap<>();
    // Set WAR file to session testing war
    warFile = new File(install.getWarFilePath());

    // Create the Cargo Container instance wrapping our physical container
    LocalConfiguration configuration = (LocalConfiguration) new DefaultConfigurationFactory()
        .createConfiguration(install.getInstallId(), ContainerType.INSTALLED,
            ConfigurationType.STANDALONE, this.containerConfigHome.getAbsolutePath());
    // Set configuration/container logging level
    configuration.setProperty(GeneralPropertySet.LOGGING, loggingLevel);
    // Removes secureRandom generation so that container startup is much faster
    configuration.setProperty(GeneralPropertySet.JVMARGS,
        "-Djava.security.egd=file:/dev/./urandom");

    // Setup the gemfire log file for this container
    gemfireLogFile = new File(logDir.getAbsolutePath() + "/gemfire.log");
    gemfireLogFile.getParentFile().mkdirs();
    setSystemProperty("log-file", gemfireLogFile.getAbsolutePath());

    logger.info("Gemfire logs can be found in " + gemfireLogFile.getAbsolutePath());

    // Create the container
    container = (InstalledLocalContainer) (new DefaultContainerFactory())
        .createContainer(install.getInstallId(), ContainerType.INSTALLED, configuration);
    // Set container's home dir to where it was installed
    container.setHome(install.getHome());
    // Set container output log to directory setup for it
    container.setOutput(logDir.getAbsolutePath() + "/container.log");

    // Set cacheXML file
    File installXMLFile = install.getCacheXMLFile();
    // Sets the cacheXMLFile variable and adds the cache XML file server system property map
    setCacheXMLFile(new File(logDir.getAbsolutePath() + "/" + installXMLFile.getName()));
    // Copy the cacheXML file to a new, unique location for this container
    FileUtils.copyFile(installXMLFile, cacheXMLFile);
  }

  /**
   * Generates a unique, mostly human readable, description string of the container using the
   * installation's description, extraIdentifiers, and the current system nano time
   */
  public String generateUniqueContainerDescription(String extraIdentifiers) {
    return String.join("_", Arrays.asList(install.getInstallDescription(), extraIdentifiers,
        Long.toString(System.nanoTime())));
  }

  /**
   * Deploys the {@link #warFile} to the cargo container ({@link #container}).
   */
  public void deployWar() {
    // Get the cargo war from the war file
    WAR war = new WAR(warFile.getAbsolutePath());
    // Set context access to nothing
    war.setContext("");
    // Deploy the war the container's configuration
    getConfiguration().addDeployable(war);

    logger.info("Deployed WAR file at " + war.getFile());
  }

  /**
   * Starts this cargo container by picking the container's ports (RMI, AJP, and regular) and
   * calling the cargo container's start function
   */
  public void start() {
    if (container.getState().isStarted())
      throw new IllegalArgumentException("Container " + description
          + " failed to start because it is currently " + container.getState());

    LocalConfiguration config = getConfiguration();
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(4);
    // Set container ports from available ports
    config.setProperty(ServletPropertySet.PORT, Integer.toString(ports[0]));
    config.setProperty(GeneralPropertySet.RMI_PORT, Integer.toString(ports[1]));
    config.setProperty(TomcatPropertySet.AJP_PORT, Integer.toString(ports[2]));
    config.setProperty(GeneralPropertySet.PORT_OFFSET, "0");
    String jvmArgs = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=" + ports[3];
    if (SystemUtils.isJavaVersionAtLeast(900)) {
      jvmArgs += " --add-opens java.base/java.lang.module=ALL-UNNAMED" +
          " --add-opens java.base/jdk.internal.module=ALL-UNNAMED" +
          " --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED" +
          " --add-opens java.base/jdk.internal.misc=ALL-UNNAMED" +
          " --add-opens java.base/jdk.internal.ref=ALL-UNNAMED" +
          " --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED";
    }
    config.setProperty(GeneralPropertySet.START_JVMARGS, jvmArgs);
    container.setConfiguration(config);


    try {
      logger.info("Starting container " + description + "RMI Port: " + ports[3]);
      // Writes settings to the expected form (either XML or WAR file)
      writeSettings();
      // Start the container through cargo
      container.start();
    } catch (Exception e) {
      throw new RuntimeException(
          "Something very bad happened to this container when starting. Check the cargo_logs folder for container logs.",
          e);
    }
  }

  /**
   * Stops this cargo container
   */
  public void stop() {
    if (!container.getState().isStarted()) {
      throw new IllegalArgumentException("Container " + description
          + " failed to stop because it is currently " + container.getState());
    }

    container.stop();
  }

  public void dumpLogs() throws IOException {
    for (File file : logDir.listFiles()) {
      dumpToStdOut(file);
    }

    for (File file : new File(containerConfigHome, "logs").listFiles()) {
      dumpToStdOut(file);
    }
  }

  private void dumpToStdOut(final File file) throws IOException {
    System.out.println("-------------------------------------------");
    System.out.println(file.getAbsolutePath());
    System.out.println("-------------------------------------------");
    FileUtils.copyFile(file, System.out);
    System.out.println("-------------------------------------------");
    System.out.println("");
  }

  /**
   * Copies the container configuration (found through {@link #getConfiguration()}) to the logging
   * directory specified by {@link #logDir}
   */
  public void cleanUp() throws IOException {
    File configDir = new File(getConfiguration().getHome());

    if (configDir.exists()) {
      logger.info("Deleting configuration folder " + configDir.getAbsolutePath());
      FileUtils.deleteDirectory(configDir);
    }
  }

  /**
   * Sets the container's locator
   *
   * Sets the two variables {@link #locatorAddress} and {@link #locatorPort}. Also calls the
   * {@link #updateLocator()} function to write the updated locator properties to the file.
   */
  public void setLocator(String address, int port) throws IOException {
    locatorAddress = address;
    locatorPort = port;
    updateLocator();
  }

  /**
   * Sets the container's cache XML file
   */
  public void setCacheXMLFile(File cacheXMLFile) throws IOException {
    setSystemProperty("cache-xml-file", cacheXMLFile.getAbsolutePath());
    this.cacheXMLFile = cacheXMLFile;
  }

  /**
   * Set a geode session replication property
   */
  public String setCacheProperty(String name, String value) throws IOException {
    return cacheProperties.put(name, value);
  }

  /**
   * Set geode distributed system property
   */
  public String setSystemProperty(String name, String value) throws IOException {
    return systemProperties.put(name, value);
  }

  /**
   * Sets the war file for this container to deploy and use
   */
  public void setWarFile(File warFile) {
    this.warFile = warFile;
  }

  /**
   * set the container's logging level
   */
  public void setLoggingLevel(String loggingLevel) {
    this.loggingLevel = loggingLevel;

    LocalConfiguration config = getConfiguration();
    config.setProperty(GeneralPropertySet.LOGGING, loggingLevel);
    container.setConfiguration(config);
  }

  public InstalledLocalContainer getContainer() {
    return container;
  }

  public ContainerInstall getInstall() {
    return install;
  }

  public File getWarFile() {
    return warFile;
  }

  public String getLoggingLevel() {
    return loggingLevel;
  }

  public LocalConfiguration getConfiguration() {
    return container.getConfiguration();
  }

  public State getState() {
    return container.getState();
  }

  public String getCacheProperty(String name) {
    return cacheProperties.get(name);
  }

  public String getSystemProperty(String name) {
    return systemProperties.get(name);
  }

  /**
   * Get the RMI port for the container
   *
   * Calls {@link #getPort()} with the {@link GeneralPropertySet#RMI_PORT} option.
   */
  public String getRMIPort() {
    return getPort(GeneralPropertySet.RMI_PORT);
  }

  /**
   * Get the basic port for the container
   *
   * Calls {@link #getPort()} with the {@link ServletPropertySet#PORT} option.
   */
  public String getPort() {
    return getPort(ServletPropertySet.PORT);
  }

  /**
   * The container's port for the specified port type
   */
  public String getPort(String portType) {
    LocalConfiguration config = getConfiguration();
    config.applyPortOffset();

    if (!container.getState().isStarted())
      throw new IllegalStateException(
          "Container is not started, thus a port has not yet been assigned to the container.");

    return config.getPropertyValue(portType);
  }

  /**
   * Called before each container startup
   *
   * This is mainly used to write properties to whatever format they need to be in for a given
   * container before the container is started. The reason for doing this is to make sure that
   * expensive property updates (such as writing to an XML file or building WAR files from the
   * command line) only happen as often as they are needed. These kinds of updates usually only need
   * to happen on container startup.
   */
  public abstract void writeSettings() throws Exception;

  /**
   * Human readable description of the container
   *
   * @return The {@link #description} variable along with the state of this {@link #container}
   */
  @Override
  public String toString() {
    return description + "_<" + container.getState() + ">";
  }

  /**
   * Updates the address and port of the locator for this container
   *
   * For Client Server installations the {@link #cacheXMLFile} is updated with the new address and
   * port. For Peer to Peer installations the locator must be specified as a system property and so
   * is added to the {@link #systemProperties} map under the 'locators' key in the form of
   * '{@link #locatorAddress}[{@link #locatorPort}]'.
   */
  private void updateLocator() throws IOException {
    if (getInstall().isClientServer()) {
      HashMap<String, String> attributes = new HashMap<>();
      attributes.put("host", locatorAddress);
      attributes.put("port", Integer.toString(locatorPort));

      ContainerInstall.editXMLFile(getSystemProperty("cache-xml-file"), "locator", "pool",
          attributes, true);
    } else {
      setSystemProperty("locators", locatorAddress + "[" + locatorPort + "]");
    }
  }
}
