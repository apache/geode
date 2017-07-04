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
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.codehaus.cargo.container.ContainerType;
import org.codehaus.cargo.container.InstalledLocalContainer;
import org.codehaus.cargo.container.State;
import org.codehaus.cargo.container.configuration.ConfigurationType;
import org.codehaus.cargo.container.configuration.LocalConfiguration;
import org.codehaus.cargo.container.deployable.WAR;
import org.codehaus.cargo.container.property.GeneralPropertySet;
import org.codehaus.cargo.container.property.ServletPropertySet;
import org.codehaus.cargo.container.tomcat.TomcatPropertySet;
import org.codehaus.cargo.generic.DefaultContainerFactory;
import org.codehaus.cargo.generic.configuration.DefaultConfigurationFactory;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.logging.LogService;

/**
 * Manages multiple J2EE containers using cargo.
 *
 * Provides methods to start and stop J2EE containers and obtain the http ports those containers are
 * listening on.
 */
public class ContainerManager {
  private static final Logger logger = LogService.getLogger();

  private ArrayList<InstalledLocalContainer> containers;
  private ArrayList<ContainerInstall> installs;

  private String testName;

  public ContainerManager() {
    containers = new ArrayList<>();
    installs = new ArrayList<>();
  }

  /**
   * Set the name of the current test
   *
   * Used for debugging so that log files can be easily identified
   */
  public void setTestName(String testName) {
    this.testName = testName;
  }

  /**
   * Add a new container to manage using the specified installation
   *
   * The container will not be running until one of the start methods is called.
   */
  public InstalledLocalContainer addContainer(ContainerInstall install) throws IOException {
    return addContainer(install, containers.size());
  }

  /**
   * Add multiple containers to manage using the specified installation.
   *
   * The containers will not be running until one of the start methods is called.
   */
  public void addContainers(int numContainers, ContainerInstall install) throws IOException {
    for (int i = 0; i < numContainers; i++)
      addContainer(install);
  }

  /**
   * Return the http port the given container is listening on, if the container is running
   * 
   * @throws IllegalStateException if the container is not running.
   */
  public String getContainerPort(int index) {
    return getContainerPort(getContainer(index));
  }

  private String getContainerPort(InstalledLocalContainer container) {
    LocalConfiguration config = container.getConfiguration();
    config.applyPortOffset();

    if (!container.getState().isStarted()) {
      throw new IllegalStateException("Port has not yet been assigned to container");
    }
    return config.getPropertyValue(ServletPropertySet.PORT);
  }

  /**
   * @return the number of containers managed
   */
  public int numContainers() {
    return containers.size();
  }

  public ArrayList<Integer> getContainerIndexesWithState(String state) {
    ArrayList<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < numContainers(); i++) {
      if (state.equals(State.STARTED.toString()) || state.equals(State.STOPPED.toString())
          || state.equals(State.STARTED.toString()) || state.equals(State.STOPPING.toString())
          || state.equals(State.UNKNOWN.toString())) {
        if (getContainer(i).getState().toString().equals(state))
          indexes.add(i);
      } else
        throw new IllegalArgumentException(
            "State must be one of the 5 specified cargo state strings (stopped, started, starting, stopping, or unknown). Given: "
                + state);
    }
    return indexes;
  }

  /**
   * Return the cargo container of all of the containers in the given state
   */
  public ArrayList<InstalledLocalContainer> getContainersWithState(String state) {
    ArrayList<InstalledLocalContainer> statedContainers = new ArrayList<>();
    for (int index : getContainerIndexesWithState(state))
      statedContainers.add(getContainer(index));
    return statedContainers;
  }

  private ArrayList<Integer> getInactiveContainerIndexes() {
    ArrayList<Integer> indexes = getContainerIndexesWithState(State.STOPPED.toString());
    indexes.addAll(getContainerIndexesWithState(State.UNKNOWN.toString()));
    return indexes;
  }

  private ArrayList<InstalledLocalContainer> getInactiveContainers() {
    ArrayList<InstalledLocalContainer> inactiveContainers =
        getContainersWithState(State.STOPPED.toString());
    inactiveContainers.addAll(getContainersWithState(State.UNKNOWN.toString()));
    return inactiveContainers;
  }

  private ArrayList<Integer> getActiveContainerIndexes() {
    return getContainerIndexesWithState(State.STARTED.toString());
  }

  public ArrayList<InstalledLocalContainer> getActiveContainers() {
    return getContainersWithState(State.STARTED.toString());
  }

  public InstalledLocalContainer getContainer(int index) {
    return containers.get(index);
  }

  public ContainerInstall getContainerInstall(int index) {
    return installs.get(index);
  }

  /**
   * Get a textual description of the given container.
   */
  public String getContainerDescription(int index) {
    return getContainerDescription(getContainer(index), getContainerInstall(index)) + " (" + index
        + ")";
  }

  private String getContainerDescription(InstalledLocalContainer container,
      ContainerInstall install) {
    String port = "<" + container.getState().toString() + ">";
    try {
      port = String.valueOf(getContainerPort(container));
    } catch (IllegalStateException ise) {
    }

    return install.getContainerDescription() + ":" + port;
  }

  /**
   * Start the given container
   */
  public void startContainer(int index) {
    InstalledLocalContainer container = getContainer(index);
    ContainerInstall install = getContainerInstall(index);
    String containerDescription = getContainerDescription(index);

    String logFilePath =
        new File("cargo_logs/containers/" + getUniqueContainerDescription(index) + ".log")
            .getAbsolutePath();
    container.setOutput(logFilePath);
    logger.info("Sending log file output to " + logFilePath);

    if (!container.getState().isStarted()) {
      logger.info("Starting container " + containerDescription);
      int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
      container.getConfiguration().setProperty(ServletPropertySet.PORT, Integer.toString(ports[0]));
      container.getConfiguration().setProperty(GeneralPropertySet.RMI_PORT,
          Integer.toString(ports[1]));
      container.getConfiguration().setProperty(TomcatPropertySet.AJP_PORT,
          Integer.toString(ports[2]));
      container.getConfiguration().setProperty(GeneralPropertySet.PORT_OFFSET, "0");

      try {
        install.writeProperties();
        container.start();
      } catch (Exception e) {
        throw new RuntimeException(
            "Something very bad happened to this container when starting. Check the cargo_logs folder for container logs.",
            e);
      }
      logger.info("Started container " + containerDescription);
    } else {
      throw new IllegalArgumentException("Cannot start container " + containerDescription
          + " its current state is " + container.getState());
    }
  }

  /**
   * Stop the given container
   */
  public void stopContainer(int index) {
    InstalledLocalContainer container = getContainer(index);
    if (container.getState().isStarted()) {
      logger.info("Stopping container" + index + " " + getContainerDescription(index));
      container.stop();
      logger.info("Stopped container" + index + " " + getContainerDescription(index));
    } else
      throw new IllegalArgumentException("Cannot stop container " + getContainerDescription(index)
          + " it is currently " + container.getState());
  }

  public void startContainers(ArrayList<Integer> indexes) {
    for (int index : indexes)
      startContainer(index);
  }

  public void stopContainers(ArrayList<Integer> indexes) {
    for (int index : indexes)
      stopContainer(index);
  }

  /**
   * Start all containers that are not currently running.
   */
  public void startAllInactiveContainers() {
    startContainers(getInactiveContainerIndexes());
  }

  /**
   * Stop all containers that are currently running.
   */
  public void stopAllActiveContainers() {
    stopContainers(getActiveContainerIndexes());
  }

  public void removeContainer(int index) {
    stopContainer(index);
    containers.remove(index);
    installs.remove(index);
  }

  /**
   * Runs {@link #clean} on all containers
   */
  public void cleanUp() throws IOException {
    for (int i = 0; i < numContainers(); i++)
      clean(i);
  }

  /**
   * Deletes the configuration directory for the specified container
   */
  private void clean(int index) throws IOException {
    ContainerInstall install = getContainerInstall(index);

    String baseLogFilePath = new File("cargo_logs").getAbsolutePath();
    String configLogFolderPath = baseLogFilePath + "/configs/";

    File configDir = new File(getContainer(index).getConfiguration().getHome());
    File configLogDir = new File(configLogFolderPath + configDir.getName());

    if (configDir.exists()) {
      configLogDir.mkdirs();

      logger.info("Configuration in " + configDir.getAbsolutePath());
      FileUtils.copyDirectory(configDir, configLogDir);
      logger.info("Copied configuration to " + configLogDir.getAbsolutePath());
      logger.info("Deleting configuration folder " + configDir.getAbsolutePath());
      FileUtils.deleteDirectory(configDir);
    }
  }

  private String getUniqueContainerDescription(int index) {
    return getUniqueContainerDescription(index, getContainerInstall(index));
  }

  /**
   * Get a human readable unique container description for container storage.
   *
   * Unique descriptions currently are generated by joining
   * {@link ContainerInstall#getContainerDescription()}, the index passed in, {@link #testName}, and
   * the {@link System#nanoTime()} with '_' characters
   */
  private String getUniqueContainerDescription(int index, ContainerInstall install) {
    return String.join("_", Arrays.asList(install.getContainerDescription(),
        Integer.toString(index), testName, Long.toString(System.nanoTime())));
  }

  /**
   * Create a container to manage, given an installation.
   */
  private InstalledLocalContainer addContainer(ContainerInstall install, int index)
      throws IOException {
    String uniqueName = getUniqueContainerDescription(index, install);

    // Create the Cargo Container instance wrapping our physical container
    LocalConfiguration configuration = (LocalConfiguration) new DefaultConfigurationFactory()
        .createConfiguration(install.getContainerId(), ContainerType.INSTALLED,
            ConfigurationType.STANDALONE, "/tmp/cargo_configs/" + uniqueName);
    configuration.setProperty(GeneralPropertySet.LOGGING, install.getLoggingLevel());

    install.modifyConfiguration(configuration);

    File gemfireLogFile = new File("cargo_logs/gemfire_modules/" + uniqueName);
    gemfireLogFile.getParentFile().mkdirs();
    install.setSystemProperty("log-file", gemfireLogFile.getAbsolutePath());
    logger.info("Gemfire logs in " + gemfireLogFile.getAbsolutePath());

    // Removes secureRandom generation so that container startup is much faster
    configuration.setProperty(GeneralPropertySet.JVMARGS,
        "-Djava.security.egd=file:/dev/./urandom");

    // Statically deploy WAR file for servlet
    WAR war = install.getDeployableWAR();
    war.setContext("");
    configuration.addDeployable(war);
    logger.info("Deployed WAR file at " + war.getFile());

    // Create the container, set it's home dir to where it was installed, and set the its output log
    InstalledLocalContainer container = (InstalledLocalContainer) (new DefaultContainerFactory())
        .createContainer(install.getContainerId(), ContainerType.INSTALLED, configuration);

    container.setHome(install.getInstallPath());

    containers.add(index, container);
    installs.add(index, install);

    logger.info("Setup container " + getContainerDescription(index));
    return container;
  }
}
