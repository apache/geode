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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.logging.log4j.Logger;
import org.codehaus.cargo.container.State;

import org.apache.geode.internal.logging.LogService;

/**
 * Manages multiple J2EE containers using cargo.
 *
 * Provides methods to start and stop J2EE containers and obtain the http ports those containers are
 * listening on.
 */
public class ContainerManager {
  private static final Logger logger = LogService.getLogger();
  private String testName;

  private ArrayList<ServerContainer> containers;

  public ContainerManager() {
    containers = new ArrayList<>();
  }

  /**
   * @return the number of containers managed
   */
  public int numContainers() {
    return containers.size();
  }

  public void cleanUp() throws IOException {
    for (int i = 0; i < numContainers(); i++)
      getContainer(i).cleanUp();
  }

  /**
   * Add a new container to manage using the specified installation
   *
   * The container will not be running until one of the start methods is called.
   */
  public ServerContainer addContainer(ContainerInstall install) throws IOException {
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
   * Start the given container
   */
  public void startContainer(int index) {
    try {
      getContainer(index).start();
    } catch (Exception e) {
      throw new RuntimeException("Something very bad happened when trying to start container "
          + getContainerDescription(index), e);
    }

    logger.info("Started container " + getContainerDescription(index));
  }

  /**
   * Start all containers specified by the given indexes
   */
  public void startContainers(ArrayList<Integer> indexes) {
    for (int index : indexes)
      startContainer(index);
  }

  /**
   * Start all containers that are not currently running.
   */
  public void startAllInactiveContainers() {
    startContainers(getInactiveContainerIndexes());
  }

  /**
   * Stop the given container
   */
  public void stopContainer(int index) {
    getContainer(index).stop();

    logger.info("Stopped container " + getContainerDescription(index));
  }

  /**
   * Stop all containers specified by the given indexes
   */
  public void stopContainers(ArrayList<Integer> indexes) {
    for (int index : indexes)
      stopContainer(index);
  }

  /**
   * Stop all containers that are currently running.
   */
  public void stopAllActiveContainers() {
    stopContainers(getActiveContainerIndexes());
  }

  public void dumpLogs() throws IOException {
    for (ServerContainer container : containers) {
      container.dumpLogs();
    }
  }

  /**
   * Set the name of the current test
   *
   * Used for debugging so that log files can be easily identified.
   */
  public void setTestName(String testName) {
    this.testName = testName;
  }

  /**
   * Get the positions of the containers with the given container state
   *
   * @param state A string representing the Cargo state a container is in. The possible states can
   *        be found in as static variables in the {@link State} class.
   */
  public ArrayList<Integer> getContainerIndexesWithState(String state) {
    if (!(state.equals(State.STARTED.toString()) || state.equals(State.STOPPED.toString())
        || state.equals(State.STARTING.toString()) || state.equals(State.STOPPING.toString())
        || state.equals(State.UNKNOWN.toString()))) {
      throw new IllegalArgumentException(
          "State must be one of the 5 specified cargo state strings (stopped, started, starting, stopping, or unknown). State given was: "
              + state);
    }

    ArrayList<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < numContainers(); i++) {
      if (getContainer(i).getState().toString().equals(state))
        indexes.add(i);
    }
    return indexes;
  }

  /**
   * Return the cargo container of all of the containers in the given state
   */
  public ArrayList<ServerContainer> getContainersWithState(String state) {
    ArrayList<ServerContainer> statedContainers = new ArrayList<>();
    for (int index : getContainerIndexesWithState(state))
      statedContainers.add(getContainer(index));
    return statedContainers;
  }

  /**
   * Get the port of the container at the given index
   */
  public String getContainerPort(int index) {
    return getContainer(index).getPort();
  }

  /**
   * Get the container at the given index
   */
  public ServerContainer getContainer(int index) {
    return containers.get(index);
  }

  /**
   * Get a human readable unique description for the container (calls
   * {@link ServerContainer#toString()})
   */
  public String getContainerDescription(int index) {
    return getContainer(index).toString();
  }

  /**
   * Create a container to manage, given an installation.
   */
  private ServerContainer addContainer(ContainerInstall install, int index) throws IOException {
    ServerContainer container = install.generateContainer(testName + "_" + index);

    containers.add(index, container);

    logger.info("Setup container " + getContainerDescription(index));
    return container;
  }

  /**
   * Remove the container in the given index from the list
   */
  public ServerContainer removeContainer(int index) {
    return containers.remove(index);
  }

  /**
   * Remove the given container
   */
  public boolean removeContainer(ServerContainer container) {
    return containers.remove(container);
  }

  /**
   * Get the indexes of all active containers
   */
  private ArrayList<Integer> getActiveContainerIndexes() {
    return getContainerIndexesWithState(State.STARTED.toString());
  }

  /**
   * Get all active containers
   */
  private ArrayList<ServerContainer> getActiveContainers() {
    return getContainersWithState(State.STARTED.toString());
  }

  /**
   * Get the indexes of all inactive containers
   */
  private ArrayList<Integer> getInactiveContainerIndexes() {
    ArrayList<Integer> indexes = getContainerIndexesWithState(State.STOPPED.toString());
    indexes.addAll(getContainerIndexesWithState(State.UNKNOWN.toString()));
    return indexes;
  }

  /**
   * Get all inactive containers
   */
  private ArrayList<ServerContainer> getInactiveContainers() {
    ArrayList<ServerContainer> inactiveContainers =
        getContainersWithState(State.STOPPED.toString());
    inactiveContainers.addAll(getContainersWithState(State.UNKNOWN.toString()));
    return inactiveContainers;
  }
}
