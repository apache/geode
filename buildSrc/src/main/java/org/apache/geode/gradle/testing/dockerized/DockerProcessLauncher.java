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
package org.apache.geode.gradle.testing.dockerized;

import static java.util.stream.Collectors.toList;
import static org.apache.geode.gradle.testing.dockerized.DockerTestWorkerConfig.getDurationWarningThreshold;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.AbstractDockerCmdExecFactory;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.netty.NettyDockerCmdExecFactory;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.internal.UncheckedException;

import org.apache.geode.gradle.testing.process.ProcessLauncher;

/**
 * A process launcher that launches each process in a Docker container.
 */
public class DockerProcessLauncher implements ProcessLauncher {
  private static final Logger LOGGER = Logging.getLogger(DockerProcessLauncher.class);
  private final DockerTestWorkerConfig config;
  private final Consumer<ProcessBuilder> adjustment;

  /**
   * @param config the configuration of the docker containers
   * @param adjustment configures the process builder before it is dockerized
   */
  public DockerProcessLauncher(DockerTestWorkerConfig config, Consumer<ProcessBuilder> adjustment) {
    this.config = config;
    this.adjustment = adjustment;
  }

  /**
   * Launches the specified process in a Docker container.
   *
   * @param processBuilder a builder that specifies the process to launch
   * @return a Process that represents the process running in the Docker container
   */
  @Override
  public Process start(ProcessBuilder processBuilder) {
    adjustment.accept(processBuilder);
    config.dockerize(processBuilder);
    int timeout = config.getTimeoutMillis();

    // For synchronous Docker operations (create, start, inspect), time out if connecting or
    // reading takes too long.
    DockerClient clientForSynchronousOperations = dockerClient(timeout, timeout);

    String containerId = createContainer(processBuilder, clientForSynchronousOperations);

    try {
      startContainer(clientForSynchronousOperations, containerId);
      // For asynchronous Docker operations, time out only on connects. The DockerProcess uses the
      // async client to listen for process output and process termination. Because the client must
      // listen indefinitely for these events, do not time out on reads.
      DockerClient clientForAsynchronousOperations = dockerClient(timeout, 0);
      return DockerProcess.attachedTo(
          config.getName(), clientForAsynchronousOperations, containerId, timeout,
          () -> removeContainer(clientForSynchronousOperations, containerId));
    } catch (Exception e) {
      removeContainer(clientForSynchronousOperations, containerId);
      UncheckedException.throwAsUncheckedException(e);
      return null; // Unreachable
    }
  }

  /**
   * Creates a docker client with the given timeouts.
   *
   * @param connectTimeout timeout for connecting, or 0 to disable connect timeouts
   * @param readTimeout timeout for reading, or 0 to disable read timeouts
   */
  private static DockerClient dockerClient(int connectTimeout, int readTimeout) {
    AbstractDockerCmdExecFactory cmdExecFactory = new NettyDockerCmdExecFactory();
    if (connectTimeout > 0) {
      cmdExecFactory.withConnectTimeout(connectTimeout);
    }
    if (readTimeout > 0) {
      cmdExecFactory.withReadTimeout(readTimeout);
    }
    // Must use the deprecated withDockerCmdExecFactory() because it is currently the only way to
    // use Netty, and Netty is currently the only transport that supports timeouts.
    @SuppressWarnings("deprecation")
    DockerClient client = DockerClientBuilder.getInstance()
        .withDockerCmdExecFactory(cmdExecFactory)
        .build();
    return client;
  }

  private String createContainer(ProcessBuilder processBuilder, DockerClient client) {
    CreateContainerCmd createContainerCommand = client.createContainerCmd(config.getImage())
        .withTty(false)
        .withStdinOpen(true)
        .withWorkingDir(processBuilder.directory().getAbsolutePath())
        .withEnv(asStrings(processBuilder.environment()))
        .withCmd(processBuilder.command());
    setUser(createContainerCommand);
    setVolumes(createContainerCommand);
    LOGGER.debug("{} creating container", this);
    try {
      long startTime = System.currentTimeMillis();
      String containerId = createContainerCommand.exec().getId();
      long duration = System.currentTimeMillis() - startTime;
      if (duration > getDurationWarningThreshold()) {
        LOGGER.warn("{} create took {}ms", this, duration);
      }
      LOGGER.debug("{} created container {}", this, containerId);
      return containerId;
    } catch (RuntimeException e) {
      String message = String.format("%s error while creating container", this);
      throw new RuntimeException(message, e);
    }
  }

  private void startContainer(DockerClient client, String containerId) {
    LOGGER.debug("{} starting container {}", this, containerId);
    try {
      long startTime = System.currentTimeMillis();
      client.startContainerCmd(containerId).exec();
      LOGGER.debug("{} started container {}", this, containerId);
      long duration = System.currentTimeMillis() - startTime;
      if (duration > getDurationWarningThreshold()) {
        LOGGER.warn("{} start {} took {}ms", this, containerId, duration);
      }
    } catch (RuntimeException e) {
      String message = String.format("%s error while starting container %s", this, containerId);
      throw new RuntimeException(message, e);
    }
    InspectContainerResponse report;
    try {
      long startTime = System.currentTimeMillis();
      report = client.inspectContainerCmd(containerId).exec();
      long duration = System.currentTimeMillis() - startTime;
      if (duration > getDurationWarningThreshold()) {
        LOGGER.warn("{} inspect {} took {}ms", this, containerId, duration);
      }
    } catch (RuntimeException e) {
      String message = String.format("%s error while inspecting container %s", this, containerId);
      throw new RuntimeException(message, e);
    }
    InspectContainerResponse.ContainerState state = report.getState();
    LOGGER.debug("{} container {} state is {}", this, containerId, state);
    Boolean isRunning = state.getRunning();
    if (isRunning == null || !isRunning) {
      String message = String.format("%s cannot attach to container %s because it is %s",
          this, containerId, state.getStatus());
      throw new RuntimeException(message);
    }
  }

  private void removeContainer(DockerClient client, String containerId) {
    LOGGER.debug("{} removing container {}", this, containerId);
    try {
      long startTime = System.currentTimeMillis();
      client.removeContainerCmd(containerId)
          .withForce(true)
          .exec();
      long duration = System.currentTimeMillis() - startTime;
      if (duration > getDurationWarningThreshold()) {
        LOGGER.warn("{} remove {} took {}ms", this, containerId, duration);
      }
      LOGGER.debug("{} removed container {}", this, containerId);
    } catch (Exception e) {
      String message = String.format("%s error while removing container %s", this, containerId);
      LOGGER.warn(message, e);
    }
    try {
      client.close();
      LOGGER.debug("{} closed client", this);
    } catch (IOException e) {
      String message = String.format("%s error while closing client", this);
      LOGGER.warn(message, e);
    }
  }

  private void setUser(CreateContainerCmd command) {
    String user = config.getUser();
    if (user != null) {
      command.withUser(user);
    }
  }

  private void setVolumes(CreateContainerCmd command) {
    List<Bind> binds = config.getVolumes().entrySet().stream()
        .map(e -> new Bind(e.getKey(), new Volume(e.getValue())))
        .collect(toList());
    List<Volume> volumes = binds.stream()
        .map(Bind::getVolume)
        .collect(toList());
    command.withVolumes(volumes);
    command.getHostConfig().withBinds(binds);
  }

  @Override
  public String toString() {
    return "DockerProcessLauncher{" + config.getName() + "}";
  }

  private static List<String> asStrings(Map<String, String> map) {
    return map.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(toList());
  }
}
