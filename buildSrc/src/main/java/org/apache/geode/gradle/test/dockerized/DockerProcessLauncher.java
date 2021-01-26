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
package org.apache.geode.gradle.test.dockerized;

import static java.lang.String.join;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;

/**
 * A process launcher that launches each process in a Docker container.
 */
public class DockerProcessLauncher {
  private final DockerizedTestConfig config;
  private final DockerClient client;

  public DockerProcessLauncher(DockerizedTestConfig config, DockerClient client) {
    this.config = config;
    this.client = client;
  }

  /**
   * Launches the specified process in a Docker container.
   *
   * @param processBuilder a builder that specifies the process to launch
   * @return a Process that represents the process running in the Docker container
   */
  public Process start(ProcessBuilder processBuilder) {
    config.getPrepareJavaCommand().call(processBuilder);
    CreateContainerCmd createContainerCommand = client.createContainerCmd(config.getImage())
        .withTty(false)
        .withStdinOpen(true)
        .withWorkingDir(processBuilder.directory().getAbsolutePath())
        .withEnv(asStrings(processBuilder.environment()))
        .withCmd(processBuilder.command());
    setUser(createContainerCommand);
    setVolumes(createContainerCommand);

    CreateContainerResponse createContainerResult = createContainerCommand.exec();
    String containerId = createContainerResult.getId();
    client.startContainerCmd(containerId).exec();
    Process process = DockerProcess.attachedTo(client, containerId);
    return process;
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

  private static List<String> asStrings(Map<String, String> map) {
    return map.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(toList());
  }
}
