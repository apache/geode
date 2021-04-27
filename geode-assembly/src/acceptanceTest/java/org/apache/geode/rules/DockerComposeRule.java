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

package org.apache.geode.rules;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.ToStringConsumer;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

public class DockerComposeRule extends ExternalResource {

  private static final Logger logger = LogService.getLogger();

  private final RuleChain delegate;
  private final String composeFile;
  private final Map<String, List<Integer>> exposedServices;
  private DockerComposeContainer<?> composeContainer;

  public DockerComposeRule(String composeFile, Map<String, List<Integer>> exposedServices) {
    this.composeFile = composeFile;
    this.exposedServices = exposedServices;
    delegate = RuleChain.outerRule(new IgnoreOnWindowsRule());
  }

  @Override
  public Statement apply(Statement base, Description description) {
    Statement containStatement = new Statement() {
      @Override
      public void evaluate() throws Throwable {

        composeContainer = new DockerComposeContainer<>("compose", new File(composeFile));
        exposedServices.forEach((service, ports) -> ports
            .forEach(p -> composeContainer.withExposedService(service, p)));

        composeContainer.start();

        try {
          base.evaluate();
        } finally {
          composeContainer.stop();
        }
      }
    };

    return delegate.apply(containStatement, description);
  }

  /**
   * When used with compose, testcontainers does not allow one to have a 'container_name'
   * attribute in the compose file. This means that container names end up looking something like:
   * {@code project_service_index}. When a container performs a reverse IP lookup it will get a
   * hostname that looks something like {@code projectjkh_db_1.my-netwotk}. This can be a problem
   * since this hostname is not RFC compliant as it contains underscores. This may cause problems
   * in particular with SSL.
   */
  public void normalizeContainerName(String serviceName) {
    ContainerState container = composeContainer.getContainerByServiceName(serviceName + "_1")
        .orElseThrow(() -> new IllegalArgumentException("Unknown service name: " + serviceName));

    String containerId = container.getContainerId();

    DockerClient dockerClient = DockerClientFactory.instance().client();
    dockerClient.renameContainerCmd(containerId).withName(serviceName).exec();
  }

  public String execForService(String serviceName, String... args) {
    ContainerState container = composeContainer.getContainerByServiceName(serviceName + "_1")
        .orElseThrow(() -> new IllegalArgumentException("Unknown service name: " + serviceName));
    Container.ExecResult result;
    try {
      result = container.execInContainer(args);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result.getExitCode() == 0 ? result.getStdout() : result.getStderr();
  }

  public Long loggingExecForService(String serviceName, String... command) {
    ContainerState container = composeContainer.getContainerByServiceName(serviceName + "_1")
        .orElseThrow(() -> new IllegalArgumentException("Unknown service name: " + serviceName));

    String containerId = container.getContainerId();
    String containerName = container.getContainerInfo().getName();

    logger.info("{}: Running 'exec' command: {}", containerName, command);

    DockerClient dockerClient = DockerClientFactory.instance().client();

    final ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(containerId)
        .withAttachStdout(true).withAttachStderr(true).withCmd(command).exec();

    final ToLogConsumer stdoutConsumer = new ToLogConsumer(serviceName, logger);
    final ToLogConsumer stderrConsumer = new ToLogConsumer(serviceName, logger);

    FrameConsumerResultCallback callback = new FrameConsumerResultCallback();
    callback.addConsumer(OutputFrame.OutputType.STDOUT, stdoutConsumer);
    callback.addConsumer(OutputFrame.OutputType.STDERR, stderrConsumer);

    try {
      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec().getExitCodeLong();
  }


  public Integer getExternalPortForService(String serviceName, int port) {
    return composeContainer.getServicePort(serviceName, port);
  }

  public void pauseService(String serviceName) {
    ContainerState container = composeContainer.getContainerByServiceName(serviceName + "_1")
        .orElseThrow(() -> new IllegalArgumentException("Unknown service name: " + serviceName));
    DockerClientFactory.instance().client().pauseContainerCmd(container.getContainerId()).exec();
  }

  public void unpauseService(String serviceName) {
    ContainerState container = composeContainer.getContainerByServiceName(serviceName + "_1")
        .orElseThrow(() -> new IllegalArgumentException("Unknown service name: " + serviceName));
    DockerClientFactory.instance().client().unpauseContainerCmd(container.getContainerId()).exec();
  }

  public static class Builder {
    private String filePath;
    private final Map<String, List<Integer>> exposedServices = new HashMap<>();

    public Builder() {}

    public DockerComposeRule build() {
      return new DockerComposeRule(filePath, exposedServices);
    }

    public Builder file(String filePath) {
      this.filePath = filePath;
      return this;
    }

    public Builder service(String serviceName, Integer port) {
      exposedServices.computeIfAbsent(serviceName, k -> new ArrayList<>()).add(port);
      return this;
    }
  }

  private static class ToLogConsumer extends BaseConsumer<ToStringConsumer> {
    private boolean firstLine = true;
    private final Logger logger;
    private final String prefix;

    public ToLogConsumer(String prefix, Logger logger) {
      this.prefix = prefix;
      this.logger = logger;
    }

    @Override
    public void accept(OutputFrame outputFrame) {
      if (outputFrame.getBytes() != null) {
        if (!firstLine) {
          logger.info("[{}]:", prefix);
        }
        logger.info("[{}]: {}", prefix, outputFrame.getUtf8String());
        firstLine = false;
      }
    }
  }

}
