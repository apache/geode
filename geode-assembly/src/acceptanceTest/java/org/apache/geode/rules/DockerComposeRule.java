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

/**
 * This class assists in managing the lifecycle of a cluster, launched via a docker-compose
 * configuration file, for testing. For example:
 *
 * <pre>
 *
 * &#64;ClassRule
 * public static DockerComposeRule cluster = new DockerComposeRule().Builder()
 *     .file("/home/bob/test/docker-compose.yml")
 *     .service("haproxy", 15223)
 *     .build();
 *
 * // Get the exposed port for haproxy
 * cluster.getExternalPortForService("haproxy", 15223);
 * </pre>
 *
 * Some limitations are as follows:
 * <ul>
 * <li>{@code testcontainers} does not support using {@code container_name:} attributes. If you
 * need your container to be named explicitly, use {@link DockerComposeRule#setContainerName}</li>
 * <li>Do not use the {@code expose} directives in your yaml file. Instead use
 * {@link DockerComposeRule.Builder#service}
 * to expose the relevant service and port.</li>
 * <li>For now, this rule only handles a single instance of a service.</li>
 * </ul>
 *
 * @see <a href=
 *      "https://www.testcontainers.org/modules/docker_compose/">https://www.testcontainers.org/modules/docker_compose/</a>
 */
public class DockerComposeRule extends ExternalResource {

  private static final Logger logger = LogService.getLogger();

  private final RuleChain delegate;
  private final String composeFile;
  private final Map<String, List<Integer>> exposedServices;
  private DockerComposeContainer<?> composeContainer;

  public DockerComposeRule(String composeFile, Map<String, List<Integer>> exposedServices) {
    this.composeFile = composeFile;
    this.exposedServices = exposedServices;

    // Docker compose does not work on windows in CI. Ignore this test on windows using a
    // RuleChain to make sure we ignore the test before the rule comes into play.
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
   * hostname that looks something like {@code projectjkh_db_1.my-network}. This can be a problem
   * since this hostname is not RFC compliant as it contains underscores. This may cause problems
   * in particular with SSL.
   *
   * @param serviceName the service who's container name to change
   * @param newName the new container name
   *
   * @throws IllegalArgumentException if the service cannot be found
   */
  public void setContainerName(String serviceName, String newName) {
    ContainerState container = composeContainer.getContainerByServiceName(serviceName + "_1")
        .orElseThrow(() -> new IllegalArgumentException("Unknown service name: " + serviceName));

    String containerId = container.getContainerId();

    DockerClient dockerClient = DockerClientFactory.instance().client();
    dockerClient.renameContainerCmd(containerId).withName(newName).exec();
  }

  /**
   * Execute a command in a service container
   *
   * @return the stdout of the container if the command was successful, else the stderr
   */
  public String execForService(String serviceName, String... command) {
    ContainerState container = composeContainer.getContainerByServiceName(serviceName + "_1")
        .orElseThrow(() -> new IllegalArgumentException("Unknown service name: " + serviceName));
    Container.ExecResult result;
    try {
      result = container.execInContainer(command);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result.getExitCode() == 0 ? result.getStdout() : result.getStderr();
  }

  /**
   * Execute a command in a service container, logging the output
   *
   * @return the exit code of the command
   */
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

  /**
   * Get the (ephemeral) exposed port for a service. This is the port that a test would use to
   * connect.
   *
   * @param serviceName the service
   * @param port the port (internal) which was exposed when building the rule
   * @return the exposed port
   */
  public Integer getExternalPortForService(String serviceName, int port) {
    return composeContainer.getServicePort(serviceName, port);
  }

  /**
   * Pause a service. This is helpful to test failure conditions.
   *
   * @see <a href=
   *      "https://docs.docker.com/engine/reference/commandline/pause/">https://docs.docker.com/engine/reference/commandline/pause/</a>
   * @param serviceName the service to pause
   */
  public void pauseService(String serviceName) {
    ContainerState container = composeContainer.getContainerByServiceName(serviceName + "_1")
        .orElseThrow(() -> new IllegalArgumentException("Unknown service name: " + serviceName));
    DockerClientFactory.instance().client().pauseContainerCmd(container.getContainerId()).exec();
  }

  /**
   * Unpause the service. This does not restart anything.
   *
   * @param serviceName the service to unpause
   */
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
