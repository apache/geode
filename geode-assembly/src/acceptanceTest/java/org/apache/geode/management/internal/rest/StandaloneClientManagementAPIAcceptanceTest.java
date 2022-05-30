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
package org.apache.geode.management.internal.rest;

import static java.lang.System.lineSeparator;
import static java.nio.file.Files.createDirectories;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.rules.gfsh.internal.ProcessLogger;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class StandaloneClientManagementAPIAcceptanceTest {

  @Parameters
  public static Collection<Boolean> data() {
    return Arrays.asList(true, false);
  }

  @Parameter
  public Boolean useSsl;

  private String trustStorePath;
  private ProcessLogger clientProcessLogger;
  private Path rootFolder;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  @Before
  public void setUp() {
    rootFolder = folderRule.getFolder().toPath();

    /*
     * This file was generated with:
     * keytool -genkey -dname "CN=localhost" -alias self -validity 3650 -keyalg EC \
     * -keystore trusted.keystore -keypass password -storepass password \
     * -ext san=ip:127.0.0.1,dns:localhost -storetype jks
     */
    trustStorePath = createTempFileFromResource(
        StandaloneClientManagementAPIAcceptanceTest.class, "/ssl/trusted.keystore")
            .getAbsolutePath();
    assertThat(trustStorePath)
        .as("java file resource not found")
        .isNotBlank();
  }

  @After
  public void tearDown() throws InterruptedException, ExecutionException, TimeoutException {
    clientProcessLogger.awaitTermination(getTimeout().toMillis(), MILLISECONDS);
    clientProcessLogger.close();
  }

  @Test
  public void clientCreatesRegionUsingClusterManagementService()
      throws IOException, InterruptedException {
    JarBuilder jarBuilder = new JarBuilder();
    String filePath = createTempFileFromResource(
        getClass(), "/ManagementClientCreateRegion.java").getAbsolutePath();
    assertThat(filePath)
        .as("java file resource not found")
        .isNotBlank();

    File outputJar = new File(rootFolder.toFile(), "output.jar");
    jarBuilder.buildJar(outputJar, new File(filePath));

    int[] availablePorts = getRandomAvailableTCPPorts(3);
    int locatorPort = availablePorts[0];
    int httpPort = availablePorts[1];
    int jmxPort = availablePorts[2];

    GfshExecution startCluster = GfshScript
        .of(String.format(
            "start locator --port=%d --http-service-port=%d --J=-Dgemfire.JMX_MANAGER_PORT=%d %s",
            locatorPort, httpPort, jmxPort, getSslParameters()),
            String.format("start server --locators=localhost[%d] --server-port=0", locatorPort))
        .withName("startCluster")
        .execute(gfshRule);

    int expectedReturnCode = 0;
    assertThat(startCluster.getProcess().exitValue())
        .as("Cluster did not start correctly").isEqualTo(expectedReturnCode);

    Process process = launchClientProcess(outputJar, httpPort);

    long processTimeout = getTimeout().getSeconds();
    boolean exited = process.waitFor(processTimeout, TimeUnit.SECONDS);
    assertThat(exited).as(String.format("Process did not exit within %d seconds", processTimeout))
        .isTrue();
    assertThat(process.exitValue())
        .as(String.format("Process did not exit with %d return code", expectedReturnCode))
        .isEqualTo(0);

    GfshExecution listRegionsResult = GfshScript
        .of(String.format("connect --locator=localhost[%d]", locatorPort), "list regions")
        .withName("listRegions")
        .execute(gfshRule);
    assertThat(listRegionsResult.getOutputText())
        .contains("REGION1");
  }

  private Process launchClientProcess(File outputJar, int httpPort) throws IOException {
    Path javaBin = Paths.get(System.getProperty("java.home"), "bin", "java");

    Path clientFolder = createDirectories(rootFolder.resolve("client"));

    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.directory(clientFolder.toFile());

    StringBuilder classPath = new StringBuilder();
    for (String module : Arrays.asList(
        "commons-logging",
        "commons-lang3",
        "geode-common",
        "geode-management",
        "jackson-annotations",
        "jackson-core",
        "jackson-databind",
        "jackson-datatype-jsr310",
        "jackson-datatype-joda",
        "joda-time",
        "httpclient",
        "httpcore",
        "spring-beans",
        "spring-core",
        "spring-web")) {
      classPath.append(getJarOrClassesForModule(module));
      classPath.append(File.pathSeparator);
    }

    classPath.append(File.pathSeparator);
    classPath.append(outputJar.getAbsolutePath());

    List<String> command = new ArrayList<>();
    command.add(javaBin.toString());

    if (useSsl) {
      command.add("-Djavax.net.ssl.keyStore=" + trustStorePath);
      command.add("-Djavax.net.ssl.keyStorePassword=password");
      command.add("-Djavax.net.ssl.trustStore=" + trustStorePath);
      command.add("-Djavax.net.ssl.trustStorePassword=password");
    }

    command.add("-classpath");
    command.add(classPath.toString());
    command.add("ManagementClientCreateRegion");
    command.add("REGION1");
    command.add(useSsl.toString());
    command.add(String.valueOf(httpPort));

    processBuilder.command(command);

    System.out.format("Launching client command: %s%s", command, lineSeparator());

    Process process = processBuilder.start();
    clientProcessLogger = new ProcessLogger(process, "clientCreateRegion");
    clientProcessLogger.start();
    return process;
  }

  private String getSslParameters() {
    if (useSsl) {
      return String.format(" --J=-Dgemfire.ssl-keystore=%1$s"
          + " --J=-Dgemfire.ssl-keystore-password=%2$s"
          + " --J=-Dgemfire.ssl-truststore=%1$s"
          + " --J=-Dgemfire.ssl-truststore-password=%2$s"
          + " --J=-Dgemfire.ssl-enabled-components=web",
          trustStorePath, "password");
    }

    return "";
  }

  private String getJarOrClassesForModule(String module) {
    String classPathValue = System.getProperty("java.class.path");
    String classPath = Arrays.stream(classPathValue
        .split(File.pathSeparator))
        .filter(x -> x.contains(module))
        .collect(Collectors.joining(File.pathSeparator));

    assertThat(classPath)
        .as("no classes found for module: " + module)
        .isNotBlank();

    return classPath;
  }
}
