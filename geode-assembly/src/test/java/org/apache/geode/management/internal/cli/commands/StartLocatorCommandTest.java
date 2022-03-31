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
package org.apache.geode.management.internal.cli.commands;

import static java.util.stream.Collectors.toSet;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.LocatorLauncher.Command.START;
import static org.apache.geode.management.internal.cli.commands.MemberJvmOptions.getMemberJvmOptions;
import static org.apache.geode.management.internal.cli.commands.VerifyCommandLine.verifyCommandLine;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.apache.geode.distributed.LocatorLauncher;

class StartLocatorCommandTest {
  // JVM options to use with every start command.
  private static final Set<String> START_COMMAND_UNCONDITIONAL_JVM_OPTIONS = Stream.of(
      "-Dgemfire.launcher.registerSignalHandlers=true",
      "-Djava.awt.headless=true",
      "-Dsun.rmi.dgc.server.gcInterval=9223372036854775806").collect(toSet());

  private final StartLocatorCommand startLocatorCommand = new StartLocatorCommand();

  @Nested
  class GetLocatorClasspath {
    @Test
    public void addsUserClasspathImmediatelyAfterGemfireJarPath() {
      final String userClasspath = "/path/to/user/lib/app.jar:/path/to/user/classes";

      String actualClasspath = startLocatorCommand.getLocatorClasspath(true, userClasspath);

      String expectedClasspath = String.join(File.pathSeparator,
          StartMemberUtils.getGemFireJarPath(),
          userClasspath,
          System.getProperty("java.class.path"),
          StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);
      assertThat(actualClasspath)
          .isEqualTo(expectedClasspath);
    }
  }

  @Nested
  class GetLocatorCommandLine {
    private static final String LOCATOR_LAUNCHER_CLASS_NAME =
        "org.apache.geode.distributed.LocatorLauncher";

    @Test
    void withTypicalOptions() throws Exception {
      Set<String> expectedJvmOptions = new HashSet<>();
      List<String> expectedStartCommandSequence = new ArrayList<>();
      Set<String> expectedStartCommandOptions = new HashSet<>();

      LocatorLauncher.Builder locatorLauncherBuilder = new LocatorLauncher.Builder();

      locatorLauncherBuilder.setCommand(START);
      expectedStartCommandSequence.add(LOCATOR_LAUNCHER_CLASS_NAME);
      expectedStartCommandSequence.add(START.getName());

      final String memberName = "with-typical-options";
      locatorLauncherBuilder.setMemberName(memberName);
      expectedStartCommandSequence.add(memberName);

      final int defaultPort = 10334;
      expectedStartCommandOptions.add("--port=" + defaultPort);

      LocatorLauncher locatorLauncher = locatorLauncherBuilder.build();

      String expectedClasspath = String.join(
          File.pathSeparator,
          StartMemberUtils.getGemFireJarPath(),
          StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);

      List<String> expectedJavaCommandSequence = Arrays.asList(
          StartMemberUtils.getJavaPath(),
          "-server",
          "-classpath",
          expectedClasspath);

      expectedJvmOptions.addAll(START_COMMAND_UNCONDITIONAL_JVM_OPTIONS);
      expectedJvmOptions.addAll(getMemberJvmOptions());

      String[] commandLine =
          startLocatorCommand.createStartLocatorCommandLine(locatorLauncher,
              null, null, new Properties(), null, false, null, null, null);

      verifyCommandLine(commandLine, expectedJavaCommandSequence, expectedJvmOptions,
          expectedStartCommandSequence, expectedStartCommandOptions);
    }

    @Test
    void withRestApiOptions() throws Exception {
      LocatorLauncher.Builder locatorLauncherBuilder = new LocatorLauncher.Builder();
      Set<String> expectedJvmOptions = new HashSet<>();
      List<String> expectedStartCommandSequence = new ArrayList<>();
      Set<String> expectedStartCommandOptions = new HashSet<>();

      locatorLauncherBuilder.setCommand(START);
      expectedStartCommandSequence.add(LOCATOR_LAUNCHER_CLASS_NAME);
      expectedStartCommandSequence.add(START.getName());

      final String memberName = "with-rest-api-options";
      locatorLauncherBuilder.setMemberName(memberName);
      expectedStartCommandSequence.add(memberName);

      final String bindAddress = "localhost";
      locatorLauncherBuilder.setBindAddress(bindAddress);
      expectedStartCommandOptions.add("--bind-address=" + bindAddress);

      final int port = 11111;
      locatorLauncherBuilder.setPort(port);
      expectedStartCommandOptions.add("--port=" + port);

      LocatorLauncher locatorLauncher = locatorLauncherBuilder.build();
      Properties gemfireProperties = new Properties();

      // TODO: Where does the command line get the bind address from? Props or launcher?
      gemfireProperties.setProperty(HTTP_SERVICE_BIND_ADDRESS, bindAddress);
      expectedJvmOptions.add("-D" + GEMFIRE_PREFIX + HTTP_SERVICE_BIND_ADDRESS + "=" + bindAddress);

      final String servicePort = "8089";
      gemfireProperties.setProperty(HTTP_SERVICE_PORT, servicePort);
      expectedJvmOptions.add("-D" + GEMFIRE_PREFIX + HTTP_SERVICE_PORT + "=" + servicePort);

      expectedJvmOptions.addAll(START_COMMAND_UNCONDITIONAL_JVM_OPTIONS);
      expectedJvmOptions.addAll(getMemberJvmOptions());

      String expectedClasspath = String.join(
          File.pathSeparator,
          StartMemberUtils.getGemFireJarPath(),
          StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);

      List<String> expectedJavaCommandSequence = Arrays.asList(
          StartMemberUtils.getJavaPath(),
          "-server",
          "-classpath",
          expectedClasspath);

      String[] commandLine =
          startLocatorCommand.createStartLocatorCommandLine(locatorLauncher,
              null, null, gemfireProperties, null, false, new String[0], null, null);

      verifyCommandLine(commandLine, expectedJavaCommandSequence, expectedJvmOptions,
          expectedStartCommandSequence, expectedStartCommandOptions);
    }

    @Test
    void withAllOptions() throws Exception {
      Set<String> expectedJvmOptions = new HashSet<>();
      List<String> expectedStartCommandSequence = new ArrayList<>();
      Set<String> expectedStartCommandOptions = new HashSet<>();

      LocatorLauncher.Builder locatorLauncherBuilder = new LocatorLauncher.Builder();

      final String memberName = "with-all-options";
      locatorLauncherBuilder.setCommand(START);
      locatorLauncherBuilder.setMemberName(memberName);
      expectedStartCommandSequence.add(LOCATOR_LAUNCHER_CLASS_NAME);
      expectedStartCommandSequence.add(START.getName());
      expectedStartCommandSequence.add(memberName);

      locatorLauncherBuilder.setDebug(true);
      expectedStartCommandOptions.add("--debug");

      // TODO: How does this affect the command line?
      locatorLauncherBuilder.setDeletePidFileOnStop(true);

      locatorLauncherBuilder.setForce(true);
      expectedStartCommandOptions.add("--force");

      final String hostnameForClients = "localhost";
      locatorLauncherBuilder.setHostnameForClients(hostnameForClients);
      expectedStartCommandOptions.add("--hostname-for-clients=" + hostnameForClients);

      final int port = 10101;
      locatorLauncherBuilder.setPort(port);
      expectedStartCommandOptions.add("--port=" + port);

      locatorLauncherBuilder.setRedirectOutput(true);
      expectedStartCommandOptions.add("--redirect-output");
      expectedJvmOptions.add("-Dgemfire.OSProcess.DISABLE_REDIRECTION_CONFIGURATION=true");

      LocatorLauncher locatorLauncher = locatorLauncherBuilder
          .build();

      Properties gemfireProperties = new Properties();

      final String statisticSampleRate = "1500";
      gemfireProperties.setProperty(STATISTIC_SAMPLE_RATE, statisticSampleRate);
      expectedJvmOptions.add("-Dgemfire.statistic-sample-rate=" + statisticSampleRate);

      final String disableAutoReconnect = "true";
      gemfireProperties.setProperty(DISABLE_AUTO_RECONNECT, disableAutoReconnect);
      expectedJvmOptions.add("-Dgemfire.disable-auto-reconnect=" + disableAutoReconnect);

      File propertiesFile = mock(File.class);
      final String propertiesFilePath = "/config/customGemfire.properties";
      when(propertiesFile.getAbsolutePath())
          .thenReturn(propertiesFilePath);
      expectedJvmOptions.add("-DgemfirePropertyFile=" + propertiesFilePath);

      File securityPropertiesFile = mock(File.class);
      final String securityPropertiesFilePath = "/config/customGemfireSecurity.properties";
      when(securityPropertiesFile.getAbsolutePath())
          .thenReturn(securityPropertiesFilePath);
      expectedJvmOptions.add("-DgemfireSecurityPropertyFile=" + securityPropertiesFilePath);

      String[] customJvmArguments = {
          "-verbose:gc",
          "-Xloggc:member-gc.log",
          "-XX:+PrintGCDateStamps",
          "-XX:+PrintGCDetails",
      };
      expectedJvmOptions.addAll(Arrays.asList(customJvmArguments));

      final String userClasspath = "/temp/domain-1.0.0.jar";
      String expectedClasspath = String.join(
          File.pathSeparator,
          StartMemberUtils.getGemFireJarPath(),
          userClasspath,
          StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);

      List<String> expectedJavaCommandSequence = Arrays.asList(
          StartMemberUtils.getJavaPath(),
          "-server",
          "-classpath",
          expectedClasspath);

      final String heapSize = "1024m";
      expectedJvmOptions.add("-Xms" + heapSize);
      expectedJvmOptions.add("-Xmx" + heapSize);
      expectedJvmOptions.add("-XX:+UseConcMarkSweepGC");
      expectedJvmOptions.add("-XX:CMSInitiatingOccupancyFraction=60");

      expectedJvmOptions.addAll(START_COMMAND_UNCONDITIONAL_JVM_OPTIONS);
      expectedJvmOptions.addAll(getMemberJvmOptions());

      String[] commandLine =
          startLocatorCommand.createStartLocatorCommandLine(locatorLauncher,
              propertiesFile, securityPropertiesFile, gemfireProperties,
              userClasspath, false, customJvmArguments, heapSize, heapSize);

      verifyCommandLine(commandLine, expectedJavaCommandSequence, expectedJvmOptions,
          expectedStartCommandSequence, expectedStartCommandOptions);
    }
  }
}
