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

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ServerLauncher.Command.START;
import static org.apache.geode.internal.lang.SystemUtils.IBM_J9_JVM_NAME;
import static org.apache.geode.internal.lang.SystemUtils.JAVA_HOTSPOT_JVM_NAME;
import static org.apache.geode.internal.lang.SystemUtils.ORACLE_JROCKIT_JVM_NAME;
import static org.apache.geode.management.internal.cli.commands.MemberJvmOptions.getGcJvmOptions;
import static org.apache.geode.management.internal.cli.commands.MemberJvmOptions.getMemberJvmOptions;
import static org.apache.geode.management.internal.cli.commands.StartServerCommand.addJvmOptionsForOutOfMemoryErrors;
import static org.apache.geode.management.internal.cli.commands.VerifyCommandLine.verifyCommandLine;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.condition.OS.WINDOWS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledOnOs;

import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.GemFireVersion;

class StartServerCommandTest {
  // JVM options to use with every start command.
  private static final Set<String> START_COMMAND_UNCONDITIONAL_JVM_OPTIONS = Stream.of(
      "-Dgemfire.launcher.registerSignalHandlers=true",
      "-Djava.awt.headless=true",
      "-Dsun.rmi.dgc.server.gcInterval=9223372036854775806")
      .collect(toSet());

  @Nested
  class AddJvmOptionsForOutOfMemoryErrors {
    private static final String IS_HOTSPOT_VM = ".*" + JAVA_HOTSPOT_JVM_NAME + ".*";
    private static final String IS_J9_VM = ".*" + IBM_J9_JVM_NAME + ".*";
    private static final String IS_ROCKIT_VM = ".*" + ORACLE_JROCKIT_JVM_NAME + ".*";

    @EnabledIfSystemProperty(named = "java.vm.name", matches = IS_HOTSPOT_VM)
    @EnabledOnOs(WINDOWS)
    @Test
    void onWindowsHotSpotVM() {
      final List<String> jvmOptions = new ArrayList<>();
      addJvmOptionsForOutOfMemoryErrors(jvmOptions);
      assertThat(jvmOptions)
          .containsExactly("-XX:OnOutOfMemoryError=taskkill /F /PID %p");
    }

    @Test
    @EnabledIfSystemProperty(named = "java.vm.name", matches = IS_HOTSPOT_VM)
    @DisabledOnOs(WINDOWS)
    void onNonWindowsHotSpotVM() {
      final List<String> jvmOptions = new ArrayList<>();
      addJvmOptionsForOutOfMemoryErrors(jvmOptions);
      assertThat(jvmOptions)
          .containsExactly("-XX:OnOutOfMemoryError=kill -KILL %p");
    }

    @Test
    @EnabledIfSystemProperty(named = "java.vm.name", matches = IS_J9_VM)
    void onJ9VM() {
      final List<String> jvmOptions = new ArrayList<>();
      addJvmOptionsForOutOfMemoryErrors(jvmOptions);
      assertThat(jvmOptions)
          .containsExactly("-Xcheck:memory");
    }

    @Test
    @EnabledIfSystemProperty(named = "java.vm.name", matches = IS_ROCKIT_VM)
    void onRockitVM() {
      final List<String> jvmOptions = new ArrayList<>();
      addJvmOptionsForOutOfMemoryErrors(jvmOptions);
      assertThat(jvmOptions)
          .containsExactly("-XXexitOnOutOfMemory");
    }

    @Test
    @DisabledIfSystemProperty(named = "java.vm.name", matches = IS_HOTSPOT_VM)
    @DisabledIfSystemProperty(named = "java.vm.name", matches = IS_J9_VM)
    @DisabledIfSystemProperty(named = "java.vm.name", matches = IS_ROCKIT_VM)
    void otherVM() {
      final List<String> jvmOptions = new ArrayList<>();
      addJvmOptionsForOutOfMemoryErrors(jvmOptions);
      assertThat(jvmOptions)
          .isEmpty();
    }
  }

  @Nested
  class GetServerClasspath {
    private final StartServerCommand serverCommands = new StartServerCommand();

    @Test
    public void addsUserClasspathImmediatelyAfterGemfireJarPath() {
      String userClasspath = "/path/to/user/lib/app.jar:/path/to/user/classes";

      String actualClasspath = serverCommands.getServerClasspath(false, userClasspath);

      String expectedClasspath = String.join(
          File.pathSeparator,
          StartMemberUtils.getGemFireJarPath(),
          userClasspath,
          StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);
      assertThat(actualClasspath)
          .isEqualTo(expectedClasspath);
    }
  }

  @Nested
  class CreateStartServerCommandLine {
    private static final String SERVER_LAUNCHER_CLASS_NAME =
        "org.apache.geode.distributed.ServerLauncher";

    private final StartServerCommand serverCommands = new StartServerCommand();
    private final Set<String> expectedJvmOptions = new HashSet<>();
    private final List<String> expectedStartCommandSequence = new ArrayList<>();
    private final Set<String> expectedStartCommandOptions = new HashSet<>();

    @BeforeEach
    void alwaysExpectUnconditionalOptions() {
      expectedJvmOptions.addAll(START_COMMAND_UNCONDITIONAL_JVM_OPTIONS);
    }

    @BeforeEach
    void alwaysExpectJreSpecificMemberJvmOptions() {
      expectedJvmOptions.addAll(getMemberJvmOptions());
    }

    @Test
    void withTypicalOptions() throws Exception {
      ServerLauncher.Builder serverLauncherBuilder = new ServerLauncher.Builder();
      serverLauncherBuilder.setCommand(START);
      expectedStartCommandSequence.add(SERVER_LAUNCHER_CLASS_NAME);
      expectedStartCommandSequence.add(START.getName());

      final String memberName = "with-typical-options";
      serverLauncherBuilder.setMemberName(memberName);
      expectedStartCommandSequence.add(memberName);

      serverLauncherBuilder.setDisableDefaultServer(true);
      expectedStartCommandOptions.add("--disable-default-server");

      serverLauncherBuilder.setRebalance(true);
      expectedStartCommandOptions.add("--rebalance");

      final int serverPort = 41214;
      serverLauncherBuilder.setServerPort(serverPort);
      expectedStartCommandOptions.add("--server-port=" + serverPort);

      final float criticalHeapPercentage = 95.5f;
      serverLauncherBuilder.setCriticalHeapPercentage(criticalHeapPercentage);
      expectedStartCommandOptions.add(
          String.format("--critical-heap-percentage=%s", criticalHeapPercentage));

      final float evictionHeapPercentage = 85.0f;
      serverLauncherBuilder.setEvictionHeapPercentage(evictionHeapPercentage);
      expectedStartCommandOptions.add(
          String.format("--eviction-heap-percentage=%s", evictionHeapPercentage));

      final int socketBufferSize = 1024 * 1024;
      serverLauncherBuilder.setSocketBufferSize(socketBufferSize);
      expectedStartCommandOptions.add(String.format("--socket-buffer-size=%d", socketBufferSize));

      final int messageTimeToLive = 93;
      serverLauncherBuilder.setMessageTimeToLive(messageTimeToLive);
      expectedStartCommandOptions.add(
          String.format("--message-time-to-live=%d", messageTimeToLive));

      ServerLauncher serverLauncher = serverLauncherBuilder.build();

      String expectedClasspath = String.join(
          File.pathSeparator,
          StartMemberUtils.getGemFireJarPath(),
          StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);

      List<String> expectedJavaCommandSequence = Arrays.asList(
          StartMemberUtils.getJavaPath(),
          "-server",
          "-classpath",
          expectedClasspath);

      boolean disableExitWhenOutOfMemory = false;
      expectedJvmOptions.addAll(jdkSpecificOutOfMemoryOptions());

      String[] commandLineElements = serverCommands.createStartServerCommandLine(
          serverLauncher, null, null, new Properties(), null, false, new String[0],
          disableExitWhenOutOfMemory, null,
          null, false);

      verifyCommandLine(commandLineElements, expectedJavaCommandSequence, expectedJvmOptions,
          expectedStartCommandSequence, expectedStartCommandOptions);
    }

    @Test
    void withRestApiOptions() throws Exception {
      ServerLauncher.Builder serverLauncherBuilder = new ServerLauncher.Builder();

      expectedStartCommandSequence.add(SERVER_LAUNCHER_CLASS_NAME);

      serverLauncherBuilder.setCommand(START);
      expectedStartCommandSequence.add(START.getName());

      final String memberName = "with-rest-api-options";
      serverLauncherBuilder.setMemberName(memberName);
      expectedStartCommandSequence.add(memberName);

      serverLauncherBuilder.setDisableDefaultServer(true);
      expectedStartCommandOptions.add("--disable-default-server");

      serverLauncherBuilder.setRebalance(true);
      expectedStartCommandOptions.add("--rebalance");

      final int serverPort = 41214;
      serverLauncherBuilder.setServerPort(serverPort);
      expectedStartCommandOptions.add("--server-port=" + serverPort);

      float criticalHeapPercentage = 95.5f;
      serverLauncherBuilder.setCriticalHeapPercentage(criticalHeapPercentage);
      expectedStartCommandOptions.add(
          String.format("--critical-heap-percentage=%s", criticalHeapPercentage));

      float evictionHeapPercentage = 85.0f;
      serverLauncherBuilder.setEvictionHeapPercentage(evictionHeapPercentage);
      expectedStartCommandOptions.add(
          String.format("--eviction-heap-percentage=%s", evictionHeapPercentage));

      ServerLauncher serverLauncher = serverLauncherBuilder.build();

      Properties gemfireProperties = new Properties();

      final String startDevRestApi = "true";
      gemfireProperties.setProperty(START_DEV_REST_API, startDevRestApi);
      expectedJvmOptions.add("-D" + GEMFIRE_PREFIX + START_DEV_REST_API + "=" + startDevRestApi);

      final String httpServicePort = "8080";
      gemfireProperties.setProperty(HTTP_SERVICE_PORT, httpServicePort);
      expectedJvmOptions.add("-D" + GEMFIRE_PREFIX + HTTP_SERVICE_PORT + "=" + httpServicePort);

      final String httpServiceBindAddress = "localhost";
      gemfireProperties.setProperty(HTTP_SERVICE_BIND_ADDRESS, httpServiceBindAddress);
      expectedJvmOptions.add("-D" + GEMFIRE_PREFIX + HTTP_SERVICE_BIND_ADDRESS
          + "=" + httpServiceBindAddress);

      final String expectedClasspath = String.join(
          File.pathSeparator,
          StartMemberUtils.getGemFireJarPath(),
          StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);

      List<String> expectedJavaCommandSequence = Arrays.asList(
          StartMemberUtils.getJavaPath(),
          "-server",
          "-classpath",
          expectedClasspath);

      boolean disableExitWhenOutOfMemory = false;
      expectedJvmOptions.addAll(jdkSpecificOutOfMemoryOptions());

      String[] commandLineElements = serverCommands.createStartServerCommandLine(
          serverLauncher, null, null, gemfireProperties, null, false, new String[0],
          disableExitWhenOutOfMemory, null,
          null, false);

      verifyCommandLine(commandLineElements, expectedJavaCommandSequence, expectedJvmOptions,
          expectedStartCommandSequence, expectedStartCommandOptions);
    }

    @Test
    void withAllOptions() throws Exception {
      ServerLauncher.Builder serverLauncherBuilder = new ServerLauncher.Builder();
      expectedStartCommandSequence.add(SERVER_LAUNCHER_CLASS_NAME);

      serverLauncherBuilder.setCommand(START);
      expectedStartCommandSequence.add(START.getName());

      final String memberName = "fullServer";
      serverLauncherBuilder.setMemberName(memberName);
      expectedStartCommandSequence.add(memberName);

      serverLauncherBuilder.setAssignBuckets(true);
      expectedStartCommandOptions.add("--assign-buckets");

      final float criticalHeapPercentage = 95.5f;
      serverLauncherBuilder.setCriticalHeapPercentage(criticalHeapPercentage);
      expectedStartCommandOptions.add("--critical-heap-percentage=" + criticalHeapPercentage);

      final float criticalOffHeapPercentage = 95.5f;
      serverLauncherBuilder.setCriticalOffHeapPercentage(criticalOffHeapPercentage);
      expectedStartCommandOptions.add(
          "--critical-off-heap-percentage=" + criticalOffHeapPercentage);

      serverLauncherBuilder.setDebug(true);
      expectedStartCommandOptions.add("--debug");

      serverLauncherBuilder.setDisableDefaultServer(true);
      expectedStartCommandOptions.add("--disable-default-server");

      final float evictionHeapPercentage = 85.0f;
      serverLauncherBuilder.setEvictionHeapPercentage(evictionHeapPercentage);
      expectedStartCommandOptions.add("--eviction-heap-percentage=" + evictionHeapPercentage);

      final float evictionOffHeapPercentage = 85.0f;
      serverLauncherBuilder.setEvictionOffHeapPercentage(evictionOffHeapPercentage);
      expectedStartCommandOptions.add(
          "--eviction-off-heap-percentage=" + evictionOffHeapPercentage);

      serverLauncherBuilder.setForce(true);
      expectedStartCommandOptions.add("--force");

      final String hostNameForClients = "localhost";
      serverLauncherBuilder.setHostNameForClients(hostNameForClients);
      expectedStartCommandOptions.add("--hostname-for-clients=" + hostNameForClients);

      final int maxConnections = 800;
      serverLauncherBuilder.setMaxConnections(maxConnections);
      expectedStartCommandOptions.add("--max-connections=" + maxConnections);

      final int maxMessageCount = 500;
      serverLauncherBuilder.setMaxMessageCount(maxMessageCount);
      expectedStartCommandOptions.add("--max-message-count=" + maxMessageCount);

      final int maxThreads = 100;
      serverLauncherBuilder.setMaxThreads(maxThreads);
      expectedStartCommandOptions.add("--max-threads=" + maxThreads);

      final int messageTimeToLive = 93;
      serverLauncherBuilder.setMessageTimeToLive(messageTimeToLive);
      expectedStartCommandOptions.add("--message-time-to-live=" + messageTimeToLive);

      serverLauncherBuilder.setRebalance(true);
      expectedStartCommandOptions.add("--rebalance");

      serverLauncherBuilder.setRedirectOutput(true);
      expectedStartCommandOptions.add("--redirect-output");
      expectedJvmOptions.add("-Dgemfire.OSProcess.DISABLE_REDIRECTION_CONFIGURATION=true");

      final int serverPort = 41214;
      serverLauncherBuilder.setServerPort(serverPort);
      expectedStartCommandOptions.add("--server-port=" + serverPort);

      final int socketBufferSize = 1024 * 1024;
      serverLauncherBuilder.setSocketBufferSize(socketBufferSize);
      expectedStartCommandOptions.add("--socket-buffer-size=" + socketBufferSize);

      final String springXmlLocation = "/config/spring-server.xml";
      serverLauncherBuilder.setSpringXmlLocation(springXmlLocation);
      expectedStartCommandOptions.add("--spring-xml-location=" + springXmlLocation);

      ServerLauncher serverLauncher = serverLauncherBuilder.build();

      Properties gemfireProperties = new Properties();

      final String disableAutoReconnect = "true";
      gemfireProperties.setProperty(DISABLE_AUTO_RECONNECT, disableAutoReconnect);
      expectedJvmOptions.add("-Dgemfire.disable-auto-reconnect=" + disableAutoReconnect);

      final String statisticSampleRate = "1500";
      gemfireProperties.setProperty(STATISTIC_SAMPLE_RATE, statisticSampleRate);
      expectedJvmOptions.add("-Dgemfire.statistic-sample-rate=" + statisticSampleRate);

      final String propertiesFilePath = "/config/customGemfire.properties";
      File gemfirePropertiesFile = mock(File.class);
      when(gemfirePropertiesFile.getAbsolutePath())
          .thenReturn(propertiesFilePath);
      expectedJvmOptions.add("-DgemfirePropertyFile=" + propertiesFilePath);

      final String securityPropertiesFilePath = "/config/customGemfireSecurity.properties";
      File gemfireSecurityPropertiesFile = mock(File.class);
      when(gemfireSecurityPropertiesFile.getAbsolutePath())
          .thenReturn(securityPropertiesFilePath);
      expectedJvmOptions.add("-DgemfireSecurityPropertyFile=" + securityPropertiesFilePath);

      final String heapSize = "1024m";
      expectedJvmOptions.add("-Xms" + heapSize);
      expectedJvmOptions.add("-Xmx" + heapSize);
      expectedJvmOptions.addAll(getGcJvmOptions(emptyList()));

      final String[] customJvmOptions = {
          "-verbose:gc",
          "-Xloggc:member-gc.log",
          "-XX:+PrintGCDateStamps",
          "-XX:+PrintGCDetails",
      };
      expectedJvmOptions.addAll(Arrays.asList(customJvmOptions));

      final String customClasspath = "/temp/domain-1.0.0.jar";
      final String expectedClasspath = String.join(
          File.pathSeparator,
          StartMemberUtils.getGemFireJarPath(),
          customClasspath,
          StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);

      List<String> expectedJavaCommandSequence = Arrays.asList(
          StartMemberUtils.getJavaPath(),
          "-server",
          "-classpath",
          expectedClasspath);

      boolean disableExitWhenOutOfMemory = false;
      expectedJvmOptions.addAll(jdkSpecificOutOfMemoryOptions());

      String[] commandLineElements = serverCommands.createStartServerCommandLine(
          serverLauncher, gemfirePropertiesFile, gemfireSecurityPropertiesFile, gemfireProperties,
          customClasspath, false, customJvmOptions, disableExitWhenOutOfMemory, heapSize, heapSize,
          false);

      verifyCommandLine(commandLineElements, expectedJavaCommandSequence, expectedJvmOptions,
          expectedStartCommandSequence, expectedStartCommandOptions);
    }

    @Test
    void withClassloaderIsolation() throws Exception {
      String geodeHome = System.getenv("GEODE_HOME");
      ServerLauncher.Builder serverLauncherBuilder = new ServerLauncher.Builder();
      expectedStartCommandSequence.add("org.jboss.modules.Main");
      expectedStartCommandSequence.add("-mp");
      Path geodeHomePath = Paths.get(geodeHome);
      expectedStartCommandSequence
          .add(geodeHomePath.resolve("moduleDescriptors").normalize().toAbsolutePath()
              + File.pathSeparator +
              geodeHomePath.resolve("../../test").resolve("deployments").normalize()
                  .toAbsolutePath());
      expectedStartCommandSequence.add("geode");

      serverLauncherBuilder.setCommand(START);
      expectedStartCommandSequence.add(START.getName());

      final String memberName = "fullServer";
      serverLauncherBuilder.setMemberName(memberName);
      expectedStartCommandSequence.add(memberName);

      serverLauncherBuilder.setAssignBuckets(true);
      expectedStartCommandOptions.add("--assign-buckets");

      final float criticalHeapPercentage = 95.5f;
      serverLauncherBuilder.setCriticalHeapPercentage(criticalHeapPercentage);
      expectedStartCommandOptions.add("--critical-heap-percentage=" + criticalHeapPercentage);

      final float criticalOffHeapPercentage = 95.5f;
      serverLauncherBuilder.setCriticalOffHeapPercentage(criticalOffHeapPercentage);
      expectedStartCommandOptions.add(
          "--critical-off-heap-percentage=" + criticalOffHeapPercentage);

      serverLauncherBuilder.setDebug(true);
      expectedStartCommandOptions.add("--debug");

      serverLauncherBuilder.setDisableDefaultServer(true);
      expectedStartCommandOptions.add("--disable-default-server");

      final float evictionHeapPercentage = 85.0f;
      serverLauncherBuilder.setEvictionHeapPercentage(evictionHeapPercentage);
      expectedStartCommandOptions.add("--eviction-heap-percentage=" + evictionHeapPercentage);

      final float evictionOffHeapPercentage = 85.0f;
      serverLauncherBuilder.setEvictionOffHeapPercentage(evictionOffHeapPercentage);
      expectedStartCommandOptions.add(
          "--eviction-off-heap-percentage=" + evictionOffHeapPercentage);

      serverLauncherBuilder.setForce(true);
      expectedStartCommandOptions.add("--force");

      final String hostNameForClients = "localhost";
      serverLauncherBuilder.setHostNameForClients(hostNameForClients);
      expectedStartCommandOptions.add("--hostname-for-clients=" + hostNameForClients);

      final int maxConnections = 800;
      serverLauncherBuilder.setMaxConnections(maxConnections);
      expectedStartCommandOptions.add("--max-connections=" + maxConnections);

      final int maxMessageCount = 500;
      serverLauncherBuilder.setMaxMessageCount(maxMessageCount);
      expectedStartCommandOptions.add("--max-message-count=" + maxMessageCount);

      final int maxThreads = 100;
      serverLauncherBuilder.setMaxThreads(maxThreads);
      expectedStartCommandOptions.add("--max-threads=" + maxThreads);

      final int messageTimeToLive = 93;
      serverLauncherBuilder.setMessageTimeToLive(messageTimeToLive);
      expectedStartCommandOptions.add("--message-time-to-live=" + messageTimeToLive);

      serverLauncherBuilder.setRebalance(true);
      expectedStartCommandOptions.add("--rebalance");

      serverLauncherBuilder.setRedirectOutput(true);
      expectedStartCommandOptions.add("--redirect-output");
      expectedJvmOptions.add("-Dgemfire.OSProcess.DISABLE_REDIRECTION_CONFIGURATION=true");

      final int serverPort = 41214;
      serverLauncherBuilder.setServerPort(serverPort);
      expectedStartCommandOptions.add("--server-port=" + serverPort);

      final int socketBufferSize = 1024 * 1024;
      serverLauncherBuilder.setSocketBufferSize(socketBufferSize);
      expectedStartCommandOptions.add("--socket-buffer-size=" + socketBufferSize);

      final String springXmlLocation = "/config/spring-server.xml";
      serverLauncherBuilder.setSpringXmlLocation(springXmlLocation);
      expectedStartCommandOptions.add("--spring-xml-location=" + springXmlLocation);

      ServerLauncher serverLauncher = serverLauncherBuilder.build();

      Properties gemfireProperties = new Properties();

      final String disableAutoReconnect = "true";
      gemfireProperties.setProperty(DISABLE_AUTO_RECONNECT, disableAutoReconnect);
      expectedJvmOptions.add("-Dgemfire.disable-auto-reconnect=" + disableAutoReconnect);

      final String statisticSampleRate = "1500";
      gemfireProperties.setProperty(STATISTIC_SAMPLE_RATE, statisticSampleRate);
      expectedJvmOptions.add("-Dgemfire.statistic-sample-rate=" + statisticSampleRate);

      final String propertiesFilePath = "/config/customGemfire.properties";
      File gemfirePropertiesFile = mock(File.class);
      when(gemfirePropertiesFile.getAbsolutePath())
          .thenReturn(propertiesFilePath);
      expectedJvmOptions.add("-DgemfirePropertyFile=" + propertiesFilePath);

      final String securityPropertiesFilePath = "/config/customGemfireSecurity.properties";
      File gemfireSecurityPropertiesFile = mock(File.class);
      when(gemfireSecurityPropertiesFile.getAbsolutePath())
          .thenReturn(securityPropertiesFilePath);
      expectedJvmOptions.add("-DgemfireSecurityPropertyFile=" + securityPropertiesFilePath);

      final String heapSize = "1024m";
      expectedJvmOptions.add("-Xms" + heapSize);
      expectedJvmOptions.add("-Xmx" + heapSize);
      expectedJvmOptions.addAll(getGcJvmOptions(emptyList()));

      final String[] customJvmOptions = {
          "-verbose:gc",
          "-Xloggc:member-gc.log",
          "-XX:+PrintGCDateStamps",
          "-XX:+PrintGCDetails",
      };
      expectedJvmOptions.addAll(Arrays.asList(customJvmOptions));

      expectedJvmOptions.add("-Djboss.modules.system.pkgs=javax.management,java.lang.management");
      expectedJvmOptions.add(
          "-Dboot.module.loader=org.apache.geode.deployment.internal.modules.loader.GeodeModuleLoader");

      final String expectedClasspath = String.join(
          File.pathSeparator,
          Paths.get(geodeHome, "lib",
              "geode-jboss-extensions-" + GemFireVersion.getGemFireVersion() + ".jar")
              .normalize().toString(),
          resolveJBossJarFile(geodeHome));

      List<String> expectedJavaCommandSequence = Arrays.asList(
          StartMemberUtils.getJavaPath(),
          "-server",
          "-classpath",
          expectedClasspath);

      boolean disableExitWhenOutOfMemory = false;
      expectedJvmOptions.addAll(jdkSpecificOutOfMemoryOptions());

      String[] commandLineElements = serverCommands.createStartServerCommandLine(
          serverLauncher, gemfirePropertiesFile, gemfireSecurityPropertiesFile, gemfireProperties,
          null, false, customJvmOptions, disableExitWhenOutOfMemory, heapSize, heapSize,
          true);

      verifyCommandLine(commandLineElements, expectedJavaCommandSequence, expectedJvmOptions,
          expectedStartCommandSequence, expectedStartCommandOptions);
    }
  }

  private String resolveJBossJarFile(String geodeHome) {
    File[] files = Paths.get(geodeHome, "lib").toFile()
        .listFiles((dir, name) -> name.startsWith("jboss-modules-"));
    assertThat(files).isNotNull();
    assertThat(files.length).isEqualTo(1);
    return files[0].toPath().toString();
  }

  private static List<String> jdkSpecificOutOfMemoryOptions() {
    List<String> jdkSpecificOptions = new ArrayList<>();
    addJvmOptionsForOutOfMemoryErrors(jdkSpecificOptions);
    return jdkSpecificOptions;
  }
}
