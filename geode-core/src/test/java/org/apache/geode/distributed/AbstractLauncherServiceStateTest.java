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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.AbstractLauncher.ServiceState.TO_STRING_CLASS_PATH;
import static org.apache.geode.distributed.AbstractLauncher.ServiceState.TO_STRING_GEODE_VERSION;
import static org.apache.geode.distributed.AbstractLauncher.ServiceState.TO_STRING_JAVA_VERSION;
import static org.apache.geode.distributed.AbstractLauncher.ServiceState.TO_STRING_JVM_ARGUMENTS;
import static org.apache.geode.distributed.AbstractLauncher.ServiceState.TO_STRING_LOG_FILE;
import static org.apache.geode.distributed.AbstractLauncher.ServiceState.TO_STRING_PROCESS_ID;
import static org.apache.geode.distributed.AbstractLauncher.ServiceState.TO_STRING_UPTIME;
import static org.apache.geode.distributed.AbstractLauncher.Status.NOT_RESPONDING;
import static org.apache.geode.distributed.AbstractLauncher.Status.ONLINE;
import static org.apache.geode.distributed.AbstractLauncher.Status.STARTING;
import static org.apache.geode.distributed.AbstractLauncher.Status.STOPPED;
import static org.apache.geode.distributed.AbstractLauncher.Status.valueOfDescription;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.management.internal.util.JsonUtil;

/**
 * Unit tests for {@link AbstractLauncher.ServiceState}. Tests marshalling of ServiceState to and
 * from JSON.
 *
 * @since GemFire 7.0
 */
public class AbstractLauncherServiceStateTest {

  private static String serviceName;
  private static String name;
  private static int pid;
  private static long uptime;
  private static String workingDirectory;
  private static List<String> jvmArguments;
  private static String classpath;
  private static String gemfireVersion;
  private static String javaVersion;

  private TestLauncher launcher;

  @Before
  public void setUp() throws Exception {
    serviceName = "Test";
    pid = ProcessUtils.identifyPid();
    uptime = 123456789;
    name = AbstractLauncherServiceStateTest.class.getSimpleName();
    workingDirectory = new File(System.getProperty("user.dir")).getAbsolutePath();
    jvmArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
    classpath = ManagementFactory.getRuntimeMXBean().getClassPath();
    gemfireVersion = GemFireVersion.getGemFireVersion();
    javaVersion = System.getProperty("java.version");

    int port = 12345;
    InetAddress host = InetAddress.getLocalHost();

    launcher = new TestLauncher(host, port, name);
    launcher.setStatus(STARTING);
  }

  @After
  public void tearDown() throws Exception {
    serviceName = null;
    name = null;
    workingDirectory = null;
    jvmArguments = null;
    classpath = null;
    gemfireVersion = null;
    javaVersion = null;
  }

  @Test
  public void serviceStateMarshalsToAndFromJsonWhenStarting() {
    TestLauncher.TestState status = launcher.status();
    String json = status.toJson();
    validateJson(status, json);
    validateStatus(status, TestLauncher.TestState.fromJson(json));
  }

  @Test
  public void serviceStateMarshalsToAndFromJsonWhenNotResponding() {
    launcher.setStatus(NOT_RESPONDING);
    TestLauncher.TestState status = launcher.status();
    String json = status.toJson();
    validateJson(status, json);
    validateStatus(status, TestLauncher.TestState.fromJson(json));
  }

  @Test
  public void serviceStateMarshalsToAndFromJsonWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();
    String json = status.toJson();
    validateJson(status, json);
    validateStatus(status, TestLauncher.TestState.fromJson(json));
  }

  @Test
  public void serviceStateMarshalsToAndFromJsonWhenStopped() {
    launcher.setStatus(STOPPED);
    TestLauncher.TestState status = launcher.status();
    String json = status.toJson();
    validateJson(status, json);
    validateStatus(status, TestLauncher.TestState.fromJson(json));
  }

  @Test
  public void toStringContainsLineSeparatorsWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(System.lineSeparator());
  }

  @Test
  public void toStringDoesNotContainLineSeparatorsWhenNotResponding() {
    launcher.setStatus(NOT_RESPONDING);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(System.lineSeparator());
  }

  @Test
  public void toStringContainsLineSeparatorsWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(System.lineSeparator());
  }

  @Test
  public void toStringDoesNotContainLineSeparatorsWhenStopped() {
    launcher.setStatus(STOPPED);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(System.lineSeparator());
  }

  @Test
  public void toStringContainsProcessIdWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_PROCESS_ID + pid);
  }

  @Test
  public void toStringDoesNotContainProcessIdWhenNotResponding() {
    launcher.setStatus(NOT_RESPONDING);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringContainsProcessIdWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_PROCESS_ID + pid);
  }

  @Test
  public void toStringDoesNotContainProcessIdWhenStopped() {
    launcher.setStatus(STOPPED);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringContainsJavaVersionWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringDoesNotContainJavaVersionWhenNotResponding() {
    launcher.setStatus(NOT_RESPONDING);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringContainsJavaVersionWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringDoesNotContainJavaVersionWhenStopped() {
    launcher.setStatus(STOPPED);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringContainsLogFileWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringDoesNotContainLogFileWhenNotResponding() {
    launcher.setStatus(NOT_RESPONDING);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringContainsLogFileWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringDoesNotContainLogFileWhenStopped() {
    launcher.setStatus(STOPPED);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_PROCESS_ID);
  }

  @Test
  public void toStringContainsJvmArgumentsWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_JVM_ARGUMENTS);
  }

  @Test
  public void toStringDoesNotContainJvmArgumentsWhenNotResponding() {
    launcher.setStatus(NOT_RESPONDING);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_JVM_ARGUMENTS);
  }

  @Test
  public void toStringContainsJvmArgumentsWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_JVM_ARGUMENTS);
  }

  @Test
  public void toStringDoesNotContainJvmArgumentsWhenStopped() {
    launcher.setStatus(STOPPED);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_JVM_ARGUMENTS);
  }

  @Test
  public void toStringContainsClassPathWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_CLASS_PATH);
  }

  @Test
  public void toStringDoesNotContainClassPathWhenNotResponding() {
    launcher.setStatus(NOT_RESPONDING);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_CLASS_PATH);
  }

  @Test
  public void toStringContainsClassPathWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_CLASS_PATH);
  }

  @Test
  public void toStringDoesNotContainClassPathWhenStopped() {
    launcher.setStatus(STOPPED);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_CLASS_PATH);
  }

  @Test
  public void toStringDoesNotContainUptimeWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_UPTIME);
  }

  @Test
  public void toStringDoesNotContainUptimeWhenNotResponding() {
    launcher.setStatus(NOT_RESPONDING);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_UPTIME);
  }

  @Test
  public void toStringContainsUptimeWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_UPTIME);
  }

  @Test
  public void toStringDoesNotContainUptimeWhenStopped() {
    launcher.setStatus(STOPPED);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_UPTIME);
  }

  @Test
  public void toStringDoesNotContainGeodeVersionWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_GEODE_VERSION);
  }

  @Test
  public void toStringDoesNotContainGeodeVersionWhenNotResponding() {
    launcher.setStatus(NOT_RESPONDING);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_GEODE_VERSION);
  }

  @Test
  public void toStringContainsGeodeVersionWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).contains(TO_STRING_GEODE_VERSION);
  }

  @Test
  public void toStringDoesNotContainGeodeVersionWhenStopped() {
    launcher.setStatus(STOPPED);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();

    assertThat(result).doesNotContain(TO_STRING_GEODE_VERSION);
  }

  @Test
  public void processIdStartsOnNewLineInToStringWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String processId = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_PROCESS_ID)) {
        processId = line;
        break;
      }
    }

    assertThat(processId)
        .as(TO_STRING_PROCESS_ID + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_PROCESS_ID);
  }

  @Test
  public void processIdStartsOnNewLineInToStringWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String processId = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_PROCESS_ID)) {
        processId = line;
        break;
      }
    }

    assertThat(processId)
        .as(TO_STRING_PROCESS_ID + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_PROCESS_ID);
  }

  @Test
  public void uptimeStartsOnNewLineInToStringWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String uptime = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_UPTIME)) {
        uptime = line;
        break;
      }
    }

    assertThat(uptime)
        .as(TO_STRING_UPTIME + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_UPTIME);
  }

  @Test
  public void geodeVersionStartsOnNewLineInToStringWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String geodeVersion = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_GEODE_VERSION)) {
        geodeVersion = line;
        break;
      }
    }

    assertThat(geodeVersion)
        .as(TO_STRING_GEODE_VERSION + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_GEODE_VERSION);
  }

  @Test
  public void javaVersionStartsOnNewLineInToStringWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String javaVersion = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_JAVA_VERSION)) {
        javaVersion = line;
        break;
      }
    }

    assertThat(javaVersion)
        .as(TO_STRING_JAVA_VERSION + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_JAVA_VERSION);
  }

  @Test
  public void javaVersionStartsOnNewLineInToStringWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String javaVersion = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_JAVA_VERSION)) {
        javaVersion = line;
        break;
      }
    }

    assertThat(javaVersion)
        .as(TO_STRING_JAVA_VERSION + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_JAVA_VERSION);
  }

  @Test
  public void logFileStartsOnNewLineInToStringWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String logFile = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_LOG_FILE)) {
        logFile = line;
        break;
      }
    }

    assertThat(logFile)
        .as(TO_STRING_LOG_FILE + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_LOG_FILE);
  }

  @Test
  public void logFileStartsOnNewLineInToStringWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String logFile = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_LOG_FILE)) {
        logFile = line;
        break;
      }
    }

    assertThat(logFile)
        .as(TO_STRING_LOG_FILE + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_LOG_FILE);
  }

  @Test
  public void jvmArgumentsStartsOnNewLineInToStringWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String jvmArguments = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_JVM_ARGUMENTS)) {
        jvmArguments = line;
        break;
      }
    }

    assertThat(jvmArguments)
        .as(TO_STRING_JVM_ARGUMENTS + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_JVM_ARGUMENTS);
  }

  @Test
  public void jvmArgumentsStartsOnNewLineInToStringWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String jvmArguments = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_JVM_ARGUMENTS)) {
        jvmArguments = line;
        break;
      }
    }

    assertThat(jvmArguments)
        .as(TO_STRING_JVM_ARGUMENTS + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_JVM_ARGUMENTS);
  }

  @Test
  public void classPathStartsOnNewLineInToStringWhenStarting() {
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String classPath = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_CLASS_PATH)) {
        classPath = line;
        break;
      }
    }

    assertThat(classPath)
        .as(TO_STRING_CLASS_PATH + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_CLASS_PATH);
  }

  @Test
  public void classPathStartsOnNewLineInToStringWhenOnline() {
    launcher.setStatus(ONLINE);
    TestLauncher.TestState status = launcher.status();

    String result = status.toString();
    List<String> lines = Arrays.asList(StringUtils.split(result, System.lineSeparator()));

    String classPath = null;
    for (String line : lines) {
      if (line.contains(TO_STRING_CLASS_PATH)) {
        classPath = line;
        break;
      }
    }

    assertThat(classPath)
        .as(TO_STRING_CLASS_PATH + " line in " + lines)
        .isNotNull()
        .startsWith(TO_STRING_CLASS_PATH);
  }

  private void validateStatus(final TestLauncher.TestState expected,
      final TestLauncher.TestState actual) {
    assertThat(actual.getClasspath()).isEqualTo(expected.getClasspath());
    assertThat(actual.getGemFireVersion()).isEqualTo(expected.getGemFireVersion());
    assertThat(actual.getJavaVersion()).isEqualTo(expected.getJavaVersion());
    assertThat(actual.getJvmArguments()).isEqualTo(expected.getJvmArguments());
    assertThat(actual.getPid()).isEqualTo(expected.getPid());
    assertThat(actual.getStatus()).isEqualTo(expected.getStatus());
    assertThat(actual.getTimestamp()).isEqualTo(expected.getTimestamp());
    assertThat(actual.getUptime()).isEqualTo(expected.getUptime());
    assertThat(actual.getWorkingDirectory()).isEqualTo(expected.getWorkingDirectory());
    assertThat(actual.getHost()).isEqualTo(expected.getHost());
    assertThat(actual.getPort()).isEqualTo(expected.getPort());
    assertThat(actual.getMemberName()).isEqualTo(expected.getMemberName());
  }

  private void validateJson(final TestLauncher.TestState expected, final String json) {
    TestLauncher.TestState actual = TestLauncher.TestState.fromJson(json);
    validateStatus(expected, actual);
  }

  private static class TestLauncher extends AbstractLauncher<String> {

    private final InetAddress bindAddress;
    private final int port;
    private final String memberName;
    private final File logFile;

    private Status status;

    TestLauncher(final InetAddress bindAddress, final int port, final String memberName) {
      this.bindAddress = bindAddress;
      this.port = port;
      this.memberName = memberName;
      logFile = new File(memberName + ".log");
    }

    public TestState status() {
      return new TestState(status, null, System.currentTimeMillis(), getId(), pid, uptime,
          workingDirectory, jvmArguments, classpath, gemfireVersion, javaVersion, getLogFileName(),
          getBindAddressAsString(), getPortAsString(), name);
    }

    @Override
    public void run() {
      // nothing
    }

    public String getId() {
      return getServiceName() + "@" + getBindAddress() + "[" + getPort() + "]";
    }

    @Override
    public String getLogFileName() {
      try {
        return logFile.getCanonicalPath();
      } catch (IOException e) {
        return logFile.getAbsolutePath();
      }
    }

    @Override
    public String getMemberName() {
      return memberName;
    }

    @Override
    public Integer getPid() {
      return null;
    }

    @Override
    public String getServiceName() {
      return serviceName;
    }

    InetAddress getBindAddress() {
      return bindAddress;
    }

    String getBindAddressAsString() {
      return bindAddress.getCanonicalHostName();
    }

    int getPort() {
      return port;
    }

    String getPortAsString() {
      return String.valueOf(getPort());
    }

    void setStatus(Status status) {
      this.status = status;
    }

    private static class TestState extends ServiceState<String> {

      protected static TestState fromJson(final String json) {
        try {
          JsonNode jsonObject = new ObjectMapper().readTree(json);

          Status status = valueOfDescription(jsonObject.get(JSON_STATUS).asText());
          List<String> jvmArguments = JsonUtil.toStringList(jsonObject.get(JSON_JVMARGUMENTS));

          return new TestState(status, jsonObject.get(JSON_STATUSMESSAGE).asText(),
              jsonObject.get(JSON_TIMESTAMP).asLong(), jsonObject.get(JSON_LOCATION).asText(),
              jsonObject.get(JSON_PID).asInt(), jsonObject.get(JSON_UPTIME).asLong(),
              jsonObject.get(JSON_WORKINGDIRECTORY).asText(), jvmArguments,
              jsonObject.get(JSON_CLASSPATH).asText(), jsonObject.get(JSON_GEMFIREVERSION).asText(),
              jsonObject.get(JSON_JAVAVERSION).asText(), jsonObject.get(JSON_LOGFILE).asText(),
              jsonObject.get(JSON_HOST).asText(), jsonObject.get(JSON_PORT).asText(),
              jsonObject.get(JSON_MEMBERNAME).asText());
        } catch (Exception e) {
          throw new IllegalArgumentException("Unable to create TestState from JSON: " + json);
        }
      }

      protected TestState(final Status status, final String statusMessage, final long timestamp,
          final String location, final Integer pid, final Long uptime,
          final String workingDirectory, final List<String> jvmArguments, final String classpath,
          final String gemfireVersion, final String javaVersion, final String logFile,
          final String host, final String port, final String name) {
        super(status, statusMessage, timestamp, location, pid, uptime, workingDirectory,
            jvmArguments, classpath, gemfireVersion, javaVersion, logFile, host, port, name);
      }

      @Override
      protected String getServiceName() {
        return serviceName;
      }
    }
  }
}
