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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

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
  public void serviceStateCanBeMarshalledToAndFromJson() throws Exception {
    TestLauncher.TestState status = launcher.status();
    String json = status.toJson();
    validateJson(status, json);
    validateStatus(status, TestLauncher.TestState.fromJson(json));
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

    TestLauncher(final InetAddress bindAddress, final int port, final String memberName) {
      this.bindAddress = bindAddress;
      this.port = port;
      this.memberName = memberName;
      this.logFile = new File(memberName + ".log");
    }

    public TestState status() {
      return new TestState(Status.ONLINE, null, System.currentTimeMillis(), getId(), pid, uptime,
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

    private static class TestState extends ServiceState<String> {

      protected static TestState fromJson(final String json) {
        try {
          GfJsonObject gfJsonObject = new GfJsonObject(json);

          Status status = Status.valueOfDescription(gfJsonObject.getString(JSON_STATUS));
          List<String> jvmArguments = Arrays
              .asList(GfJsonArray.toStringArray(gfJsonObject.getJSONArray(JSON_JVMARGUMENTS)));

          return new TestState(status, gfJsonObject.getString(JSON_STATUSMESSAGE),
              gfJsonObject.getLong(JSON_TIMESTAMP), gfJsonObject.getString(JSON_LOCATION),
              gfJsonObject.getInt(JSON_PID), gfJsonObject.getLong(JSON_UPTIME),
              gfJsonObject.getString(JSON_WORKINGDIRECTORY), jvmArguments,
              gfJsonObject.getString(JSON_CLASSPATH), gfJsonObject.getString(JSON_GEMFIREVERSION),
              gfJsonObject.getString(JSON_JAVAVERSION), gfJsonObject.getString(JSON_LOGFILE),
              gfJsonObject.getString(JSON_HOST), gfJsonObject.getString(JSON_PORT),
              gfJsonObject.getString(JSON_MEMBERNAME));
        } catch (GfJsonException e) {
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
