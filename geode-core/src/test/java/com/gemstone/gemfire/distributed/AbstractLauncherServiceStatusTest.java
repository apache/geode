/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests marshaling of ServiceStatus to and from JSON.
 * 
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class AbstractLauncherServiceStatusTest {

  private static String serviceName;
  private static InetAddress host;
  private static int port;
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
    port = 12345;
    host = InetAddress.getLocalHost();
    pid = ProcessUtils.identifyPid();
    uptime = 123456789;
    name = AbstractLauncherServiceStatusTest.class.getSimpleName();
    workingDirectory = new File(System.getProperty("user.dir")).getAbsolutePath();
    jvmArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
    classpath = ManagementFactory.getRuntimeMXBean().getClassPath();
    gemfireVersion = GemFireVersion.getGemFireVersion();
    javaVersion = System.getProperty("java.version");

    this.launcher = new TestLauncher(host, port, name);
  }

  @After
  public void tearDown() throws Exception {
    serviceName = null;
    host = null;
    name = null;
    workingDirectory = null;
    jvmArguments = null;
    classpath = null;
    gemfireVersion = null;
    javaVersion = null;
  }

  @Test
  public void testMarshallingTestStatusToAndFromJson() {
    final TestLauncher.TestState status = this.launcher.status();
    final String json = status.toJson();
    validateJson(status, json);
    validateStatus(status, TestLauncher.TestState.fromJson(json));
  }

  private void validateStatus(final TestLauncher.TestState expected, final TestLauncher.TestState actual) {
    assertEquals(expected.getClasspath(), actual.getClasspath());
    assertEquals(expected.getGemFireVersion(), actual.getGemFireVersion());
    assertEquals(expected.getJavaVersion(), actual.getJavaVersion());
    assertEquals(expected.getJvmArguments(), actual.getJvmArguments());
    assertEquals(expected.getPid(), actual.getPid());
    assertEquals(expected.getStatus(), actual.getStatus());
    assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertEquals(expected.getUptime(), actual.getUptime());
    assertEquals(expected.getWorkingDirectory(), actual.getWorkingDirectory());
    assertEquals(expected.getHost(), actual.getHost());
    assertEquals(expected.getPort(), actual.getPort());
    assertEquals(expected.getMemberName(), actual.getMemberName());
  }

  private void validateJson(final TestLauncher.TestState expected, final String json) {
    final TestLauncher.TestState actual = TestLauncher.TestState.fromJson(json);
    validateStatus(expected, actual);
  }

  private static class TestLauncher extends AbstractLauncher<String> {

    private final InetAddress bindAddress;
    private final int port;
    private final String memberName;
    private final File logFile;

    TestLauncher(InetAddress bindAddress,
                 int port,
                 String memberName) {
      this.bindAddress = bindAddress;
      this.port = port;
      this.memberName = memberName;
      this.logFile = new File(memberName + ".log");
    }

    public TestState status() {
      return new TestState(Status.ONLINE,
        null,
        System.currentTimeMillis(),
        getId(),
              pid,
              uptime,
              workingDirectory,
              jvmArguments,
              classpath,
              gemfireVersion,
              javaVersion,
        getLogFileName(),
        getBindAddressAsString(),
        getPortAsString(),
              name);
    }

    @Override
    public void run() {
    }

    public String getId() {
      return getServiceName() + "@" + getBindAddress() + "[" + getPort() + "]";
    }

    @Override
    public String getLogFileName() {
      try {
        return this.logFile.getCanonicalPath();
      }
      catch (IOException e) {
        return this.logFile.getAbsolutePath();
      }
    }

    @Override
    public String getMemberName() {
      return this.memberName;
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
      return this.bindAddress;
    }

    String getBindAddressAsString() {
      return this.bindAddress.getCanonicalHostName();
    }

    int getPort() {
      return this.port;
    }

    String getPortAsString() {
      return String.valueOf(getPort());
    }

    private static class TestState extends ServiceState<String> {

      protected static TestState fromJson(final String json) {
        try {
          final GfJsonObject gfJsonObject = new GfJsonObject(json);

          final Status status = Status.valueOfDescription(gfJsonObject.getString(JSON_STATUS));
          final List<String> jvmArguments =
            Arrays.asList(GfJsonArray.toStringArray(gfJsonObject.getJSONArray(JSON_JVMARGUMENTS)));

          return new TestState(status,
            gfJsonObject.getString(JSON_STATUSMESSAGE),
            gfJsonObject.getLong(JSON_TIMESTAMP),
            gfJsonObject.getString(JSON_LOCATION),
            gfJsonObject.getInt(JSON_PID),
            gfJsonObject.getLong(JSON_UPTIME),
            gfJsonObject.getString(JSON_WORKINGDIRECTORY),
            jvmArguments,
            gfJsonObject.getString(JSON_CLASSPATH),
            gfJsonObject.getString(JSON_GEMFIREVERSION),
            gfJsonObject.getString(JSON_JAVAVERSION),
            gfJsonObject.getString(JSON_LOGFILE),
            gfJsonObject.getString(JSON_HOST),
            gfJsonObject.getString(JSON_PORT),
            gfJsonObject.getString(JSON_MEMBERNAME));
        }
        catch (GfJsonException e) {
          throw new IllegalArgumentException("Unable to create TestState from JSON: " + json);
        }
      }

      protected TestState(final Status status,
                          final String statusMessage,
                          final long timestamp,
                          final String location,
                          final Integer pid,
                          final Long uptime,
                          final String workingDirectory,
                          final List<String> jvmArguments,
                          final String classpath,
                          final String gemfireVersion,
                          final String javaVersion,
                          final String logFile,
                          final String host,
                          final String port,
                          final String name) {
        super(status, statusMessage, timestamp, location, pid, uptime, workingDirectory, jvmArguments, classpath,
          gemfireVersion, javaVersion, logFile, host, port, name);
      }

      @Override
      protected String getServiceName() {
        return serviceName;
      }
    }
  }

}
