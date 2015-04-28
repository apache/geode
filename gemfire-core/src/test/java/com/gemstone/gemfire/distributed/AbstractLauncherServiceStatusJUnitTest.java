/* =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import com.gemstone.gemfire.distributed.AbstractLauncherServiceStatusJUnitTest.TestLauncher.TestState;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.process.PidUnavailableException;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.junit.UnitTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests marshaling of ServiceStatus to and from JSON.
 * @author Kirk Lund
 * @since 7.0
 */
@Category(UnitTest.class)
public class AbstractLauncherServiceStatusJUnitTest {

  private static final String SERVICE_NAME = "Test";

  private static InetAddress HOST;

  static {
    try {
      HOST = InetAddress.getLocalHost();
    }
    catch (UnknownHostException e) {
      HOST = null;
    }
  }

  private static final int PORT = 12345;
  private static final String NAME = AbstractLauncherServiceStatusJUnitTest.class.getSimpleName();

  private static final int PID = identifyPid();
  private static final long UPTIME = 123456789;
  private static final String WORKING_DIRECTORY = identifyWorkingDirectory();
  private static final List<String> JVM_ARGUMENTS = ManagementFactory.getRuntimeMXBean().getInputArguments();
  private static final String CLASSPATH = ManagementFactory.getRuntimeMXBean().getClassPath();
  private static final String GEMFIRE_VERSION = GemFireVersion.getGemFireVersion();
  private static final String JAVA_VERSION = System.getProperty("java.version");

  private TestLauncher launcher;

  @Before
  public void setUp() {
    this.launcher = new TestLauncher(HOST, PORT, NAME);
  }

  @After
  public void tearDown() {
    this.launcher = null;
  }

  @Test
  public void testMarshallingTestStatusToAndFromJson() {
    final TestState status = this.launcher.status();
    final String json = status.toJson();
    validateJson(status, json);
    validateStatus(status, TestState.fromJson(json));
  }

  private void validateStatus(final TestState expected, final TestState actual) {
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

  private void validateJson(final TestState expected, final String json) {
    final TestState actual = TestState.fromJson(json);
    validateStatus(expected, actual);
  }

  private static int identifyPid() {
    try {
      return ProcessUtils.identifyPid();
    }
    catch (PidUnavailableException e) {
      return 0;
    }
  }

  private static String identifyWorkingDirectory() {
    try {
      return new File(System.getProperty("user.dir")).getCanonicalPath();
    }
    catch (IOException e) {
      return new File(System.getProperty("user.dir")).getAbsolutePath();
    }
  }

  static class TestLauncher extends AbstractLauncher<String> {

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
        PID,
        UPTIME,
        WORKING_DIRECTORY,
        JVM_ARGUMENTS,
        CLASSPATH,
        GEMFIRE_VERSION,
        JAVA_VERSION,
        getLogFileName(),
        getBindAddressAsString(),
        getPortAsString(),
        NAME);
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
      return SERVICE_NAME;
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

    public static class TestState extends ServiceState<String> {

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
        return SERVICE_NAME;
      }
    }
  }

}
