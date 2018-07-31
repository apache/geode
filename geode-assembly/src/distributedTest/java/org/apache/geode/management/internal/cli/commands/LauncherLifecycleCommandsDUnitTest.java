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

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.distributed.AbstractLauncher.ServiceState;
import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.Command;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.ServerLauncher.ServerState;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

/**
 * The LauncherLifecycleCommandsDUnitTest class is a test suite of integration tests testing the
 * contract and functionality of the GemFire launcher lifecycle commands inside Gfsh.
 *
 * @see javax.management.MBeanServerConnection
 * @see javax.management.remote.JMXConnector
 * @see org.apache.geode.distributed.AbstractLauncher
 * @see org.apache.geode.distributed.LocatorLauncher
 * @see org.apache.geode.distributed.ServerLauncher
 * @see org.apache.geode.internal.AvailablePortHelper
 * @see org.apache.geode.management.internal.cli.shell.Gfsh
 * @see org.apache.geode.management.internal.cli.commands.CliCommandTestBase
 * @see org.apache.geode.management.internal.cli.util.CommandStringBuilder
 * @since GemFire 7.0
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SuppressWarnings("serial")
public class LauncherLifecycleCommandsDUnitTest extends CliCommandTestBase {

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  protected static final DateFormat TIMESTAMP = new SimpleDateFormat("yyyyMMddHHmmssSSS");

  private final Queue<Integer> processIds = new ConcurrentLinkedDeque<>();

  protected static String getMemberId(final int jmxManagerPort, final String memberName)
      throws Exception {
    return getMemberId(InetAddress.getLocalHost().getHostName(), jmxManagerPort, memberName);
  }

  protected static String getMemberId(final String jmxManagerHost, final int jmxManagerPort,
      final String memberName) throws Exception {
    JMXConnector connector = null;

    try {
      connector = JMXConnectorFactory.connect(new JMXServiceURL(String.format(
          "service:jmx:rmi://%1$s/jndi/rmi://%1$s:%2$d/jmxrmi", jmxManagerHost, jmxManagerPort)));

      MBeanServerConnection connection = connector.getMBeanServerConnection();

      ObjectName objectNamePattern = ObjectName.getInstance("GemFire:type=Member,*");

      QueryExp query = Query.eq(Query.attr("Name"), Query.value(memberName));

      Set<ObjectName> objectNames = connection.queryNames(objectNamePattern, query);

      assertNotNull(objectNames);
      assertFalse(objectNames.isEmpty());
      assertEquals(1, objectNames.size());

      // final ObjectName objectName = ObjectName.getInstance("GemFire:type=Member,Name=" +
      // memberName);
      ObjectName objectName = objectNames.iterator().next();

      // System.err.printf("ObjectName for Member with Name (%1$s) is %2$s%n", memberName,
      // objectName);

      return ObjectUtils.toString(connection.getAttribute(objectName, "Id"));
    } finally {
      IOUtils.close(connector);
    }
  }

  @Override
  public final void postTearDown() throws Exception {
    Integer pid;

    while ((pid = processIds.poll()) != null) {
      if (ProcessUtils.isProcessAlive(pid)) {
        try {
          String killCommand = String.format("%1$s %2$d",
              SystemUtils.isWindows() ? "taskkill /F /PID" : "kill -9", pid);
          Runtime.getRuntime().exec(killCommand);
        } catch (Throwable ignore) {
        }
      }
    }
  }

  @SuppressWarnings("unused")
  protected void assertStatus(final LocatorState expectedStatus, final LocatorState actualStatus) {
    assertEquals(expectedStatus.getStatus(), actualStatus.getStatus());
    assertEquals(expectedStatus.getTimestamp(), actualStatus.getTimestamp());
    assertEquals(expectedStatus.getServiceLocation(), actualStatus.getServiceLocation());
    assertTrue(ObjectUtils.equalsIgnoreNull(expectedStatus.getPid(), actualStatus.getPid()));
    assertEquals(expectedStatus.getUptime(), actualStatus.getUptime());
    assertEquals(expectedStatus.getWorkingDirectory(), actualStatus.getWorkingDirectory());
    assertEquals(expectedStatus.getJvmArguments(), actualStatus.getJvmArguments());
    assertEquals(expectedStatus.getClasspath(), actualStatus.getClasspath());
    assertEquals(expectedStatus.getGemFireVersion(), actualStatus.getGemFireVersion());
    assertEquals(expectedStatus.getJavaVersion(), actualStatus.getJavaVersion());
  }

  protected String toString(final Result result) {
    assert result != null : "The Result object from the command execution cannot be null!";

    StringBuilder buffer = new StringBuilder(StringUtils.LINE_SEPARATOR);

    while (result.hasNextLine()) {
      buffer.append(result.nextLine());
      buffer.append(StringUtils.LINE_SEPARATOR);
    }

    return buffer.toString();
  }

  @Test
  public void test010StopLocatorUsingMemberNameIDWhenGfshIsNotConnected() {
    CommandResult result =
        executeCommand(CliStrings.STOP_LOCATOR + " --name=" + getTestMethodName());

    assertNotNull(result);
    assertEquals(Result.Status.ERROR, result.getStatus());
    assertEquals(
        CliStrings.format(CliStrings.STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, "Locator"),
        StringUtils.trim(toString(result)));
  }

  @Test
  public void test011StopLocatorUsingMemberName() throws IOException {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    final int jmxManagerPort = ports[0];
    final int locatorPort = ports[1];

    String pathname = (getClass().getSimpleName() + "_" + getTestMethodName());
    File workingDirectory = temporaryFolder.newFolder(pathname);

    try {
      assertTrue(workingDirectory.isDirectory() || workingDirectory.mkdir());

      CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_LOCATOR);

      command.addOption(CliStrings.START_LOCATOR__MEMBER_NAME, pathname);
      command.addOption(CliStrings.START_LOCATOR__CONNECT, Boolean.FALSE.toString());
      command.addOption(CliStrings.START_LOCATOR__DIR, workingDirectory.getCanonicalPath());
      command.addOption(CliStrings.START_LOCATOR__PORT, String.valueOf(locatorPort));
      command.addOption(CliStrings.START_LOCATOR__ENABLE__SHARED__CONFIGURATION,
          Boolean.FALSE.toString());
      command.addOption(CliStrings.START_LOCATOR__FORCE, Boolean.TRUE.toString());
      command.addOption(CliStrings.START_LOCATOR__J,
          "-D" + DistributionConfig.GEMFIRE_PREFIX + "http-service-port=0");
      command.addOption(CliStrings.START_LOCATOR__J,
          "-D" + DistributionConfig.GEMFIRE_PREFIX + "jmx-manager-port=" + jmxManagerPort);

      CommandResult result = executeCommand(command.toString());

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      final LocatorLauncher locatorLauncher =
          new Builder().setCommand(Command.STOP).setBindAddress(null).setPort(locatorPort)
              .setWorkingDirectory(workingDirectory.getPath()).build();

      assertNotNull(locatorLauncher);

      LocatorState locatorStatus = locatorLauncher.waitOnStatusResponse(60, 10, TimeUnit.SECONDS);

      assertNotNull(locatorStatus);
      assertEquals(Status.ONLINE, locatorStatus.getStatus());

      result = executeCommand(
          String.format("%1$s --locator=localhost[%2$d]", CliStrings.CONNECT, locatorPort));

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      result = executeCommand(
          String.format("%1$s --name=invalidLocatorMemberName", CliStrings.STOP_LOCATOR));

      assertNotNull(result);
      assertEquals(Result.Status.ERROR, result.getStatus());
      assertEquals(
          CliStrings.format(CliStrings.STOP_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE,
              "invalidLocatorMemberName"),
          StringUtils.trim(toString(result)));

      locatorStatus = locatorLauncher.status();

      assertNotNull(locatorStatus);
      assertEquals(Status.ONLINE, locatorStatus.getStatus());

      result = executeCommand(String.format("%1$s --name=%2$s", CliStrings.STOP_LOCATOR, pathname));

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      // TODO figure out what output to assert and validate on now that 'stop locator' uses Gfsh's
      // logger
      // and standard err/out...
      // assertIndexDetailsEquals(CliStrings.format(CliStrings.STOP_LOCATOR__SHUTDOWN_MEMBER_MESSAGE,
      // pathname),
      // StringUtils.trim(toString(result)));

      WaitCriterion waitCriteria = new WaitCriterion() {
        @Override
        public boolean done() {
          final LocatorState locatorStatus = locatorLauncher.status();
          return (locatorStatus != null && Status.NOT_RESPONDING.equals(locatorStatus.getStatus()));
        }

        @Override
        public String description() {
          return "wait for the Locator to stop; the Locator will no longer respond after it stops";
        }
      };

      waitForCriterion(waitCriteria, 15 * 1000, 5000, true);

      locatorStatus = locatorLauncher.status();

      assertNotNull(locatorStatus);
      assertEquals(Status.NOT_RESPONDING, locatorStatus.getStatus());
    } finally {
    }

  }

  // @see Trac Bug # 46760
  @Test
  public void test012StopLocatorUsingMemberId() throws Exception {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    final int jmxManagerPort = ports[0];
    final int locatorPort = ports[1];

    String pathname = (getClass().getSimpleName() + "_" + getTestMethodName());
    File workingDirectory = temporaryFolder.newFolder(pathname);

    try {
      assertTrue(workingDirectory.isDirectory() || workingDirectory.mkdir());

      CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_LOCATOR);

      command.addOption(CliStrings.START_LOCATOR__MEMBER_NAME, pathname);
      command.addOption(CliStrings.START_LOCATOR__CONNECT, Boolean.FALSE.toString());
      command.addOption(CliStrings.START_LOCATOR__DIR, workingDirectory.getCanonicalPath());
      command.addOption(CliStrings.START_LOCATOR__PORT, String.valueOf(locatorPort));
      command.addOption(CliStrings.START_LOCATOR__ENABLE__SHARED__CONFIGURATION,
          Boolean.FALSE.toString());
      command.addOption(CliStrings.START_LOCATOR__FORCE, Boolean.TRUE.toString());
      command.addOption(CliStrings.START_LOCATOR__J,
          "-D" + DistributionConfig.GEMFIRE_PREFIX + "http-service-port=0");
      command.addOption(CliStrings.START_LOCATOR__J,
          "-D" + DistributionConfig.GEMFIRE_PREFIX + "jmx-manager-port=" + jmxManagerPort);

      CommandResult result = executeCommand(command.toString());

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      final LocatorLauncher locatorLauncher =
          new Builder().setCommand(Command.STOP).setBindAddress(null).setPort(locatorPort)
              .setWorkingDirectory(workingDirectory.getPath()).build();

      assertNotNull(locatorLauncher);

      LocatorState locatorState = locatorLauncher.waitOnStatusResponse(60, 10, TimeUnit.SECONDS);

      assertNotNull(locatorState);
      assertEquals(Status.ONLINE, locatorState.getStatus());

      result = executeCommand(
          String.format("%1$s --locator=localhost[%2$d]", CliStrings.CONNECT, locatorPort));

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      String memberId = getMemberId(jmxManagerPort, pathname);

      result = executeCommand(String.format("%1$s --name=%2$s", CliStrings.STOP_LOCATOR, memberId));

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      // TODO figure out what output to assert and validate on now that 'stop locator' uses Gfsh's
      // logger
      // and standard err/out...
      // assertIndexDetailsEquals(CliStrings.format(CliStrings.STOP_LOCATOR__SHUTDOWN_MEMBER_MESSAGE,
      // memberId),
      // StringUtils.trim(toString(result)));

      WaitCriterion waitCriteria = new WaitCriterion() {
        @Override
        public boolean done() {
          LocatorState locatorState = locatorLauncher.status();
          return (locatorState != null && Status.NOT_RESPONDING.equals(locatorState.getStatus()));
        }

        @Override
        public String description() {
          return "wait for the Locator to stop; the Locator will no longer respond after it stops";
        }
      };

      waitForCriterion(waitCriteria, 15 * 1000, 5000, true);

      locatorState = locatorLauncher.status();

      assertNotNull(locatorState);
      assertEquals(Status.NOT_RESPONDING, locatorState.getStatus());
    } finally {
    }
  }

  @Test
  @Category(FlakyTest.class) // GEODE-1866
  public void test014GemFireServerJvmProcessTerminatesOnOutOfMemoryError() throws Exception {
    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int serverPort = ports[0];
    final int locatorPort = ports[1];

    String pathname = getClass().getSimpleName().concat("_").concat(getTestMethodName());
    File workingDirectory = temporaryFolder.newFolder(pathname);

    assertTrue(workingDirectory.isDirectory() || workingDirectory.mkdir());

    CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_SERVER);

    command.addOption(CliStrings.START_SERVER__NAME,
        pathname + TIMESTAMP.format(Calendar.getInstance().getTime()));
    command.addOption(CliStrings.START_SERVER__SERVER_PORT, String.valueOf(serverPort));
    command.addOption(CliStrings.START_SERVER__USE_CLUSTER_CONFIGURATION, Boolean.FALSE.toString());
    command.addOption(CliStrings.START_SERVER__MAXHEAP, "12M");
    command.addOption(CliStrings.START_SERVER__LOG_LEVEL, "config");
    command.addOption(CliStrings.START_SERVER__DIR, workingDirectory.getCanonicalPath());
    command.addOption(CliStrings.START_SERVER__CACHE_XML_FILE,
        IOUtils.tryGetCanonicalPathElseGetAbsolutePath(writeAndGetCacheXmlFile(workingDirectory)));
    command.addOption(CliStrings.START_SERVER__INCLUDE_SYSTEM_CLASSPATH);
    command.addOption(CliStrings.START_SERVER__J, "-D" + DistributionConfig.GEMFIRE_PREFIX + ""
        + START_LOCATOR + "=localhost[" + locatorPort + "]");


    CommandResult result = executeCommand(command.toString());
    System.out.println("result=" + result);

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    ServerLauncher serverLauncher =
        new ServerLauncher.Builder().setCommand(ServerLauncher.Command.STATUS)
            .setWorkingDirectory(IOUtils.tryGetCanonicalPathElseGetAbsolutePath(workingDirectory))
            .build();

    assertNotNull(serverLauncher);

    ServerState serverState = serverLauncher.status();

    assertNotNull(serverState);
    assertEquals(Status.ONLINE, serverState.getStatus());

    // Verify our GemFire Server JVM process is running!
    assertTrue(serverState.isVmWithProcessIdRunning());

    ClientCache clientCache = setupClientCache(pathname + String.valueOf(serverPort), serverPort);

    assertNotNull(clientCache);

    try {
      Region<Long, String> exampleRegion = clientCache.getRegion("/Example");
      // run the GemFire Server "out-of-town" with an OutOfMemoryError!
      for (long index = 0; index < Long.MAX_VALUE; index++) {
        exampleRegion.put(index, String.valueOf(index));
      }
    } catch (Exception ignore) {
      System.err.printf("%1$s: %2$s%n", ignore.getClass().getName(), ignore.getMessage());
    } finally {
      clientCache.close();

      final int serverPid = serverState.getPid();

      WaitCriterion waitCriteria = new WaitCriterion() {
        @Override
        public boolean done() {
          return !ProcessUtils.isProcessAlive(serverPid);
        }

        @Override
        public String description() {
          return "Wait for the GemFire Server JVM process that ran out-of-memory to exit.";
        }
      };

      waitForCriterion(waitCriteria, TimeUnit.SECONDS.toMillis(30), TimeUnit.SECONDS.toMillis(10),
          true);

      // Verify our GemFire Server JVM process is was terminated!
      assertFalse(serverState.isVmWithProcessIdRunning());

      serverState = serverLauncher.status();

      assertNotNull(serverState);
      assertEquals(Status.NOT_RESPONDING, serverState.getStatus());
    }
  }

  private File writeAndGetCacheXmlFile(final File workingDirectory) throws IOException {
    File cacheXml = new File(workingDirectory, "cache.xml");
    StringBuilder buffer = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

    buffer.append(StringUtils.LINE_SEPARATOR);
    buffer.append(
        "<!DOCTYPE cache PUBLIC  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.0//EN\"");
    buffer.append(StringUtils.LINE_SEPARATOR);
    buffer.append("  \"http://www.gemstone.com/dtd/cache7_0.dtd\">");
    buffer.append(StringUtils.LINE_SEPARATOR);
    buffer.append("<cache>");
    buffer.append(StringUtils.LINE_SEPARATOR);
    buffer.append("  <region name=\"Example\" refid=\"REPLICATE\"/>");
    buffer.append(StringUtils.LINE_SEPARATOR);
    buffer.append("</cache>");

    BufferedWriter fileWriter = null;

    try {
      fileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(cacheXml, false),
          Charset.forName("UTF-8").newEncoder()));
      fileWriter.write(buffer.toString());
      fileWriter.flush();
    } finally {
      IOUtils.close(fileWriter);
    }

    return cacheXml;
  }

  private ClientCache setupClientCache(final String durableClientId, final int serverPort) {
    ClientCache clientCache =
        new ClientCacheFactory().set(DURABLE_CLIENT_ID, durableClientId).create();

    PoolFactory poolFactory = PoolManager.createFactory();

    poolFactory.setMaxConnections(10);
    poolFactory.setMinConnections(1);
    poolFactory.setReadTimeout(5000);
    poolFactory.addServer("localhost", serverPort);

    Pool pool = poolFactory.create("serverConnectionPool");

    assertNotNull("The 'serverConnectionPool' was not properly configured and initialized!", pool);

    ClientRegionFactory<Long, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);

    regionFactory.setPoolName(pool.getName());
    regionFactory.setKeyConstraint(Long.class);
    regionFactory.setValueConstraint(String.class);

    Region<Long, String> exampleProxy = regionFactory.create("Example");

    assertNotNull("The 'Example' Client Region was not properly configured and initialized",
        exampleProxy);

    return clientCache;
  }

}
