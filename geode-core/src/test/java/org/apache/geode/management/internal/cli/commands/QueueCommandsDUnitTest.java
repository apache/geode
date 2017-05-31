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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;

import org.apache.commons.io.FileUtils;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A distributed test suite of test cases for testing the queue commands that are part of Gfsh.
 *
 * @since GemFire 8.0
 */
@Category(DistributedTest.class)
public class QueueCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  private final List<String> filesToBeDeleted = new CopyOnWriteArrayList<>();

  @Override
  public final void preSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Category(FlakyTest.class) // GEODE-1429
  @Test
  public void testAsyncEventQueue() throws IOException {
    final String queue1Name = "testAsyncEventQueue1";
    final String queue2Name = "testAsyncEventQueue2";
    final String diskStoreName = "testAsyncEventQueueDiskStore";

    Properties localProps = new Properties();
    localProps.setProperty(GROUPS, "Group0");
    setUpJmxManagerOnVm0ThenConnect(localProps);

    CommandResult cmdResult = executeCommand(CliStrings.LIST_ASYNC_EVENT_QUEUES);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No Async Event Queues Found"));

    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "VM" + vm1.getPid();
    final File diskStoreDir = new File(new File(".").getAbsolutePath(), diskStoreName);
    this.filesToBeDeleted.add(diskStoreDir.getAbsolutePath());
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        diskStoreDir.mkdirs();

        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm1Name);
        localProps.setProperty(GROUPS, "Group1");
        getSystem(localProps);
        getCache();
      }
    });

    final VM vm2 = Host.getHost(0).getVM(2);
    final String vm2Name = "VM" + vm2.getPid();
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm2Name);
        localProps.setProperty(GROUPS, "Group2");
        getSystem(localProps);
        getCache();
      }
    });

    // Deploy a JAR file with an
    // AsyncEventListener/GatewayEventFilter/GatewayEventSubstitutionFilter
    // that can be instantiated on each server
    final File jarFile = new File(new File(".").getAbsolutePath(), "QueueCommandsDUnit.jar");
    QueueCommandsDUnitTest.this.filesToBeDeleted.add(jarFile.getAbsolutePath());

    ClassBuilder classBuilder = new ClassBuilder();
    byte[] jarBytes =
        classBuilder.createJarFromClassContent("com/qcdunit/QueueCommandsDUnitTestHelper",
            "package com.qcdunit;" + "import java.util.List; import java.util.Properties;"
                + "import org.apache.geode.internal.cache.xmlcache.Declarable2; import org.apache.geode.cache.asyncqueue.AsyncEvent;"
                + "import org.apache.geode.cache.wan.GatewayEventFilter; import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;"
                + "import org.apache.geode.cache.asyncqueue.AsyncEventListener; import org.apache.geode.cache.wan.GatewayQueueEvent;"
                + "import org.apache.geode.cache.EntryEvent;"
                + "public class QueueCommandsDUnitTestHelper implements Declarable2, GatewayEventFilter, GatewayEventSubstitutionFilter, AsyncEventListener {"
                + "Properties props;"
                + "public boolean processEvents(List<AsyncEvent> events) { return true; }"
                + "public void afterAcknowledgement(GatewayQueueEvent event) {}"
                + "public boolean beforeEnqueue(GatewayQueueEvent event) { return true; }"
                + "public boolean beforeTransmit(GatewayQueueEvent event) { return true; }"
                + "public Object getSubstituteValue(EntryEvent event) { return null; }"
                + "public void close() {}"
                + "public void init(final Properties props) {this.props = props;}"
                + "public Properties getConfig() {return this.props;}}");
    writeJarBytesToFile(jarFile, jarBytes);

    cmdResult = executeCommand("deploy --jar=QueueCommandsDUnit.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    CommandStringBuilder commandStringBuilder =
        new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStoreName);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE,
        diskStoreDir.getAbsolutePath());
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Success"));

    commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, queue1Name);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, "514");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, "true");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE, diskStoreName);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY,
        "213");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL, "946");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL, "true");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION,
        "true");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS, "2");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY, "PARTITION");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER,
        "com.qcdunit.QueueCommandsDUnitTestHelper");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER,
        "com.qcdunit.QueueCommandsDUnitTestHelper");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS, "false");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY,
        "true");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER,
        "com.qcdunit.QueueCommandsDUnitTestHelper");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE,
        "param1,param2#value2");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Success"));

    // Verify that the queue was created on the correct member
    cmdResult = executeCommand(CliStrings.LIST_ASYNC_EVENT_QUEUES);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, vm1Name + " .*" + queue1Name + " .*514 .*true .*"
        + diskStoreName + " .*213 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper" + ".*"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*param2=value2.*"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*param1=[^\\w].*"));
    assertFalse(stringContainsLine(stringResult, vm2Name + ".*" + queue1Name + ".*"));

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        AsyncEventQueue queue = cache.getAsyncEventQueue(queue1Name);
        assertEquals(queue.getBatchSize(), 514);
        assertEquals(queue.isPersistent(), true);
        assertEquals(queue.getDiskStoreName(), diskStoreName);
        assertEquals(queue.getMaximumQueueMemory(), 213);
        assertEquals(queue.getBatchTimeInterval(), 946);
        assertEquals(queue.isParallel(), true);
        assertEquals(queue.isBatchConflationEnabled(), true);
        assertEquals(queue.getDispatcherThreads(), 2);
        assertEquals(queue.getOrderPolicy().toString(), "PARTITION");
        assertEquals(queue.getGatewayEventFilters().size(), 1);
        assertEquals(queue.getGatewayEventFilters().get(0).getClass().getName(),
            "com.qcdunit.QueueCommandsDUnitTestHelper");
        assertEquals(queue.getGatewayEventSubstitutionFilter().getClass().getName(),
            "com.qcdunit.QueueCommandsDUnitTestHelper");
        assertEquals(queue.isDiskSynchronous(), false);
        assertEquals(queue.isForwardExpirationDestroy(), true);
        assertEquals(queue.getAsyncEventListener().getClass().getName(),
            "com.qcdunit.QueueCommandsDUnitTestHelper");
      }
    });

    commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, queue2Name);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER,
        "com.qcdunit.QueueCommandsDUnitTestHelper");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(5, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Manager.*Success"));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*Success"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Success"));

    // Verify that the queue was created on the correct members
    cmdResult = executeCommand(CliStrings.LIST_ASYNC_EVENT_QUEUES);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(6, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Manager .*" + queue2Name
        + " .*100 .*false .*null .*100 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper"));
    assertTrue(stringContainsLine(stringResult, vm1Name + " .*" + queue1Name + " .*514 .*true .*"
        + diskStoreName + " .*213 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper" + ".*"));
    assertTrue(stringContainsLine(stringResult, vm1Name + " .*" + queue2Name
        + " .*100 .*false .*null .*100 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper"));
    assertTrue(stringContainsLine(stringResult, vm2Name + " .*" + queue2Name
        + " .*100 .*false .*null .*100 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper"));
  }

  /**
   * Asserts that creating async event queues correctly updates the shared configuration.
   */
  @Category(FlakyTest.class) // GEODE-1976
  @Test
  public void testCreateUpdatesSharedConfig() throws IOException {
    disconnectAllFromDS();
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    jmxPort = ports[0];
    httpPort = ports[1];
    try {
      jmxHost = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ignore) {
      jmxHost = "localhost";
    }

    final String queueName = "testAsyncEventQueueQueue";
    final String groupName = "testAsyncEventQueueSharedConfigGroup";

    final Properties locatorProps = new Properties();
    locatorProps.setProperty(NAME, "Locator");
    locatorProps.setProperty(MCAST_PORT, "0");
    locatorProps.setProperty(LOG_LEVEL, "fine");
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    locatorProps.setProperty(JMX_MANAGER, "true");
    locatorProps.setProperty(JMX_MANAGER_START, "true");
    locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS, String.valueOf(jmxHost));
    locatorProps.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    locatorProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        final File locatorLogFile = new File("locator-" + locatorPort + ".log");

        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locatorPort,
              locatorLogFile, null, locatorProps);

          WaitCriterion wc = new WaitCriterion() {
            @Override
            public boolean done() {
              return locator.isSharedConfigurationRunning();
            }

            @Override
            public String description() {
              return "Waiting for shared configuration to be started";
            }
          };
          waitForCriterion(wc, 5000, 500, true);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }
      }
    });

    connect(jmxHost, jmxPort, httpPort, getDefaultShell());

    // Create a cache in VM 1
    VM vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(GROUPS, groupName);
        getSystem(localProps);
        assertNotNull(getCache());
      }
    });

    // Deploy a JAR file with an AsyncEventListener that can be instantiated on each server
    final File jarFile = new File(new File(".").getAbsolutePath(), "QueueCommandsDUnit.jar");
    QueueCommandsDUnitTest.this.filesToBeDeleted.add(jarFile.getAbsolutePath());

    ClassBuilder classBuilder = new ClassBuilder();
    byte[] jarBytes =
        classBuilder.createJarFromClassContent("com/qcdunit/QueueCommandsDUnitTestListener",
            "package com.qcdunit;" + "import java.util.List; import java.util.Properties;"
                + "import org.apache.geode.internal.cache.xmlcache.Declarable2; import org.apache.geode.cache.asyncqueue.AsyncEvent;"
                + "import org.apache.geode.cache.asyncqueue.AsyncEventListener;"
                + "public class QueueCommandsDUnitTestListener implements Declarable2, AsyncEventListener {"
                + "Properties props;"
                + "public boolean processEvents(List<AsyncEvent> events) { return true; }"
                + "public void close() {}"
                + "public void init(final Properties props) {this.props = props;}"
                + "public Properties getConfig() {return this.props;}}");
    writeJarBytesToFile(jarFile, jarBytes);

    CommandResult cmdResult = executeCommand("deploy --jar=QueueCommandsDUnit.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Test creating the queue
    CommandStringBuilder commandStringBuilder =
        new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, queueName);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, groupName);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER,
        "com.qcdunit.QueueCommandsDUnitTestListener");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the queue exists in the shared config
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        ClusterConfigurationService sharedConfig =
            ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        String xmlFromConfig;
        try {
          xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
          assertTrue(xmlFromConfig.contains(queueName));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });

    // Close cache in the vm1 and restart it to get the shared configuration
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        assertNotNull(cache);
        cache.close();

        assertTrue(cache.isClosed());

        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(GROUPS, groupName);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        getSystem(localProps);
        cache = getCache();
        assertNotNull(cache);
        AsyncEventQueue aeq = cache.getAsyncEventQueue(queueName);

        assertNotNull(aeq);
      }
    });
  }

  @Override
  protected final void preTearDownCliCommandTestBase() throws Exception {
    for (String path : this.filesToBeDeleted) {
      try {
        final File fileToDelete = new File(path);
        if (fileToDelete.isDirectory())
          FileUtils.deleteDirectory(fileToDelete);
        else
          Files.delete(fileToDelete.toPath());
        if (path.endsWith(".jar")) {
          executeCommand("undeploy --jar=" + fileToDelete.getName());
        }
      } catch (IOException e) {
        getLogWriter().error("Unable to delete file", e);
      }
    }
    this.filesToBeDeleted.clear();
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }
}
