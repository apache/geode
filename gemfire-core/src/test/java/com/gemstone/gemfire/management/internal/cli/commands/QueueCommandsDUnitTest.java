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
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A distributed test suite of test cases for testing the queue commands that are part of Gfsh.
 *
 * @author David Hoots
 * @since 8.0
 */
public class QueueCommandsDUnitTest extends CliCommandTestBase {
  private static final long serialVersionUID = 1L;

  final List<String> filesToBeDeleted = new CopyOnWriteArrayList<String>();

  public QueueCommandsDUnitTest(final String testName) {
    super(testName);
  }

  public void setUp() throws Exception {
    disconnectAllFromDS();
    super.setUp();
  }

  public void testAsyncEventQueue() throws IOException {
    final String queue1Name = "testAsyncEventQueue1";
    final String queue2Name = "testAsyncEventQueue2";
    final String diskStoreName = "testAsyncEventQueueDiskStore";

    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group0");
    createDefaultSetup(localProps);

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
        localProps.setProperty(DistributionConfig.NAME_NAME, vm1Name);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
        getSystem(localProps);
        getCache();
      }
    });

    final VM vm2 = Host.getHost(0).getVM(2);
    final String vm2Name = "VM" + vm2.getPid();
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, vm2Name);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group2");
        getSystem(localProps);
        getCache();
      }
    });

    // Deploy a JAR file with an AsyncEventListener/GatewayEventFilter/GatewayEventSubstitutionFilter
    // that can be instantiated on each server
    final File jarFile = new File(new File(".").getAbsolutePath(), "QueueCommandsDUnit.jar");
    QueueCommandsDUnitTest.this.filesToBeDeleted.add(jarFile.getAbsolutePath());

    ClassBuilder classBuilder = new ClassBuilder();
    byte[] jarBytes = classBuilder.createJarFromClassContent("com/qcdunit/QueueCommandsDUnitTestHelper",
        "package com.qcdunit;" +
            "import java.util.List; import java.util.Properties;" +
            "import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2; import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;" +
            "import com.gemstone.gemfire.cache.wan.GatewayEventFilter; import com.gemstone.gemfire.cache.wan.GatewayEventSubstitutionFilter;" +
            "import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener; import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;" +
            "import com.gemstone.gemfire.cache.EntryEvent;" +
            "public class QueueCommandsDUnitTestHelper implements Declarable2, GatewayEventFilter, GatewayEventSubstitutionFilter, AsyncEventListener {" +
            "Properties props;" +
            "public boolean processEvents(List<AsyncEvent> events) { return true; }" +
            "public void afterAcknowledgement(GatewayQueueEvent event) {}" +
            "public boolean beforeEnqueue(GatewayQueueEvent event) { return true; }" +
            "public boolean beforeTransmit(GatewayQueueEvent event) { return true; }" +
            "public Object getSubstituteValue(EntryEvent event) { return null; }" +
            "public void close() {}" +
            "public void init(final Properties props) {this.props = props;}" +
            "public Properties getConfig() {return this.props;}}");
    writeJarBytesToFile(jarFile, jarBytes);

    cmdResult = executeCommand("deploy --jar=QueueCommandsDUnit.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStoreName);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskStoreDir.getAbsolutePath());
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
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY, "213");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL, "946");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL, "true");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION, "true");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS, "2");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY, "PARTITION");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER,
        "com.qcdunit.QueueCommandsDUnitTestHelper");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER,
        "com.qcdunit.QueueCommandsDUnitTestHelper");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS, "false");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER,
        "com.qcdunit.QueueCommandsDUnitTestHelper");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE, "param1");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE, "param2#value2");
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
    assertTrue(stringContainsLine(stringResult,
        vm1Name + " .*" + queue1Name + " .*514 .*true .*" + diskStoreName + " .*213 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper" + ".*"));
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
        assertEquals(queue.getAsyncEventListener().getClass().getName(), "com.qcdunit.QueueCommandsDUnitTestHelper");
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
    assertTrue(stringContainsLine(stringResult,
        "Manager .*" + queue2Name + " .*100 .*false .*null .*100 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper"));
    assertTrue(stringContainsLine(stringResult,
        vm1Name + " .*" + queue1Name + " .*514 .*true .*" + diskStoreName + " .*213 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper" + ".*"));
    assertTrue(stringContainsLine(stringResult,
        vm1Name + " .*" + queue2Name + " .*100 .*false .*null .*100 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper"));
    assertTrue(stringContainsLine(stringResult,
        vm2Name + " .*" + queue2Name + " .*100 .*false .*null .*100 .*" + " .*com.qcdunit.QueueCommandsDUnitTestHelper"));
  }

  /**
   * Asserts that creating async event queues correctly updates the shared configuration.
   */
  public void testCreateUpdatesSharedConfig() throws IOException {
    disconnectAllFromDS();

    final String queueName = "testAsyncEventQueueQueue";
    final String groupName = "testAsyncEventQueueSharedConfigGroup";

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {

        final File locatorLogFile = new File("locator-" + locatorPort + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, "Locator");
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
        locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locatorPort, locatorLogFile, null,
              locatorProps);

          DistributedTestCase.WaitCriterion wc = new DistributedTestCase.WaitCriterion() {
            @Override
            public boolean done() {
              return locator.isSharedConfigurationRunning();
            }

            @Override
            public String description() {
              return "Waiting for shared configuration to be started";
            }
          };
          DistributedTestCase.waitForCriterion(wc, 5000, 500, true);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }
      }
    });

    // Start the default manager
    Properties managerProps = new Properties();
    managerProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    managerProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
    createDefaultSetup(managerProps);

    // Create a cache in VM 1
    VM vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
        getSystem(localProps);
        assertNotNull(getCache());
      }
    });

    // Deploy a JAR file with an AsyncEventListener that can be instantiated on each server
    final File jarFile = new File(new File(".").getAbsolutePath(), "QueueCommandsDUnit.jar");
    QueueCommandsDUnitTest.this.filesToBeDeleted.add(jarFile.getAbsolutePath());

    ClassBuilder classBuilder = new ClassBuilder();
    byte[] jarBytes = classBuilder.createJarFromClassContent("com/qcdunit/QueueCommandsDUnitTestListener",
        "package com.qcdunit;" +
            "import java.util.List; import java.util.Properties;" +
            "import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2; import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;" +
            "import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;" +
            "public class QueueCommandsDUnitTestListener implements Declarable2, AsyncEventListener {" +
            "Properties props;" +
            "public boolean processEvents(List<AsyncEvent> events) { return true; }" +
            "public void close() {}" +
            "public void init(final Properties props) {this.props = props;}" +
            "public Properties getConfig() {return this.props;}}");
    writeJarBytesToFile(jarFile, jarBytes);

    CommandResult cmdResult = executeCommand("deploy --jar=QueueCommandsDUnit.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Test creating the queue
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, queueName);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, groupName);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER,
        "com.qcdunit.QueueCommandsDUnitTestListener");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the queue exists in the shared config
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        String xmlFromConfig;
        try {
          xmlFromConfig = sharedConfig.getConfiguration(groupName).getCacheXmlContent();
          assertTrue(xmlFromConfig.contains(queueName));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });

    //Close cache in the vm1 and restart it to get the shared configuration
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        assertNotNull(cache);
        cache.close();

        assertTrue(cache.isClosed());

        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
        localProps.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "true");
        getSystem(localProps);
        cache = getCache();
        assertNotNull(cache);
        AsyncEventQueue aeq = cache.getAsyncEventQueue(queueName);

        assertNotNull(aeq);
      }
    });
  }

  @Override
  public void tearDown2() throws Exception {
    for (String path : this.filesToBeDeleted) {
      try {
        final File fileToDelete = new File(path);
        FileUtil.delete(fileToDelete);
        if (path.endsWith(".jar")) {
          executeCommand("undeploy --jar=" + fileToDelete.getName());
        }
      } catch (IOException e) {
        getLogWriter().error("Unable to delete file", e);
      }
    }
    this.filesToBeDeleted.clear();
    super.tearDown2();
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }
}
