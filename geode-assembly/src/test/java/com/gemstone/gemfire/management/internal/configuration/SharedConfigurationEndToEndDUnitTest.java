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
package com.gemstone.gemfire.management.internal.configuration;

import static com.gemstone.gemfire.cache.RegionShortcut.*;
import static com.gemstone.gemfire.distributed.internal.DistributionConfig.*;
import static com.gemstone.gemfire.internal.AvailablePortHelper.*;
import static com.gemstone.gemfire.internal.FileUtil.*;
import static com.gemstone.gemfire.internal.lang.StringUtils.*;
import static com.gemstone.gemfire.management.internal.cli.CliUtil.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;
import static com.gemstone.gemfire.test.dunit.Host.*;
import static com.gemstone.gemfire.test.dunit.IgnoredException.*;
import static com.gemstone.gemfire.test.dunit.LogWriterUtils.*;
import static com.gemstone.gemfire.test.dunit.Wait.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.internal.JarDeployer;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllRequest;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class SharedConfigurationEndToEndDUnitTest extends CliCommandTestBase {

  private static final int TIMEOUT = 10000;
  private static final int INTERVAL = 500;

  private static final String REGION1 = "R1";
  private static final String REGION2 = "R2";
  private static final String INDEX1 = "ID1";

  private static Set<String> serverNames;
  private static Set<String> jarFileNames;

  private transient ClassBuilder classBuilder;
  private transient String jmxHost;
  private transient int jmxPort;
  private transient int httpPort;
  private transient String locatorString;

  @Override
  public final void postSetUpCliCommandTestBase() throws Exception {
    disconnectAllFromDS();

    addIgnoredException("EntryDestroyedException");

    serverNames = new HashSet<>();
    jarFileNames = new HashSet<>();

    this.classBuilder = new ClassBuilder();

    Object[] result = setup();
    int locatorPort = (Integer) result[0];

    this.jmxHost = (String) result[1];
    this.jmxPort = (Integer) result[2];
    this.httpPort = (Integer) result[3];
    this.locatorString = "localHost[" + locatorPort + "]";
  }

  @Override
  public final void preTearDownCliCommandTestBase() throws Exception {
    //shutdown everything
    shutdownAll();

    serverNames.clear();
    jarFileNames.clear();

    serverNames = null;
    jarFileNames = null;
  }

  @Test
  public void testStartServerAndExecuteCommands() throws Exception {
    final HeadlessGfsh gfsh = new HeadlessGfsh("gfsh2", 300, this.gfshDir);
    assertNotNull(gfsh);
    connect(jmxHost, jmxPort, httpPort, gfsh);

    serverNames.addAll(startServers(gfsh, locatorString, 2, "Server", 1));
    doCreateCommands();
    serverNames.addAll(startServers(gfsh, locatorString, 1, "NewMember", 4));

    verifyRegionCreateOnAllMembers(REGION1);
    verifyRegionCreateOnAllMembers(REGION2);
    verifyIndexCreationOnAllMembers(INDEX1);
    verifyAsyncEventQueueCreation();
  }

  private Set<String> startServers(final HeadlessGfsh gfsh, final String locatorString, final int numServers, final String serverNamePrefix, final int startNum) throws ClassNotFoundException, IOException {
    Set<String> serverNames = new HashSet<>();

    final int[] serverPorts = getRandomAvailableTCPPorts(numServers);
    for (int i=0; i<numServers; i++) {
      int port = serverPorts[i];
      String serverName = serverNamePrefix+ Integer.toString(i+startNum) + "-" + port;

      CommandStringBuilder csb = new CommandStringBuilder(CliStrings.START_SERVER);
      csb.addOption(CliStrings.START_SERVER__NAME, serverName);
      csb.addOption(CliStrings.START_SERVER__LOCATORS, locatorString);
      csb.addOption(CliStrings.START_SERVER__SERVER_PORT, Integer.toString(port));

      CommandResult cmdResult = executeCommand(gfsh, csb.getCommandString());

      assertEquals(Status.OK, cmdResult.getStatus());
    }
    return serverNames;
  }

  private void doCreateCommands() throws IOException {
    createRegion(REGION1, REPLICATE, null);
    createRegion(REGION2, PARTITION, null);
    createIndex(INDEX1 , "AAPL", REGION1, null);
    createAndDeployJar(this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + "Deploy1.jar");
    createAsyncEventQueue("q1");
    final String autoCompact = "true";
    final String allowForceCompaction = "true";
    final String compactionThreshold = "50";
    final String duCritical = "90";
    final String duWarning = "85";
    final String maxOplogSize = "1000";
    final String queueSize = "300";
    final String timeInterval = "10";
    final String writeBufferSize="100";
    final String diskStoreName = "ds1";
    final String diskDirs = "ds1";
    
    createDiskStore(diskStoreName, diskDirs, autoCompact, allowForceCompaction, compactionThreshold, duCritical, duWarning, maxOplogSize, queueSize, timeInterval, writeBufferSize);
  }

  private void executeAndVerifyCommand(final String commandString) {
    CommandResult cmdResult = executeCommand(commandString);
    getLogWriter().info("Command Result : \n" + commandResultToString(cmdResult));
    assertEquals(Status.OK, cmdResult.getStatus());
    assertFalse(cmdResult.failedToPersist());
  }

  private void createRegion(final String regionName, final RegionShortcut regionShortCut, final String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void destroyRegion(final String regionName) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    csb.addOption(CliStrings.DESTROY_REGION__REGION, regionName);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void stopServer(final String serverName) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.STOP_SERVER);
    csb.addOption(CliStrings.STOP_SERVER__MEMBER, serverName);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void createAsyncEventQueue(final String queueName) throws IOException {
    String queueCommandsJarName = "testEndToEndSC-QueueCommands.jar";
    final File jarFile = new File(queueCommandsJarName);

    try {
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

      FileUtils.writeByteArrayToFile(jarFile, jarBytes);
      CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DEPLOY);
      csb.addOption(CliStrings.DEPLOY__JAR, queueCommandsJarName);
      executeAndVerifyCommand(csb.getCommandString());

      csb = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, queueName);
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER, "com.qcdunit.QueueCommandsDUnitTestListener");
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, "100");
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL, "200");
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS, "4");
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION, "true");
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS, "true");
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY, "1000");
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY, OrderPolicy.KEY.toString());
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, "true");
      csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL, "true");
      
      executeAndVerifyCommand(csb.getCommandString());

    } finally {
      FileUtils.deleteQuietly(jarFile);
    }
  }

  private void createDiskStore(final String diskStoreName,
                               final String diskDirs,
                               final String autoCompact,
                               final String allowForceCompaction,
                               final String compactionThreshold,
                               final String duCritical,
                               final String duWarning,
                               final String maxOplogSize,
                               final String queueSize,
                               final String timeInterval,
                               final String writeBufferSize) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
    csb.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStoreName);
    csb.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskDirs);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__AUTO_COMPACT, autoCompact);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION, allowForceCompaction);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD, compactionThreshold);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT, duCritical);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT, duWarning);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE, maxOplogSize);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__QUEUE_SIZE, queueSize);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__TIME_INTERVAL, timeInterval);
    csb.addOptionWithValueCheck(CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE, writeBufferSize);
    executeAndVerifyCommand(csb.getCommandString());
  }
  
  private void destroyDiskStore(final String diskStoreName, final String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);
    csb.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStoreName);
    csb.addOptionWithValueCheck(CliStrings.DESTROY_DISK_STORE__GROUP, group);
    executeAndVerifyCommand(csb.toString());
  }

  private void createIndex(final String indexName, final String expression, final String regionName, final String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, expression);
    csb.addOption(CliStrings.CREATE_INDEX__REGION, regionName);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void destroyIndex(final String indexName, final String regionName, final String group) {
    if (isBlank(indexName) && isBlank(regionName) && isBlank(group)) {
      return;
    }
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    if (!isBlank(indexName)) {
      csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    }

    if (!isBlank(regionName)) {
      csb.addOption(CliStrings.DESTROY_INDEX__REGION, regionName);
    }

    if (!isBlank(group)) {
      csb.addOption(CliStrings.DESTROY_INDEX__GROUP, group);
    }
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void createAndDeployJar(final String jarName) throws IOException {
    File newDeployableJarFile = new File(jarName);
    this.classBuilder.writeJarFromName("ShareConfigClass", newDeployableJarFile);
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DEPLOY);
    csb.addOption(CliStrings.DEPLOY__JAR, jarName);
    executeAndVerifyCommand(csb.getCommandString());
    jarFileNames.add(jarName);
  }

  private void deleteSavedJarFiles() throws IOException {
    deleteMatching(new File("."), "^" + JarDeployer.JAR_PREFIX + "Deploy1.*#\\d++$");
    delete(new File("Deploy1.jar"));
  }

  private Object[] setup() throws IOException {
    final int [] ports = getRandomAvailableTCPPorts(3);
    final int locator1Port = ports[0];

    final String locator1Name = "locator1-" + locator1Port;
    final String locatorLogPath = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + "locator-" + locator1Port + ".log";

    VM locatorAndMgr = getHost(0).getVM(3);
    Object[] result = (Object[]) locatorAndMgr.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        int httpPort;
        int jmxPort;
        String jmxHost;

        try {
          jmxHost = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException ignore) {
          jmxHost = "localhost";
        }

        final int[] ports = getRandomAvailableTCPPorts(2);

        jmxPort = ports[0];
        httpPort = ports[1];

        final File locatorLogFile = new File(locatorLogPath);

        final Properties locatorProps = new Properties();
        locatorProps.setProperty(NAME_NAME, locator1Name);
        locatorProps.setProperty(MCAST_PORT_NAME, "0");
        locatorProps.setProperty(LOG_LEVEL_NAME, "config");
        locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        locatorProps.setProperty(JMX_MANAGER_NAME, "true");
        locatorProps.setProperty(JMX_MANAGER_START_NAME, "true");
        locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS_NAME, String.valueOf(jmxHost));
        locatorProps.setProperty(JMX_MANAGER_PORT_NAME, String.valueOf(jmxPort));
        locatorProps.setProperty(HTTP_SERVICE_PORT_NAME, String.valueOf(httpPort));

        final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator1Port, locatorLogFile, null, locatorProps);

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
        waitForCriterion(wc, TIMEOUT, INTERVAL, true);

        final Object[] result = new Object[4];
        result[0] = locator1Port;
        result[1] = jmxHost;
        result[2] = jmxPort;
        result[3] = httpPort;
        return result;
      }
    });

    HeadlessGfsh gfsh = getDefaultShell();
    String jmxHost = (String)result[1];
    int jmxPort = (Integer)result[2];
    int httpPort = (Integer)result[3];

    connect(jmxHost, jmxPort, httpPort, gfsh);

    // Create a cache in VM 1
    VM dataMember = getHost(0).getVM(1);
    dataMember.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT_NAME, "0");
        localProps.setProperty(LOCATORS_NAME, "localhost:" + locator1Port);
        localProps.setProperty(NAME_NAME, "DataMember");
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);
        return getAllNormalMembers(cache);
      }
    });
    return result;
  }

  private void shutdownAll() throws IOException {
    VM locatorAndMgr = getHost(0).getVM(3);
    locatorAndMgr.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
        ShutdownAllRequest.send(cache.getDistributedSystem().getDistributionManager(), -1);
        return null;
      }
    });

    locatorAndMgr.invoke(SharedConfigurationTestUtils.cleanupLocator);

    //Clean up the directories
    if (!serverNames.isEmpty()) {
      for (String serverName : serverNames) {
        final File serverDir = new File(serverName);
        FileUtils.cleanDirectory(serverDir);
        FileUtils.deleteDirectory(serverDir);
      }
    }
    serverNames.clear();
  }

  private void verifyRegionCreateOnAllMembers(final String regionName) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESCRIBE_REGION);
    csb.addOption(CliStrings.DESCRIBE_REGION__NAME, regionName);
    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultAsString = commandResultToString(cmdResult);

    for (String serverName : serverNames) {
      assertTrue(resultAsString.contains(serverName));
    }
  }     

  private void verifyIndexCreationOnAllMembers(final String indexName) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_INDEX);
    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultAsString = commandResultToString(cmdResult);

    for (String serverName : serverNames) {
      assertTrue(resultAsString.contains(serverName));
    }
  }
  
  private void verifyAsyncEventQueueCreation() {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_ASYNC_EVENT_QUEUES);
    CommandResult cmdResult = executeCommand(csb.toString());
    String resultAsString = commandResultToString(cmdResult);
    
    for (String serverName : serverNames) {
      assertTrue(resultAsString.contains(serverName));
    }
  }

}
