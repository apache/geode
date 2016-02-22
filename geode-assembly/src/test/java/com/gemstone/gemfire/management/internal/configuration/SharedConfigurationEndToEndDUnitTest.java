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

import static com.gemstone.gemfire.test.dunit.Wait.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionShortcut;

import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.JarDeployer;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllRequest;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class SharedConfigurationEndToEndDUnitTest extends CliCommandTestBase {
  private static final int TIMEOUT = 10000;
  private static final int INTERVAL = 500;
  private static final String REGION1 = "R1";
  private static final String REGION2 = "R2";
  private static final String INDEX1 = "ID1";
  private transient ClassBuilder classBuilder = new ClassBuilder();
  public static Set<String> serverNames = new HashSet<String>();
  public static Set<String> jarFileNames = new HashSet<String>();

  public SharedConfigurationEndToEndDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }

  private static final long serialVersionUID = -2276690105585944041L;

  public Set<String> startServers(HeadlessGfsh gfsh, String locatorString, int numServers, String serverNamePrefix, int startNum) throws ClassNotFoundException, IOException {
    Set<String> serverNames = new HashSet<String>();

    final int[] serverPorts = AvailablePortHelper.getRandomAvailableTCPPorts(numServers);
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

  public void testStartServerAndExecuteCommands() throws InterruptedException, ClassNotFoundException, IOException, ExecutionException {
    IgnoredException.addIgnoredException("EntryDestroyedException");
    Object[] result = setup();
    final int locatorPort = (Integer) result[0];
    final String jmxHost = (String) result[1];
    final int jmxPort = (Integer) result[2];
    final int httpPort = (Integer) result[3];
    final String locatorString = "localHost[" + locatorPort + "]";

    final HeadlessGfsh gfsh = new HeadlessGfsh("gfsh2", 300);
    assertNotNull(gfsh);
    shellConnect(jmxHost, jmxPort, httpPort, gfsh);

    serverNames.addAll(startServers(gfsh, locatorString, 2, "Server", 1));
    doCreateCommands();
    serverNames.addAll(startServers(gfsh, locatorString, 1, "NewMember", 4));
    verifyRegionCreateOnAllMembers(REGION1);
    verifyRegionCreateOnAllMembers(REGION2);
    verifyIndexCreationOnAllMembers(INDEX1);
    verifyAsyncEventQueueCreation();
   


    //shutdown everything
    LogWriterUtils.getLogWriter().info("Shutting down all the members");
    shutdownAll();
    deleteSavedJarFiles();
  }


  private void doCreateCommands() {
    createRegion(REGION1, RegionShortcut.REPLICATE, null);
    createRegion(REGION2, RegionShortcut.PARTITION, null);
    createIndex(INDEX1 , "AAPL", REGION1, null);
    createAndDeployJar("Deploy1.jar");
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


  protected void executeAndVerifyCommand(String commandString) {
    CommandResult cmdResult = executeCommand(commandString);
    LogWriterUtils.getLogWriter().info("Command Result : \n" + commandResultToString(cmdResult));
    assertEquals(Status.OK, cmdResult.getStatus());
    assertFalse(cmdResult.failedToPersist());
  }

  private void createRegion(String regionName, RegionShortcut regionShortCut, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void destroyRegion(String regionName) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    csb.addOption(CliStrings.DESTROY_REGION__REGION, regionName);
    executeAndVerifyCommand(csb.getCommandString());
  }

  private void stopServer(String serverName) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.STOP_SERVER);
    csb.addOption(CliStrings.STOP_SERVER__MEMBER, serverName);
    executeAndVerifyCommand(csb.getCommandString());
  }

  public void createAsyncEventQueue(String queueName) {
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

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      FileUtils.deleteQuietly(jarFile);
    }
  }
  private void createDiskStore(String diskStoreName, 
      String diskDirs, 
      String autoCompact, 
      String allowForceCompaction, 
      String compactionThreshold, 
      String duCritical, 
      String duWarning,
      String maxOplogSize,
      String queueSize,
      String timeInterval,
      String writeBufferSize) {
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
  
  private void destroyDiskStore(String diskStoreName, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);
    csb.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStoreName);
    csb.addOptionWithValueCheck(CliStrings.DESTROY_DISK_STORE__GROUP, group);
    executeAndVerifyCommand(csb.toString());
  }
  public void createIndex(String indexName, String expression, String regionName, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, expression);
    csb.addOption(CliStrings.CREATE_INDEX__REGION, regionName);
    executeAndVerifyCommand(csb.getCommandString());
  }

  public void destoyIndex(String indexName, String regionName, String group) {
    if (StringUtils.isBlank(indexName) && StringUtils.isBlank(regionName) && StringUtils.isBlank(group)) {
      return;
    }
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    if (!StringUtils.isBlank(indexName)) {
      csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    }

    if (!StringUtils.isBlank(regionName)) {
      csb.addOption(CliStrings.DESTROY_INDEX__REGION, regionName);
    }

    if (!StringUtils.isBlank(group)) {
      csb.addOption(CliStrings.DESTROY_INDEX__GROUP, group);
    }
    executeAndVerifyCommand(csb.getCommandString());
  }

  public void createAndDeployJar(String jarName) {
    File newDeployableJarFile = new File(jarName);
    try {
      this.classBuilder.writeJarFromName("ShareConfigClass", newDeployableJarFile);
      CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DEPLOY);
      csb.addOption(CliStrings.DEPLOY__JAR, jarName);
      executeAndVerifyCommand(csb.getCommandString());
      jarFileNames.add(jarName);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void deleteSavedJarFiles() {
    try {
      FileUtil.deleteMatching(new File("."), "^" + JarDeployer.JAR_PREFIX + "Deploy1.*#\\d++$");
      FileUtil.delete(new File("Deploy1.jar"));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  public Object[] setup() {
    disconnectAllFromDS();
    final int [] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    final int locator1Port = ports[0];
    final String locator1Name = "locator1-" + locator1Port;
    VM locatorAndMgr = Host.getHost(0).getVM(3);

    Object[] result = (Object[]) locatorAndMgr.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        int httpPort;
        int jmxPort;
        String jmxHost;

        try {
          jmxHost = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException ignore) {
          jmxHost = "localhost";
        }

        final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

        jmxPort = ports[0];
        httpPort = ports[1];

        final File locatorLogFile = new File("locator-" + locator1Port + ".log");

        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, locator1Name);
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "config");
        locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        locatorProps.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
        locatorProps.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "true");
        locatorProps.setProperty(DistributionConfig.JMX_MANAGER_BIND_ADDRESS_NAME, String.valueOf(jmxHost));
        locatorProps.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(jmxPort));
        locatorProps.setProperty(DistributionConfig.HTTP_SERVICE_PORT_NAME, String.valueOf(httpPort));

        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator1Port, locatorLogFile, null,
              locatorProps);
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
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }

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

    shellConnect(jmxHost, jmxPort, httpPort, gfsh);
    // Create a cache in VM 1
    VM dataMember = Host.getHost(0).getVM(1);
    dataMember.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locator1Port);
        localProps.setProperty(DistributionConfig.NAME_NAME, "DataMember");
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);
        return CliUtil.getAllNormalMembers(cache);
      }
    });
    return result;
  }

  private void shutdownAll() throws IOException {
    VM locatorAndMgr = Host.getHost(0).getVM(3);
    locatorAndMgr.invoke(new SerializableCallable() {
      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
        ShutdownAllRequest.send(cache.getDistributedSystem().getDistributionManager(), -1);
        return null;
      }
    });

    locatorAndMgr.invoke(SharedConfigurationDUnitTest.locatorCleanup);
    //Clean up the directories
    if (!serverNames.isEmpty()) {
      for (String serverName : serverNames) {
        final File serverDir = new File(serverName);
        FileUtils.cleanDirectory(serverDir);
        FileUtils.deleteDirectory(serverDir);
      }
    }
    serverNames.clear();
    serverNames = null;
  }

  private void verifyRegionCreateOnAllMembers(String regionName) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESCRIBE_REGION);
    csb.addOption(CliStrings.DESCRIBE_REGION__NAME, regionName);
    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultAsString = commandResultToString(cmdResult);

    for (String serverName : serverNames) {
      assertTrue(resultAsString.contains(serverName));
    }
  }     

  private void verifyIndexCreationOnAllMembers(String indexName) {
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
