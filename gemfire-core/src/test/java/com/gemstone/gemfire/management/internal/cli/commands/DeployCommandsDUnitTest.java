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

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.internal.JarDeployer;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.remote.CommandExecutionContext;
import com.gemstone.gemfire.management.internal.cli.remote.CommandProcessor;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Unit tests for the DeployCommands class
 *
 * @author David Hoots
 * @since 7.0
 */
public class DeployCommandsDUnitTest extends CliCommandTestBase {
  private static final long serialVersionUID = 1L;

  File newDeployableJarFile = new File("DeployCommandsDUnit1.jar");

  transient private ClassBuilder classBuilder = new ClassBuilder();
  transient private CommandProcessor commandProcessor;

  public DeployCommandsDUnitTest(String name) {
    super(name);
  }

  @SuppressWarnings("serial")
  @Override
  public void setUp() throws Exception {
    super.setUp();

    this.commandProcessor = new CommandProcessor();
    assertFalse(this.commandProcessor.isStopped());

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        deleteSavedJarFiles();
      }
    });
    deleteSavedJarFiles();
  }

  @SuppressWarnings("serial")
  @Override
  public void tearDown2() throws Exception {
    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        DistributionManager.isDedicatedAdminVM = false;
      }
    });

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        deleteSavedJarFiles();
      }
    });
    deleteSavedJarFiles();

    super.tearDown2();
  }

  @SuppressWarnings("serial")
  public void testDeploy() throws IOException {
    final Properties props = new Properties();
    final Host host = Host.getHost(0);
    final VM vm = host.getVM(0);
    final String vmName = "VM" + vm.getPid();

    // Create the cache in this VM
    props.setProperty(DistributionConfig.NAME_NAME, "Controller");
    props.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
    getSystem(props);
    getCache();

    // Create the cache in the other VM
    vm.invoke(new SerializableRunnable() {
      public void run() {
        props.setProperty(DistributionConfig.NAME_NAME, vmName);
        props.setProperty(DistributionConfig.GROUPS_NAME, "Group2");
        getSystem(props);
        getCache();
      }
    });

    DeployCommands deployCommands = new DeployCommands();

    // Single JAR all members
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit1.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitA")});
    Result result = deployCommands.deploy(null, "DeployCommandsDUnit1.jar", null);

    assertEquals(true, result.hasNextLine());

    String resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(1, countMatchesInString(resultString, "Controller"));
    assertEquals(1, countMatchesInString(resultString, vmName));
    assertEquals(4, countMatchesInString(resultString, "DeployCommandsDUnit1.jar"));

    // Single JAR with group
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit2.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitB")});
    result = deployCommands.deploy(new String[]{"Group2"}, "DeployCommandsDUnit2.jar", null);

    assertEquals(true, result.hasNextLine());

    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(false, resultString.contains("Controller"));
    assertEquals(1, countMatchesInString(resultString, vmName));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit2.jar"));

    // Multiple JARs to all members
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit3.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitC"), "DeployCommandsDUnit4.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitD")});
    result = deployCommands.deploy(null, null, "AnyDirectory");

    assertEquals(true, result.hasNextLine());

    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(2, countMatchesInString(resultString, "Controller"));
    assertEquals(2, countMatchesInString(resultString, vmName));
    assertEquals(4, countMatchesInString(resultString, "DeployCommandsDUnit3.jar"));
    assertEquals(4, countMatchesInString(resultString, "DeployCommandsDUnit4.jar"));

    // Multiple JARs to a group
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit5.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitE"), "DeployCommandsDUnit6.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitF")});
    result = deployCommands.deploy(new String[]{"Group1"}, null, "AnyDirectory");

    assertEquals(true, result.hasNextLine());

    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(2, countMatchesInString(resultString, "Controller"));
    assertEquals(false, resultString.contains(vmName));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit5.jar"));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit6.jar"));
  }

  @SuppressWarnings("serial")
  public void testUndeploy() throws IOException {
    final Properties props = new Properties();
    final Host host = Host.getHost(0);
    final VM vm = host.getVM(0);
    final String vmName = "VM" + vm.getPid();

    // Create the cache in this VM
    props.setProperty(DistributionConfig.NAME_NAME, "Controller");
    props.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
    getSystem(props);
    getCache();

    // Create the cache in the other VM
    vm.invoke(new SerializableRunnable() {
      public void run() {
        props.setProperty(DistributionConfig.NAME_NAME, vmName);
        props.setProperty(DistributionConfig.GROUPS_NAME, "Group2");
        getSystem(props);
        getCache();
      }
    });

    DeployCommands deployCommands = new DeployCommands();

    // Deploy a couple of JAR files which can be undeployed
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit1.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitA")});
    deployCommands.deploy(new String[]{"Group1"}, "DeployCommandsDUnit1.jar", null);
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit2.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitB")});
    deployCommands.deploy(new String[]{"Group2"}, "DeployCommandsDUnit2.jar", null);
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit3.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitC")});
    deployCommands.deploy(null, "DeployCommandsDUnit3.jar", null);
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit4.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitD")});
    deployCommands.deploy(null, "DeployCommandsDUnit4.jar", null);
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit5.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitE")});
    deployCommands.deploy(null, "DeployCommandsDUnit5.jar", null);

    // Undeploy for 1 group
    Result result = deployCommands.undeploy(new String[]{"Group1"}, "DeployCommandsDUnit1.jar");
    assertEquals(true, result.hasNextLine());
    String resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(1, countMatchesInString(resultString, "Controller"));
    assertEquals(false, resultString.contains(vmName));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit1.jar"));

    // Multiple Undeploy for all members
    result = deployCommands.undeploy(null, "DeployCommandsDUnit2.jar, DeployCommandsDUnit3.jar");
    assertEquals(true, result.hasNextLine());
    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(2, countMatchesInString(resultString, "Controller"));
    assertEquals(2, countMatchesInString(resultString, vmName));
    assertEquals(3, countMatchesInString(resultString, "DeployCommandsDUnit2.jar"));
    assertEquals(4, countMatchesInString(resultString, "DeployCommandsDUnit3.jar"));

    // Undeploy all (no JAR specified)
    result = deployCommands.undeploy(null, null);
    assertEquals(true, result.hasNextLine());
    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(2, countMatchesInString(resultString, "Controller"));
    assertEquals(2, countMatchesInString(resultString, vmName));
    assertEquals(4, countMatchesInString(resultString, "DeployCommandsDUnit4.jar"));
    assertEquals(4, countMatchesInString(resultString, "DeployCommandsDUnit5.jar"));
  }

  @SuppressWarnings("serial")
  public void testListDeployed() throws IOException {
    final Properties props = new Properties();
    final Host host = Host.getHost(0);
    final VM vm = host.getVM(0);
    final String vmName = "VM" + vm.getPid();

    // Create the cache in this VM
    props.setProperty(DistributionConfig.NAME_NAME, "Controller");
    props.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
    getSystem(props);
    getCache();

    // Create the cache in the other VM
    vm.invoke(new SerializableRunnable() {
      public void run() {
        props.setProperty(DistributionConfig.NAME_NAME, vmName);
        props.setProperty(DistributionConfig.GROUPS_NAME, "Group2");
        getSystem(props);
        getCache();
      }
    });

    DeployCommands deployCommands = new DeployCommands();

    // Deploy a couple of JAR files which can be listed
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit1.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitA")});
    deployCommands.deploy(new String[]{"Group1"}, "DeployCommandsDUnit1.jar", null);
    CommandExecutionContext.setBytesFromShell(
        new byte[][]{"DeployCommandsDUnit2.jar".getBytes(), this.classBuilder.createJarFromName(
            "DeployCommandsDUnitB")});
    deployCommands.deploy(new String[]{"Group2"}, "DeployCommandsDUnit2.jar", null);

    // List for all members
    Result result = deployCommands.listDeployed(null);
    assertEquals(true, result.hasNextLine());
    String resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(1, countMatchesInString(resultString, "Controller"));
    assertEquals(1, countMatchesInString(resultString, vmName));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit1.jar"));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit2.jar"));

    // List for members in Group1
    result = deployCommands.listDeployed("Group1");
    assertEquals(true, result.hasNextLine());
    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(1, countMatchesInString(resultString, "Controller"));
    assertEquals(false, resultString.contains(vmName));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit1.jar"));
    assertEquals(false, resultString.contains("DeployCommandsDUnit2.jar"));

    // List for members in Group2
    result = deployCommands.listDeployed("Group2");
    assertEquals(true, result.hasNextLine());
    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(false, resultString.contains("Controller"));
    assertEquals(1, countMatchesInString(resultString, vmName));
    assertEquals(false, resultString.contains("DeployCommandsDUnit1.jar"));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit2.jar"));
  }

  /**
   * Does an end-to-end test using the complete CLI framework while ensuring that the shared configuration is updated.
   */
  public void testEndToEnd() throws IOException {
    disconnectAllFromDS();

    final String groupName = "testDeployEndToEndGroup";

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
    managerProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
    managerProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
    createDefaultSetup(managerProps);

    // Create a JAR file
    this.classBuilder.writeJarFromName("DeployCommandsDUnitA", this.newDeployableJarFile);

    // Deploy the JAR
    CommandResult cmdResult = executeCommand("deploy --jar=DeployCommandsDUnit1.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    String stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*JAR.*JAR Location"));
    assertTrue(stringContainsLine(stringResult,
        "Manager.*DeployCommandsDUnit1.jar.*" + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1"));

    // Undeploy the JAR
    cmdResult = executeCommand("undeploy --jar=DeployCommandsDUnit1.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*JAR.*Un-Deployed From JAR Location"));
    assertTrue(stringContainsLine(stringResult,
        "Manager.*DeployCommandsDUnit1.jar.*" + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1"));

    // Deploy the JAR to a group
    cmdResult = executeCommand("deploy --jar=DeployCommandsDUnit1.jar --group=" + groupName);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*JAR.*JAR Location"));
    assertTrue(stringContainsLine(stringResult,
        "Manager.*DeployCommandsDUnit1.jar.*" + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1"));

    // Make sure the deployed jar in the shared config
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        try {
          assertTrue(sharedConfig.getConfiguration(groupName).getJarNames().contains("DeployCommandsDUnit1.jar"));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });

    // List deployed for group
    cmdResult = executeCommand("list deployed --group=" + groupName);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*JAR.*JAR Location"));
    assertTrue(stringContainsLine(stringResult,
        "Manager.*DeployCommandsDUnit1.jar.*" + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1"));

    // Undeploy for group
    cmdResult = executeCommand("undeploy --group=" + groupName);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*JAR.*Un-Deployed From JAR Location"));
    assertTrue(stringContainsLine(stringResult,
        "Manager.*DeployCommandsDUnit1.jar.*" + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1"));

    // Make sure the deployed jar was removed from the shared config
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        try {
          assertFalse(sharedConfig.getConfiguration(groupName).getJarNames().contains("DeployCommandsDUnit1.jar"));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });

    // List deployed with nothing deployed
    cmdResult = executeCommand("list deployed");
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains(CliStrings.LIST_DEPLOYED__NO_JARS_FOUND_MESSAGE));
  }

  final Pattern pattern = Pattern.compile("^" + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit.*#\\d++$");

  void deleteSavedJarFiles() {
    this.newDeployableJarFile.delete();

    File dirFile = new File(".");
    // Find all deployed JAR files
    File[] oldJarFiles = dirFile.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(final File file, final String name) {
        return DeployCommandsDUnitTest.this.pattern.matcher(name).matches();
      }
    });

    // Now delete them
    if (oldJarFiles != null) {
      for (File oldJarFile : oldJarFiles) {
        oldJarFile.delete();
      }
    }
  }
}
