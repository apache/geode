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
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.remote.CommandProcessor;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Unit tests for the DeployCommands class
 *
 * @since GemFire 7.0
 */
@SuppressWarnings("serial")
@Category(DistributedTest.class)
public class DeployCommandsDUnitTest extends CliCommandTestBase {

  private final Pattern pattern =
      Pattern.compile("^" + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit.*#\\d++$");
  private File newDeployableJarFile;
  private transient ClassBuilder classBuilder;
  private transient CommandProcessor commandProcessor;

  @Override
  public final void postSetUpCliCommandTestBase() throws Exception {
    this.newDeployableJarFile = new File(this.temporaryFolder.getRoot().getCanonicalPath()
        + File.separator + "DeployCommandsDUnit1.jar");
    this.classBuilder = new ClassBuilder();
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
  protected final void preTearDownCliCommandTestBase() throws Exception {
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
  }

  @Test
  public void testDeploy() throws Exception {
    final Properties props = new Properties();
    final Host host = Host.getHost(0);
    final VM vm = host.getVM(0);
    final String vmName = "VM" + vm.getPid();

    // Create the cache in this VM
    props.setProperty(NAME, "Controller");
    props.setProperty(GROUPS, "Group1");
    getSystem(props);
    getCache();

    // Create the cache in the other VM
    vm.invoke(new SerializableRunnable() {
      public void run() {
        props.setProperty(NAME, vmName);
        props.setProperty(GROUPS, "Group2");
        getSystem(props);
        getCache();
      }
    });

    DeployCommands deployCommands = new DeployCommands();

    // Single JAR all members
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit1.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitA")});
    Result result = deployCommands.deploy(null, "DeployCommandsDUnit1.jar", null);

    assertEquals(true, result.hasNextLine());

    String resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(1, countMatchesInString(resultString, "Controller"));
    assertEquals(1, countMatchesInString(resultString, vmName));
    assertEquals(4, countMatchesInString(resultString, "DeployCommandsDUnit1.jar"));

    // Single JAR with group
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit2.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitB")});
    result = deployCommands.deploy(new String[] {"Group2"}, "DeployCommandsDUnit2.jar", null);

    assertEquals(true, result.hasNextLine());

    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(false, resultString.contains("Controller"));
    assertEquals(1, countMatchesInString(resultString, vmName));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit2.jar"));

    // Multiple JARs to all members
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit3.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitC"),
        "DeployCommandsDUnit4.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitD")});
    result = deployCommands.deploy(null, null, "AnyDirectory");

    assertEquals(true, result.hasNextLine());

    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(2, countMatchesInString(resultString, "Controller"));
    assertEquals(2, countMatchesInString(resultString, vmName));
    assertEquals(4, countMatchesInString(resultString, "DeployCommandsDUnit3.jar"));
    assertEquals(4, countMatchesInString(resultString, "DeployCommandsDUnit4.jar"));

    // Multiple JARs to a group
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit5.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitE"),
        "DeployCommandsDUnit6.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitF")});
    result = deployCommands.deploy(new String[] {"Group1"}, null, "AnyDirectory");

    assertEquals(true, result.hasNextLine());

    resultString = result.nextLine();
    assertEquals(false, resultString.contains("ERROR"));
    assertEquals(2, countMatchesInString(resultString, "Controller"));
    assertEquals(false, resultString.contains(vmName));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit5.jar"));
    assertEquals(2, countMatchesInString(resultString, "DeployCommandsDUnit6.jar"));
  }

  @Test
  public void testUndeploy() throws Exception {
    final Properties props = new Properties();
    final Host host = Host.getHost(0);
    final VM vm = host.getVM(0);
    final String vmName = "VM" + vm.getPid();

    // Create the cache in this VM
    props.setProperty(NAME, "Controller");
    props.setProperty(GROUPS, "Group1");
    getSystem(props);
    getCache();

    // Create the cache in the other VM
    vm.invoke(new SerializableRunnable() {
      public void run() {
        props.setProperty(NAME, vmName);
        props.setProperty(GROUPS, "Group2");
        getSystem(props);
        getCache();
      }
    });

    DeployCommands deployCommands = new DeployCommands();

    // Deploy a couple of JAR files which can be undeployed
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit1.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitA")});
    deployCommands.deploy(new String[] {"Group1"}, "DeployCommandsDUnit1.jar", null);
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit2.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitB")});
    deployCommands.deploy(new String[] {"Group2"}, "DeployCommandsDUnit2.jar", null);
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit3.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitC")});
    deployCommands.deploy(null, "DeployCommandsDUnit3.jar", null);
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit4.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitD")});
    deployCommands.deploy(null, "DeployCommandsDUnit4.jar", null);
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit5.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitE")});
    deployCommands.deploy(null, "DeployCommandsDUnit5.jar", null);

    // Undeploy for 1 group
    Result result = deployCommands.undeploy(new String[] {"Group1"}, "DeployCommandsDUnit1.jar");
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

  @Test
  public void testListDeployed() throws Exception {
    final Properties props = new Properties();
    final Host host = Host.getHost(0);
    final VM vm = host.getVM(0);
    final String vmName = "VM" + vm.getPid();

    // Create the cache in this VM
    props.setProperty(NAME, "Controller");
    props.setProperty(GROUPS, "Group1");
    getSystem(props);
    getCache();

    // Create the cache in the other VM
    vm.invoke(new SerializableRunnable() {
      public void run() {
        props.setProperty(NAME, vmName);
        props.setProperty(GROUPS, "Group2");
        getSystem(props);
        getCache();
      }
    });

    DeployCommands deployCommands = new DeployCommands();

    // Deploy a couple of JAR files which can be listed
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit1.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitA")});
    deployCommands.deploy(new String[] {"Group1"}, "DeployCommandsDUnit1.jar", null);
    CommandExecutionContext.setBytesFromShell(new byte[][] {"DeployCommandsDUnit2.jar".getBytes(),
        this.classBuilder.createJarFromName("DeployCommandsDUnitB")});
    deployCommands.deploy(new String[] {"Group2"}, "DeployCommandsDUnit2.jar", null);

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
   * Does an end-to-end test using the complete CLI framework while ensuring that the shared
   * configuration is updated.
   */
  @Test
  public void testEndToEnd() throws Exception {
    final String groupName = getName();
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    jmxPort = ports[0];
    httpPort = ports[1];
    try {
      jmxHost = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ignore) {
      jmxHost = "localhost";
    }

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locatorLogPath = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator
        + "locator-" + locatorPort + ".log";

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

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        final File locatorLogFile = new File(locatorLogPath);
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
          Wait.waitForCriterion(wc, 5000, 500, true);

        } catch (IOException e) {
          fail("Unable to create a locator with a shared configuration", e);
        }
      }
    });

    connect(jmxHost, jmxPort, httpPort, getDefaultShell());

    Host.getHost(0).getVM(1).invoke(() -> {
      Properties properties = new Properties();
      properties.setProperty("name", "Manager");
      properties.setProperty("groups", groupName);
      ServerStarterRule serverStarterRule = new ServerStarterRule(properties);
      serverStarterRule.startServer(locatorPort);
    });

    // Create a JAR file
    this.classBuilder.writeJarFromName("DeployCommandsDUnitA", this.newDeployableJarFile);

    // Deploy the JAR
    CommandResult cmdResult =
        executeCommand("deploy --jar=" + this.newDeployableJarFile.getCanonicalPath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    String stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*JAR.*JAR Location"));
    assertTrue(stringContainsLine(stringResult, "Manager.*DeployCommandsDUnit1.jar.*"
        + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1"));

    // Undeploy the JAR
    cmdResult = executeCommand("undeploy --jar=DeployCommandsDUnit1.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertThat(stringContainsLine(stringResult, "Member.*JAR.*Un-Deployed From JAR Location"))
        .describedAs(stringResult).isTrue();
    assertThat(stringContainsLine(stringResult, "Manager.*DeployCommandsDUnit1.jar.*"
        + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1")).describedAs(stringResult)
            .isTrue();;


    // Deploy the JAR to a group
    cmdResult = executeCommand(
        "deploy --jar=" + this.newDeployableJarFile.getCanonicalPath() + " --group=" + groupName);
    assertThat(cmdResult.getStatus()).describedAs(cmdResult.toString()).isEqualTo(Result.Status.OK);

    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertThat(stringContainsLine(stringResult, "Member.*JAR.*JAR Location"))
        .describedAs(stringResult).isTrue();

    assertThat(stringContainsLine(stringResult, "Manager.*DeployCommandsDUnit1.jar.*"
        + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1")).describedAs(stringResult)
            .isTrue();

    // Make sure the deployed jar in the shared config
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        ClusterConfigurationService sharedConfig =
            ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        try {
          assertTrue(sharedConfig.getConfiguration(groupName).getJarNames()
              .contains("DeployCommandsDUnit1.jar"));
        } catch (Exception e) {
          Assert.fail("Error occurred in cluster configuration service", e);
        }
      }
    });

    // List deployed for group
    cmdResult = executeCommand("list deployed --group=" + groupName);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*JAR.*JAR Location"));
    assertTrue(stringContainsLine(stringResult, "Manager.*DeployCommandsDUnit1.jar.*"
        + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1"));

    // Undeploy for group
    cmdResult = executeCommand("undeploy --group=" + groupName);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*JAR.*Un-Deployed From JAR Location"));
    assertTrue(stringContainsLine(stringResult, "Manager.*DeployCommandsDUnit1.jar.*"
        + JarDeployer.JAR_PREFIX + "DeployCommandsDUnit1.jar#1"));

    // Make sure the deployed jar was removed from the shared config
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        ClusterConfigurationService sharedConfig =
            ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        try {
          assertFalse(sharedConfig.getConfiguration(groupName).getJarNames()
              .contains("DeployCommandsDUnit1.jar"));
        } catch (Exception e) {
          Assert.fail("Error occurred in cluster configuration service", e);
        }
      }
    });

    // List deployed with nothing deployed
    cmdResult = executeCommand("list deployed");
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(
        commandResultToString(cmdResult).contains(CliStrings.LIST_DEPLOYED__NO_JARS_FOUND_MESSAGE));
  }

  private void deleteSavedJarFiles() {
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
