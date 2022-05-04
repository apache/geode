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
package org.apache.geode.session.tests;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.geode.internal.GemFireVersion.getGemFireVersion;
import static org.apache.geode.test.version.TestVersions.atLeast;
import static org.apache.geode.test.version.VmConfigurations.hasGeodeVersion;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.apache.commons.lang3.JavaVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.gfsh.GfshExecutor;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.test.version.VmConfiguration;
import org.apache.geode.test.version.VmConfigurations;

/**
 * This test iterates through the versions of Geode and executes session client compatibility with
 * the current version of Geode.
 */
@Category(BackwardCompatibilityTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class Tomcat8ClientServerRollingUpgradeTest {

  @Parameters(name = "{0}")
  public static Collection<VmConfiguration> data() {
    String minimumVersion = isJavaVersionAtLeast(JavaVersion.JAVA_9) ? "1.8.0" : "1.7.0";
    return VmConfigurations.upgrades().stream()
        .filter(hasGeodeVersion(atLeast(TestVersion.valueOf(minimumVersion))))
        .collect(toList());
  }

  private final UniquePortSupplier portSupplier = new UniquePortSupplier();
  private final VmConfiguration sourceVmConfiguration;
  private Path locatorDir;
  private Path server1Dir;
  private Path server2Dir;
  private GfshExecutor oldGfsh;

  private GfshExecutor currentGfsh;

  protected Client client;
  protected ContainerManager manager;

  private TomcatInstall tomcat8AndOldModules;
  private TomcatInstall tomcat8AndCurrentModules;

  private int locatorPort;
  private int locatorJmxPort;

  private String classPathTomcat8AndCurrentModules;
  private String classPathTomcat8AndOldModules;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule();
  @Rule
  public TestName testName = new TestName();

  public Tomcat8ClientServerRollingUpgradeTest(VmConfiguration vmConfiguration) {
    sourceVmConfiguration = vmConfiguration;
  }

  private void startServer(String name, String classPath, int locatorPort, GfshExecutor gfsh,
      Path serverDir) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_SERVER);
    command.addOption(CliStrings.START_SERVER__NAME, name);
    command.addOption(CliStrings.START_SERVER__SERVER_PORT, "0");
    command.addOption(CliStrings.START_SERVER__CLASSPATH, classPath);
    command.addOption(CliStrings.START_SERVER__LOCATORS, "localhost[" + locatorPort + "]");
    command.addOption(CliStrings.START_SERVER__DIR, serverDir.toString());

    gfsh.execute(GfshScript.of(command.toString()).expectExitCode(0));
  }

  private void startLocator(String name, String classPath, int port, GfshExecutor gfsh,
      Path locatorDir) {
    CommandStringBuilder locStarter = new CommandStringBuilder(CliStrings.START_LOCATOR);
    locStarter.addOption(CliStrings.START_LOCATOR__MEMBER_NAME, name);
    locStarter.addOption(CliStrings.START_LOCATOR__CLASSPATH, classPath);
    locStarter.addOption(CliStrings.START_LOCATOR__PORT, Integer.toString(port));
    locStarter.addOption(CliStrings.START_LOCATOR__DIR, locatorDir.toString());
    locStarter.addOption(CliStrings.START_LOCATOR__HTTP_SERVICE_PORT, "0");
    locStarter.addOption(CliStrings.START_LOCATOR__J,
        "-Dgemfire.jmx-manager-port=" + locatorJmxPort);
    gfsh.execute(GfshScript.of(locStarter.toString()).expectExitCode(0));
  }

  @Before
  public void setup() throws Exception {
    Path tempFolder = folderRule.getFolder().toPath();
    currentGfsh = gfshRule.executor()
        .build(tempFolder);
    oldGfsh = gfshRule.executor()
        .withVmConfiguration(sourceVmConfiguration)
        .build(tempFolder);

    String version = sourceVmConfiguration.geodeVersion().toString();
    Path installLocation;
    if (version == null || VersionManager.isCurrentVersion(version)) {
      installLocation = new RequiresGeodeHome().getGeodeHome().toPath();
    } else {
      installLocation = Paths.get(VersionManager.getInstance().getInstall(version));
    }

    File oldBuild = installLocation.toFile();
    File oldModules = installLocation.resolve("tools").resolve("Modules").toFile();

    tomcat8AndOldModules =
        new TomcatInstall(tempFolder, "Tomcat8AndOldModules", TomcatInstall.TomcatVersion.TOMCAT8,
            ContainerInstall.ConnectionType.CLIENT_SERVER,
            oldModules.getAbsolutePath(),
            oldBuild.getAbsolutePath() + "/lib",
            portSupplier::getAvailablePort, TomcatInstall.CommitValve.DEFAULT);

    tomcat8AndCurrentModules =
        new TomcatInstall(tempFolder, "Tomcat8AndCurrentModules",
            TomcatInstall.TomcatVersion.TOMCAT8,
            ContainerInstall.ConnectionType.CLIENT_SERVER,
            portSupplier::getAvailablePort, TomcatInstall.CommitValve.DEFAULT);

    classPathTomcat8AndOldModules = getClassPathTomcat8AndOldModules();

    classPathTomcat8AndCurrentModules = getClassPathTomcat8AndCurrentModules();

    // Get available port for the locator
    locatorPort = portSupplier.getAvailablePort();
    locatorJmxPort = portSupplier.getAvailablePort();

    tomcat8AndOldModules.setDefaultLocatorPort(locatorPort);
    tomcat8AndCurrentModules.setDefaultLocatorPort(locatorPort);

    client = new Client();
    manager = new ContainerManager();
    // Due to parameterization of the test name, the URI would be malformed. Instead, it strips off
    // the [] symbols
    manager.setTestName(testName.getMethodName().replaceAll("[\\[\\] ,]+", ""));

    locatorDir = tempFolder.resolve("loc");
    server1Dir = tempFolder.resolve("server1");
    server2Dir = tempFolder.resolve("server2");
  }

  /**
   * Stops all containers that were previously started and cleans up their configurations
   */
  @After
  public void stop() throws IOException {
    manager.stopAllActiveContainers();
    manager.cleanUp();
  }

  @Test
  public void canDoARollingUpgradeOfGeodeServersWithSessionModules()
      throws IOException, ExecutionException, InterruptedException, TimeoutException,
      URISyntaxException {

    startLocator("loc", classPathTomcat8AndOldModules, locatorPort, oldGfsh, locatorDir);
    startServer("server1", classPathTomcat8AndOldModules, locatorPort, oldGfsh, server1Dir);
    startServer("server2", classPathTomcat8AndOldModules, locatorPort, oldGfsh, server2Dir);
    createRegion(oldGfsh);

    // Start two tomcat servers with the old geode modules
    ServerContainer container1 = manager.addContainer(tomcat8AndOldModules);
    ServerContainer container2 = manager.addContainer(tomcat8AndOldModules);

    // This has to happen at the start of every test
    manager.startAllInactiveContainers();

    verifySessionReplication();

    // Upgrade the geode locator
    stopLocator(oldGfsh, locatorDir);
    startLocator("loc", classPathTomcat8AndCurrentModules, locatorPort, currentGfsh, locatorDir);

    // Upgrade a server
    stopServer(oldGfsh, server1Dir);
    startServer("server1", classPathTomcat8AndCurrentModules, locatorPort, currentGfsh, server1Dir);

    // verify again
    verifySessionReplication();

    // Upgrade second server
    stopServer(oldGfsh, server2Dir);
    startServer("server2", classPathTomcat8AndCurrentModules, locatorPort, currentGfsh, server2Dir);

    // verify again
    verifySessionReplication();

    // Upgrade a tomcat server
    container1.stop();
    manager.removeContainer(container1);
    ServerContainer newContainer1 = manager.addContainer(tomcat8AndCurrentModules);
    newContainer1.start();

    verifySessionReplication();

    // Upgrade the second tomcat server
    container2.stop();
    manager.removeContainer(container2);
    ServerContainer newContainer2 = manager.addContainer(tomcat8AndCurrentModules);
    newContainer2.start();

    verifySessionReplication();
  }

  private void createRegion(GfshExecutor gfsh) {
    CommandStringBuilder connect = new CommandStringBuilder(CliStrings.CONNECT)
        .addOption(CliStrings.CONNECT__LOCATOR, "localhost[" + locatorPort + "]");

    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_REGION);
    command.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT,
        RegionShortcut.PARTITION_REDUNDANT.name())
        .addOption(CliStrings.CREATE_REGION__REGION, "gemfire_modules_sessions")
        .addOption(CliStrings.CREATE_REGION__STATISTICSENABLED, "true")
        .addOption(CliStrings.ENTRY_IDLE_TIME_CUSTOM_EXPIRY,
            "org.apache.geode.modules.util.SessionCustomExpiry");

    final GfshScript script = GfshScript.of(connect.toString(), command.toString());
    gfsh.execute(script);
  }

  private void stopLocator(GfshExecutor gfsh, Path locatorDir) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STOP_LOCATOR)
        .addOption(CliStrings.STOP_LOCATOR__DIR, locatorDir.toString());
    gfsh.execute(command.toString());
  }

  private void stopServer(GfshExecutor gfsh, Path serverDir) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STOP_SERVER)
        .addOption(CliStrings.STOP_SERVER__DIR, serverDir.toString());
    gfsh.execute(command.toString());
  }

  private void verifySessionReplication() throws IOException, URISyntaxException {
    String key = "value_testSessionPersists";
    String value = "Foo" + System.currentTimeMillis();

    client.setPort(Integer.parseInt(manager.getContainerPort(0)));
    Client.Response resp = client.set(key, value);
    String cookie = resp.getSessionCookie();

    for (int i = 0; i < manager.numContainers(); i++) {
      System.out.println("Checking get for container:" + i);
      client.setPort(Integer.parseInt(manager.getContainerPort(i)));
      resp = client.get(key);

      assertEquals("Sessions are not replicating properly", cookie, resp.getSessionCookie());
      assertEquals("Session data is not replicating properly", value, resp.getResponse());
    }
  }

  /**
   * If this test breaks due to a change in required jars, please update the
   * "Setting up the Module" section of the Tomcat module documentation!
   *
   * Returns the jars required on the classpath for the old modules. This list may
   * differ from the list required by the current modules at some point in the future, hence the
   * duplication of the requiredClasspathJars array.
   *
   * @return Paths to required jars
   */
  private String getClassPathTomcat8AndOldModules() {
    TestVersion geodeVersion = sourceVmConfiguration.geodeVersion();
    if (geodeVersion.equals(TestVersion.CURRENT_VERSION)) {
      return getClassPathTomcat8AndCurrentModules();
    }

    return getRequiredClasspathJars(tomcat8AndOldModules.getHome(), geodeVersion.toString());
  }

  /**
   * If this test breaks due to a change in required jars, please update the
   * "Setting up the Module" section of the Tomcat module documentation!
   *
   * Returns the jars required on the classpath for the current modules. This list may
   * differ from the list required by the old modules at some point in the future, hence the
   * duplication of the requiredClasspathJars array.
   *
   * @return Paths to required jars
   */
  private String getClassPathTomcat8AndCurrentModules() {
    return getRequiredClasspathJars(tomcat8AndCurrentModules.getHome(), getGemFireVersion());
  }

  private String getRequiredClasspathJars(final Path installDir, final String version) {
    return Stream.of(
        "/lib/geode-modules-" + version + ".jar",
        "/lib/geode-modules-tomcat8-" + version + ".jar",
        "/lib/servlet-api.jar",
        "/lib/catalina.jar",
        "/lib/tomcat-util.jar",
        "/bin/tomcat-juli.jar")
        .map(installDir.toString()::concat)
        .collect(joining(File.pathSeparator));
  }
}
