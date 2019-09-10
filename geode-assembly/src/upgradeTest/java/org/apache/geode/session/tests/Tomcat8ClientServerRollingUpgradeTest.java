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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.VersionManager;

/**
 * This test iterates through the versions of Geode and executes session client compatibility with
 * the current version of Geode.
 */
@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class Tomcat8ClientServerRollingUpgradeTest {
  private final UniquePortSupplier portSupplier = new UniquePortSupplier();
  private final String oldVersion;
  private String locatorDir;
  private String server1Dir;
  private String server2Dir;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    int minimumVersion = SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9) ? 180 : 170;
    result.removeIf(s -> Integer.parseInt(s) < minimumVersion);
    return result;
  }

  @Rule
  public transient GfshRule oldGfsh;

  @Rule
  public final transient GfshRule currentGfsh = new GfshRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public transient TestName testName = new TestName();

  protected transient Client client;
  protected transient ContainerManager manager;


  protected TomcatInstall tomcat8AndOldModules;
  protected TomcatInstall tomcat8AndCurrentModules;

  protected int locatorPort;
  protected int locatorJmxPort;

  protected String classPathTomcat8AndCurrentModules;
  private String classPathTomcat8AndOldModules;

  public Tomcat8ClientServerRollingUpgradeTest(String version) {
    this.oldVersion = version;
    oldGfsh = new GfshRule(oldVersion);
  }

  protected void startServer(String name, String classPath, int locatorPort, GfshRule gfsh,
      String serverDir) throws Exception {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_SERVER);
    command.addOption(CliStrings.START_SERVER__NAME, name);
    command.addOption(CliStrings.START_SERVER__SERVER_PORT, "0");
    command.addOption(CliStrings.START_SERVER__CLASSPATH, classPath);
    command.addOption(CliStrings.START_SERVER__LOCATORS, "localhost[" + locatorPort + "]");
    command.addOption(CliStrings.START_SERVER__DIR, serverDir);
    gfsh.execute(GfshScript.of(command.toString()).expectExitCode(0));
  }

  protected void startLocator(String name, String classPath, int port, GfshRule gfsh,
      String locatorDir) throws Exception {
    CommandStringBuilder locStarter = new CommandStringBuilder(CliStrings.START_LOCATOR);
    locStarter.addOption(CliStrings.START_LOCATOR__MEMBER_NAME, name);
    locStarter.addOption(CliStrings.START_LOCATOR__CLASSPATH, classPath);
    locStarter.addOption(CliStrings.START_LOCATOR__PORT, Integer.toString(port));
    locStarter.addOption(CliStrings.START_LOCATOR__DIR, locatorDir);
    locStarter.addOption(CliStrings.START_LOCATOR__HTTP_SERVICE_PORT, "0");
    locStarter.addOption(CliStrings.START_LOCATOR__J,
        "-Dgemfire.jmx-manager-port=" + locatorJmxPort);
    gfsh.execute(GfshScript.of(locStarter.toString()).expectExitCode(0));
  }

  @Before
  public void setup() throws Exception {
    VersionManager versionManager = VersionManager.getInstance();
    String installLocation = versionManager.getInstall(oldVersion);
    File oldBuild = new File(installLocation);
    File oldModules = new File(installLocation + "/tools/Modules/");


    tomcat8AndOldModules =
        new TomcatInstall("Tomcat8AndOldModules", TomcatInstall.TomcatVersion.TOMCAT8,
            ContainerInstall.ConnectionType.CLIENT_SERVER,
            oldModules.getAbsolutePath(),
            oldBuild.getAbsolutePath() + "/lib",
            portSupplier::getAvailablePort);

    tomcat8AndCurrentModules =
        new TomcatInstall("Tomcat8AndCurrentModules", TomcatInstall.TomcatVersion.TOMCAT8,
            ContainerInstall.ConnectionType.CLIENT_SERVER,
            portSupplier::getAvailablePort);

    classPathTomcat8AndOldModules = tomcat8AndOldModules.getHome() + "/lib/*" + File.pathSeparator
        + tomcat8AndOldModules.getHome() + "/bin/*";

    classPathTomcat8AndCurrentModules =
        tomcat8AndCurrentModules.getHome() + "/lib/*" + File.pathSeparator
            + tomcat8AndCurrentModules.getHome() + "/bin/*";

    // Get available port for the locator
    locatorPort = portSupplier.getAvailablePort();
    locatorJmxPort = portSupplier.getAvailablePort();

    tomcat8AndOldModules.setDefaultLocatorPort(locatorPort);
    tomcat8AndCurrentModules.setDefaultLocatorPort(locatorPort);

    client = new Client();
    manager = new ContainerManager();
    // Due to parameterization of the test name, the URI would be malformed. Instead, it strips off
    // the [] symbols
    manager.setTestName(testName.getMethodName().replace("[", "").replace("]", ""));

    locatorDir = tempFolder.newFolder("loc").getPath();
    server1Dir = tempFolder.newFolder("server1").getPath();
    server2Dir = tempFolder.newFolder("server2").getPath();
  }

  /**
   * Stops all containers that were previously started and cleans up their configurations
   */
  @After
  public void stop() throws Exception {
    manager.stopAllActiveContainers();
    manager.cleanUp();

    CommandStringBuilder connect = new CommandStringBuilder(CliStrings.CONNECT)
        .addOption(CliStrings.CONNECT__LOCATOR, "localhost[" + locatorPort + "]");

    CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHUTDOWN);
    command.addOption(CliStrings.INCLUDE_LOCATORS, "true");
    final GfshScript script = GfshScript.of(connect.toString(), command.toString());
    try {
      oldGfsh.execute(script);
    } catch (Throwable e) {
      // ignore
    }

    try {
      currentGfsh.execute(script);
    } catch (Throwable e) {
      // ignore
    }
  }

  @Test
  public void canDoARollingUpgradeOfGeodeServersWithSessionModules() throws Exception {

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

  private void createRegion(GfshRule gfsh) {
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

  private void stopLocator(GfshRule gfsh, String locatorDir) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STOP_LOCATOR)
        .addOption(CliStrings.STOP_LOCATOR__DIR, locatorDir);
    gfsh.execute(command.toString());
  }

  private void stopServer(GfshRule gfsh, String serverDir) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.STOP_SERVER)
        .addOption(CliStrings.STOP_SERVER__DIR, serverDir);
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

}
