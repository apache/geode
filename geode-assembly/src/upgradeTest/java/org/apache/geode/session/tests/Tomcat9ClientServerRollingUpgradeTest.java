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

import static org.apache.geode.session.tests.TomcatInstall.TomcatVersion.TOMCAT9_0_20;
import static org.apache.geode.session.tests.TomcatInstall.TomcatVersion.TOMCAT9_RECENT;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import org.codehaus.cargo.container.ContainerException;
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
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

/**
 * This test iterates through the versions of Geode and executes session client compatibility with
 * the current version of Geode.
 */
@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class Tomcat9ClientServerRollingUpgradeTest {
  private final UniquePortSupplier portSupplier = new UniquePortSupplier();
  private final String oldVersion;
  private String locatorDir;
  private String server1Dir;
  private String server2Dir;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    // The Tomcat 9 module was not added until Geode 1.8.0
    String minimumVersion = "1.8.0";
    result.removeIf(s -> TestVersion.compare(s, minimumVersion) < 0);
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


  protected TomcatInstall tomcat9AndOldModules;
  protected TomcatInstall tomcat9AndCurrentModules;

  protected int locatorPort;
  protected int locatorJmxPort;

  protected String classPathTomcat9AndCurrentModules;
  private String classPathTomcat9AndOldModules;

  public Tomcat9ClientServerRollingUpgradeTest(String version) {
    this.oldVersion = version;
    oldGfsh = new GfshRule(oldVersion);
  }

  protected void startServer(String name, String classPath, int locatorPort, GfshRule gfsh,
      String serverDir) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.START_SERVER);
    command.addOption(CliStrings.START_SERVER__NAME, name);
    command.addOption(CliStrings.START_SERVER__SERVER_PORT, "0");
    command.addOption(CliStrings.START_SERVER__CLASSPATH, classPath);
    command.addOption(CliStrings.START_SERVER__LOCATORS, "localhost[" + locatorPort + "]");
    command.addOption(CliStrings.START_SERVER__DIR, serverDir);
    gfsh.execute(GfshScript.of(command.toString()).expectExitCode(0));
  }

  protected void startLocator(String classPath, int port, GfshRule gfsh, String locatorDir) {
    CommandStringBuilder locStarter = new CommandStringBuilder(CliStrings.START_LOCATOR);
    locStarter.addOption(CliStrings.START_LOCATOR__MEMBER_NAME, "loc");
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
    TomcatInstall.TomcatVersion tomcatVersion = TOMCAT9_0_20;
    // If Geode version is 1.12.2 or 1.13.3+, Tomcat 9 version > 9.0.20 may be used
    if (TestVersion.compare(oldVersion, "1.13.2") > 0 ||
        (TestVersion.compare(oldVersion, "1.13.0") < 0
            && TestVersion.compare(oldVersion, "1.12.1") > 0)) {
      tomcatVersion = TOMCAT9_RECENT;
    }

    tomcat9AndOldModules =
        new TomcatInstall("Tomcat9AndOldModules", tomcatVersion,
            ContainerInstall.ConnectionType.CLIENT_SERVER,
            oldModules.getAbsolutePath(),
            oldBuild.getAbsolutePath() + "/lib",
            portSupplier::getAvailablePort, TomcatInstall.CommitValve.DEFAULT);

    tomcat9AndCurrentModules =
        new TomcatInstall("Tomcat9AndCurrentModules", TOMCAT9_RECENT,
            ContainerInstall.ConnectionType.CLIENT_SERVER,
            portSupplier::getAvailablePort, TomcatInstall.CommitValve.DEFAULT);

    classPathTomcat9AndOldModules = getClassPathTomcat9AndOldModules();

    classPathTomcat9AndCurrentModules = getClassPathTomcat9AndCurrentModules();

    // Get available port for the locator
    locatorPort = portSupplier.getAvailablePort();
    locatorJmxPort = portSupplier.getAvailablePort();

    tomcat9AndOldModules.setDefaultLocatorPort(locatorPort);
    tomcat9AndCurrentModules.setDefaultLocatorPort(locatorPort);

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
    try {
      manager.stopAllActiveContainers();
    } catch (ContainerException e) {
      // container might fail to start within certain timeframe for older Geode versions due to
      // classcast exception that was fixed in later versions
    }

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

    startLocator(classPathTomcat9AndOldModules, locatorPort, oldGfsh, locatorDir);
    startServer("server1", classPathTomcat9AndOldModules, locatorPort, oldGfsh, server1Dir);
    startServer("server2", classPathTomcat9AndOldModules, locatorPort, oldGfsh, server2Dir);
    createRegion(oldGfsh);

    // Start two tomcat servers with the old geode modules
    ServerContainer container1 = manager.addContainer(tomcat9AndOldModules);
    ServerContainer container2 = manager.addContainer(tomcat9AndOldModules);

    // This has to happen at the start of every test
    manager.startAllInactiveContainers();

    verifySessionReplication();

    // Upgrade the geode locator
    stopLocator(oldGfsh, locatorDir);
    startLocator(classPathTomcat9AndCurrentModules, locatorPort, currentGfsh, locatorDir);

    // Upgrade a server
    stopServer(oldGfsh, server1Dir);
    startServer("server1", classPathTomcat9AndCurrentModules, locatorPort, currentGfsh, server1Dir);

    // verify again
    verifySessionReplication();

    // Upgrade second server
    stopServer(oldGfsh, server2Dir);
    startServer("server2", classPathTomcat9AndCurrentModules, locatorPort, currentGfsh, server2Dir);

    // verify again
    verifySessionReplication();

    // Upgrade a tomcat server
    try {
      container1.stop();
    } catch (ContainerException e) {
      // container might fail to start within certain timeframe for older Geode versions due to
      // classcast exception that was fixed in later versions
    }
    manager.removeContainer(container1);
    ServerContainer newContainer1 = manager.addContainer(tomcat9AndCurrentModules);
    newContainer1.start();

    verifySessionReplication();

    // Upgrade the second tomcat server
    try {
      container2.stop();
    } catch (ContainerException e) {
      // container might fail to start within certain timeframe for older Geode versions due to
      // classcast exception that was fixed in later versions
    }
    manager.removeContainer(container2);
    ServerContainer newContainer2 = manager.addContainer(tomcat9AndCurrentModules);
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
  private String getClassPathTomcat9AndOldModules() {
    final String[] requiredClasspathJars = {
        "/lib/geode-modules-" + oldVersion + ".jar",
        "/lib/geode-modules-tomcat9-" + oldVersion + ".jar",
        "/lib/servlet-api.jar",
        "/lib/catalina.jar",
        "/lib/tomcat-util.jar",
        "/bin/tomcat-juli.jar"
    };

    return getRequiredClasspathJars(tomcat9AndOldModules.getHome(), requiredClasspathJars);
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
  private String getClassPathTomcat9AndCurrentModules() {
    String currentVersion = GemFireVersion.getGemFireVersion();

    final String[] requiredClasspathJars = {
        "/lib/geode-modules-" + currentVersion + ".jar",
        "/lib/geode-modules-tomcat9-" + currentVersion + ".jar",
        "/lib/servlet-api.jar",
        "/lib/catalina.jar",
        "/lib/tomcat-util.jar",
        "/bin/tomcat-juli.jar"
    };

    return getRequiredClasspathJars(tomcat9AndCurrentModules.getHome(), requiredClasspathJars);
  }

  private String getRequiredClasspathJars(final String tomcat9AndRequiredModules,
      final String[] requiredClasspathJars) {
    StringBuilder completeJarList = new StringBuilder();
    for (String requiredJar : requiredClasspathJars) {
      completeJarList.append(tomcat9AndRequiredModules)
          .append(requiredJar)
          .append(File.pathSeparator);
    }

    completeJarList.deleteCharAt(completeJarList.length() - 1);

    return completeJarList.toString();
  }
}
