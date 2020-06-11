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
package org.apache.geode.internal.cache.rollingupgrade;

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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.rules.GfshCommandRule;
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
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

/**
 * This test iterates through the versions of Geode and executes client compatibility with
 * the current version of Geode.
 */
@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RollingUpgradeWithGfshDUnitTest {
  private static final String REGIONNAME = "myRegion";
  private final UniquePortSupplier portSupplier = new UniquePortSupplier();
  private final String oldVersion;
  private String locatorDir;
  private String locator2Dir;
  private String server1Dir;
  private String server2Dir;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    //
    result.removeIf(s -> TestVersion.compare(s, "1.7.0") < 0);
    return result;
  }

  @Rule
  public transient GfshRule oldGfsh;

  @Rule
  public final transient GfshRule currentGfsh = new GfshRule();

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public transient TestName testName = new TestName();

  protected String hostname;
  protected int locatorPort;
  protected int locatorJmxPort;
  protected int locator2Port;
  protected int locator2JmxPort;

  public RollingUpgradeWithGfshDUnitTest(String version) {
    oldVersion = version;
    oldGfsh = new GfshRule(oldVersion);
  }

  protected String getLocatorsString() {
    return hostname + "[" + locatorPort + "]";
  }

  protected void startServer(String name, String locatorsString, GfshRule gfsh, int port,
                             String serverDir) {
    String command = "start server --name=" + name
            + " --server-port=" + port
            + " --locators=" + locatorsString
            + " --dir=" + serverDir
            + " --use-cluster-configuration=yes";
    gfsh.execute(GfshScript.of(command));
  }

  protected void startLocator(String name, int port, int jmxPort, GfshRule gfsh,
                              String locatorDir, String locators) {
    String command = "start locator --name=" + name
            + " --port=" + port
            + " --dir=" + locatorDir + " --http-service-port=0"
            + " --enable-cluster-configuration=true";
    if (!locators.equals("")) {
      command += " --locators=" + locators;
    }
    command += " --J=-Dgemfire.jmx-manager-port=" + jmxPort;
    gfsh.execute(GfshScript.of(command).expectExitCode(0));
  }

  @Before
  public void setup() throws Exception {
    locatorPort = portSupplier.getAvailablePort();
    locatorJmxPort = portSupplier.getAvailablePort();
    locator2Port = portSupplier.getAvailablePort();
    locator2JmxPort = portSupplier.getAvailablePort();

    locatorDir = tempFolder.newFolder("loc").getPath();
    locator2Dir = tempFolder.newFolder("loc2").getPath();
    server1Dir = tempFolder.newFolder("server1").getPath();
    server2Dir = tempFolder.newFolder("server2").getPath();
  }

  private String getConnectCommand(String hostname, int locatorPort) {
    return "connect --locator=" + hostname + "[" + locatorPort+ "]";
  }

  @After
  public void stop() {
    String connectCommand = getConnectCommand(hostname, locatorPort);
    String shutdownCommand = "shutdown --include-locators";
    final GfshScript script = GfshScript.of(connectCommand, shutdownCommand);
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
  public void testRollingUpgradeOfGeodeWithGfs() throws Exception {
    hostname = "localhost";
    int server1Port = portSupplier.getAvailablePort();
    int server2Port = portSupplier.getAvailablePort();
    startLocator("loc", locatorPort, locatorJmxPort, oldGfsh, locatorDir, "");
    startLocator("loc2", locator2Port, locator2JmxPort, oldGfsh, locator2Dir, getLocatorsString());
    startServer("server1", getLocatorsString(), oldGfsh, server1Port, server1Dir);
    startServer("server2", getLocatorsString(), oldGfsh, server2Port, server2Dir);
    createRegion(oldGfsh);

    // Coming from version 1.12, the locator in the new version does not start if
    // custom jars are deployed like in the following line:
    deployDir(oldGfsh);

    putSomeEntries(oldGfsh);

    stopLocator(oldGfsh, locatorDir);
    startLocator("loc", locatorPort, locatorJmxPort, currentGfsh, locatorDir, "");

    stopLocator(oldGfsh, locator2Dir);
    startLocator("loc2", locator2Port, locator2JmxPort, currentGfsh, locator2Dir, "");

    stopServer(oldGfsh, server1Dir);
    startServer("server1", getLocatorsString(), currentGfsh, server1Port, server1Dir);

    // With the following check some versions do not work:
    // Coming from versions 1.2.0 and 1.3.0 it shows an error
    // Coming from versions 1.4.0, 1.5.0 and 1.6.0 it hangs
    if (!oldVersion.equals("1.4.0") && !oldVersion.equals("1.5.0")) {
      verifyEntries();
    }

    stopServer(currentGfsh, server2Dir);
    startServer("server2", getLocatorsString(), currentGfsh, server2Port, server2Dir);

    verifyEntries();

    // Coming from version 1.12, the following two checks fail (in order to see them, no jars
    // must be deployed. Otherwise, the starting of the first locator will fail before).
    // The first locator upgraded
    // shows in "list members" that he is still on version 1.12 even though it was restarted.
    // The shutdown command fails indicating that it cannot find the first locator.
    checkListMembersDoesNotContainVersion(oldVersion);
    executeAndCheckShutdown();
  }

  private void deployDir(GfshRule oldGfsh) throws IOException {
    ClassBuilder classBuilder = new ClassBuilder();
    File jarsDir = tempFolder.newFolder();
    String jarName1 = "DeployCommandsDUnit1.jar";
    File jar1 = new File(jarsDir, jarName1);
    String class1 = "DeployCommandsDUnitA";
    classBuilder.writeJarFromName(class1, jar1);
    deployDirWithGfsh(oldGfsh, jarsDir.getAbsolutePath());
  }

  private void createRegion(GfshRule gfsh) {
    String connectCommand = getConnectCommand(hostname, locatorPort);
    String createRegionCommand = "create region --name=" + REGIONNAME + " --type="
            + RegionShortcut.PARTITION_REDUNDANT_PERSISTENT.name()
            + " --enable-statistics=true";
    final GfshScript script = GfshScript.of(connectCommand, createRegionCommand);
    gfsh.execute(script);
  }

  private void stopLocator(GfshRule gfsh, String locatorDir) {
    String command = "stop locator --dir=" + locatorDir;
    gfsh.execute(command);
  }

  private void stopServer(GfshRule gfsh, String serverDir) {
    String command = "stop server --dir=" + serverDir;
    gfsh.execute(command);
  }

  private void deployDirWithGfsh(GfshRule gfsh, String dir) {
    String connectCommand = getConnectCommand(hostname, locatorPort);
    String deployCommand = "deploy --dir=" + dir;
    final GfshScript script = GfshScript.of(connectCommand, deployCommand);
    gfsh.execute(script);
  }

  private void checkListMembersDoesNotContainVersion(String startingVersion) throws Exception {
    String listMembersCommand = "list members";
    gfsh.connectAndVerify(locatorJmxPort, GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat(listMembersCommand).statusIsSuccess()
            .doesNotContainOutput(startingVersion);
  }

  private void executeAndCheckShutdown() throws Exception {
    String shutDownCommand = "shutdown --include-locators=true";
    gfsh.connectAndVerify(locatorJmxPort, GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat(shutDownCommand).statusIsSuccess();
  }

  private void putSomeEntries(GfshRule gfsh) {
    String connectCommand = getConnectCommand(hostname, locatorPort);
    String putCommand1 = "put --key=0 --value=0 --region=" + REGIONNAME;
    String putCommand2 = "put --key=1 --value=1 --region=" + REGIONNAME;
    String putCommand3 = "put --key=2 --value=2 --region=" + REGIONNAME;
    final GfshScript script = GfshScript.of(connectCommand, putCommand1, putCommand2, putCommand3);
    gfsh.execute(script);
  }

  private void verifyEntries() throws Exception {
    String listMembersCommand = "query --query=\"select count(*) from /" + REGIONNAME + "\"";
    gfsh.connectAndVerify(locatorJmxPort, GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat(listMembersCommand).statusIsSuccess()
            .containsOutput("3");
  }
}
