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
package org.apache.geode.management;

import static org.apache.geode.test.junit.rules.gfsh.GfshRule.startLocatorCommand;
import static org.apache.geode.test.junit.rules.gfsh.GfshRule.startServerCommand;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
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
  private final UniquePortSupplier portSupplier = new UniquePortSupplier();
  private final String oldVersion;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(s -> TestVersion.compare(s, "1.10.0") < 0);
    return result;
  }

  @Rule
  public GfshRule oldGfsh;

  @Rule
  public GfshRule currentGfsh = new GfshRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  public RollingUpgradeWithGfshDUnitTest(String version) {
    oldVersion = version;
    oldGfsh = new GfshRule(oldVersion);
  }

  @Test
  public void testRollingUpgradeWithDeployment() throws Exception {
    int locatorPort = portSupplier.getAvailablePort();
    int locatorJmxPort = portSupplier.getAvailablePort();
    int locator2Port = portSupplier.getAvailablePort();
    int locator2JmxPort = portSupplier.getAvailablePort();
    int server1Port = portSupplier.getAvailablePort();
    int server2Port = portSupplier.getAvailablePort();

    GfshExecution startupExecution =
        GfshScript.of(startLocatorCommand("loc1", locatorPort, locatorJmxPort, 0, -1))
            .and(startLocatorCommand("loc2", locator2Port, locator2JmxPort, 0, locatorPort))
            .and(startServerCommand("server1", server1Port, locatorPort))
            .and(startServerCommand("server2", server2Port, locatorPort))
            .and(deployDirCommand())
            .execute(oldGfsh);

    // doing rolling upgrades
    oldGfsh.stopLocator(startupExecution, "loc1");
    GfshScript.of(startLocatorCommand("loc1", locatorPort, locatorJmxPort, 0, locator2Port))
        .execute(currentGfsh);
    verifyListDeployed(locatorPort);

    oldGfsh.stopLocator(startupExecution, "loc2");
    GfshScript.of(startLocatorCommand("loc2", locator2Port, locator2JmxPort, 0, locatorPort))
        .execute(currentGfsh);
    verifyListDeployed(locator2Port);

    // make sure servers can do rolling upgrade too
    oldGfsh.stopServer(startupExecution, "server1");
    GfshScript.of(startServerCommand("server1", server1Port, locatorPort)).execute(currentGfsh);

    oldGfsh.stopServer(startupExecution, "server2");
    GfshScript.of(startServerCommand("server2", server2Port, locatorPort)).execute(currentGfsh);
  }

  private void verifyListDeployed(int locatorPort) {
    GfshExecution list_deployed = GfshScript.of("connect --locator=localhost[" + locatorPort + "]")
        .and("list deployed").execute(currentGfsh);
    assertThat(list_deployed.getOutputText()).contains("DeployCommandsDUnit1.jar")
        .contains("server1").contains("server2");
    currentGfsh.execute("disconnect");
  }

  private String deployDirCommand() throws IOException {
    ClassBuilder classBuilder = new ClassBuilder();
    File jarsDir = tempFolder.newFolder();
    String jarName1 = "DeployCommandsDUnit1.jar";
    File jar1 = new File(jarsDir, jarName1);
    String class1 = "DeployCommandsDUnitA";
    classBuilder.writeJarFromName(class1, jar1);
    return "deploy --dir=" + jarsDir.getAbsolutePath();
  }
}
