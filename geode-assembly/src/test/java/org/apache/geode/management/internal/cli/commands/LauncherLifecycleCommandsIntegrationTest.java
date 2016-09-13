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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * The LauncherLifecycleCommandsJUnitTest class is a test suite of test cases testing the contract and functionality of
 * the lifecycle launcher GemFire shell (Gfsh) commands.
 *
 * @see com.gemstone.gemfire.management.internal.cli.commands.LauncherLifecycleCommands
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(IntegrationTest.class)
public class LauncherLifecycleCommandsIntegrationTest {

  private LauncherLifecycleCommands launcherCommands;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setup() {
    launcherCommands = new LauncherLifecycleCommands();
  }

  @After
  public void tearDown() {
    launcherCommands = null;
  }

  @Test
  public void testGemFireCoreClasspath() throws IOException {
    File coreDependenciesJar = new File(LauncherLifecycleCommands.CORE_DEPENDENCIES_JAR_PATHNAME);

    assertNotNull(coreDependenciesJar);
    assertTrue(coreDependenciesJar + " is not a file", coreDependenciesJar.isFile());

    Collection<String> expectedJarDependencies = Arrays.asList("antlr", "commons-io", "commons-lang", "commons-logging",
            "geode", "jackson-annotations", "jackson-core", "jackson-databind", "jansi", "jline", "snappy",
            "spring-core", "spring-shell", "jetty-server", "jetty-servlet", "jetty-webapp", "jetty-util", "jetty-http",
            "servlet-api", "jetty-io", "jetty-security", "jetty-xml"

    );

    assertJarFileManifestClassPath(coreDependenciesJar, expectedJarDependencies);
  }

  @Test
  public void testReadPid() throws IOException {
    final int expectedPid = 12345;

    File folder = temporaryFolder.newFolder();
    File pidFile = new File(folder, getClass().getSimpleName() + "_" + testName.getMethodName() + ".pid");

    assertTrue(pidFile.createNewFile());

    pidFile.deleteOnExit();
    writePid(pidFile, expectedPid);

    final int actualPid = getLauncherLifecycleCommands().readPid(pidFile);

    assertEquals(expectedPid, actualPid);
  }

  private LauncherLifecycleCommands getLauncherLifecycleCommands() {
    return launcherCommands;
  }

  private void writePid(final File pidFile, final int pid) throws IOException {
    final FileWriter fileWriter = new FileWriter(pidFile, false);
    fileWriter.write(String.valueOf(pid));
    fileWriter.write("\n");
    fileWriter.flush();
    IOUtils.close(fileWriter);
  }

  private void assertJarFileManifestClassPath(final File dependenciesJar,
                                              final Collection<String> expectedJarDependencies) throws IOException {
    JarFile dependenciesJarFile = new JarFile(dependenciesJar);
    Manifest manifest = dependenciesJarFile.getManifest();

    assertNotNull(manifest);

    Attributes attributes = manifest.getMainAttributes();

    assertNotNull(attributes);
    assertTrue(attributes.containsKey(Name.CLASS_PATH));

    String[] actualJarDependencies = attributes.getValue(Name.CLASS_PATH).split(" ");

    assertNotNull(actualJarDependencies);
    assertTrue(String.format("Expected the actual number of JAR dependencies to be (%1$d); but was (%2$d)!",
            expectedJarDependencies.size(), actualJarDependencies.length),
            actualJarDependencies.length >= expectedJarDependencies.size());
    //assertTrue(Arrays.asList(actualJarDependencies).containsAll(expectedJarDependencies));

    List<String> actualJarDependenciesList = new ArrayList<>(Arrays.asList(actualJarDependencies));
    List<String> missingExpectedJarDependenciesList = new ArrayList<>(expectedJarDependencies.size());

    for (String expectedJarDependency : expectedJarDependencies) {
      boolean containsExpectedJar = false;

      for (int index = 0, size = actualJarDependenciesList.size(); index < size; index++) {
        if (actualJarDependenciesList.get(index).toLowerCase().contains(expectedJarDependency.toLowerCase())) {
          actualJarDependenciesList.remove(index);
          containsExpectedJar = true;
          break;
        }
      }

      if (!containsExpectedJar) {
        missingExpectedJarDependenciesList.add(expectedJarDependency);
      }
    }

    assertTrue(String.format(
            "GemFire dependencies JAR file (%1$s) does not contain the expected dependencies (%2$s) in the Manifest Class-Path attribute (%3$s)!",
            dependenciesJar, missingExpectedJarDependenciesList, attributes.getValue(Name.CLASS_PATH)),
            missingExpectedJarDependenciesList.isEmpty());
  }

}
