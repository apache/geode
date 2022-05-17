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
 *
 */

package org.apache.geode.jdk;

import static java.util.stream.Collectors.joining;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.util.JarUtils.createJarWithClasses;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.version.JavaVersions;

/**
 * Test several ways to make normally inaccessible JDK packages accessible on JDK 17.
 */
public class JdkEncapsulationTest {
  private static final String TRAVERSE_INACCESSIBLE_OBJECT = String.join(" ",
      "execute", "function", "--id=" + TraverseInaccessibleJdkObject.ID);

  private static final Path LINUX_ARGUMENT_FILE_RELATIVE_PATH =
      Paths.get(System.getenv("GEODE_HOME"), "config", "open-all-jdk-packages-linux-openjdk-17")
          .toAbsolutePath().normalize();

  @Rule(order = 0)
  public final FolderRule folderRule = new FolderRule();

  @Rule(order = 1)
  public final GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  private String connectToLocator;
  private int serverPort;
  private String locatorString;

  @BeforeClass
  public static void validOnlyOnJdk17AndLater() {
    assumeThat(JavaVersions.current().specificationVersion())
        .isGreaterThanOrEqualTo(17);
  }

  @Before
  public void startLocatorWithObjectTraverserFunction() throws IOException {
    int[] availablePorts = getRandomAvailableTCPPorts(2);
    int locatorPort = availablePorts[0];
    serverPort = availablePorts[1];
    locatorString = "localhost[" + locatorPort + "]";
    connectToLocator = "connect --locator=" + locatorString;

    String startLocator = String.join(" ",
        "start",
        "locator",
        "--port=" + locatorPort,
        "--http-service-port=0");

    Path jarPath = folderRule.getFolder().toPath().resolve("traverse-object.jar");
    createJarWithClasses(jarPath, TraverseInaccessibleJdkObject.class);
    String deployObjectTraverserFunction = String.join(" ",
        "deploy",
        "--jar=" + jarPath);
    gfshRule.execute(startLocator, deployObjectTraverserFunction);
  }

  // If this test fails, it means the object we're trying to traverse has no inaccessible fields,
  // and so is not useful for the other tests. If it fails, update TraverseInaccessibleJdkObject
  // to use a type that actually has inaccessible fields.
  @Test
  public void cannotMakeInaccessibleFieldsAccessibleByDefault() {
    gfshRule.execute(startServerWithOptions()); // No options

    GfshExecution execution = GfshScript.of(connectToLocator)
        .and(TRAVERSE_INACCESSIBLE_OBJECT)
        .expectExitCode(1) // Because java.math is not opened by default.
        .execute(gfshRule);

    assertThat(execution.getOutputText())
        .as("result of traversing %s", TraverseInaccessibleJdkObject.OBJECT.getClass())
        .contains("Exception: java.lang.reflect.InaccessibleObjectException");
  }

  @Test
  public void canMakeFieldsAccessibleOnExplicitlyOpenedPackages() {
    String opensOptionFormat = "--J=--add-opens=%s/%s=ALL-UNNAMED";
    String objectPackage = TraverseInaccessibleJdkObject.OBJECT.getClass().getPackage().getName();
    String objectModule = TraverseInaccessibleJdkObject.MODULE;
    String opensOption = String.format(opensOptionFormat, objectModule, objectPackage);
    gfshRule.execute(startServerWithOptions(opensOption));

    GfshScript.of(connectToLocator)
        .and(TRAVERSE_INACCESSIBLE_OBJECT)
        .expectExitCode(0) // Because we explicitly opened java.math.
        .execute(gfshRule);
  }

  @Test
  public void canMakeFieldsAccessibleOnPackagesOpenedByArgumentFile() {
    // A few of the packages opened by this argument file are specific to Linux JDKs. Running this
    // test on other operating systems might emit some warnings, but they are harmless.
    gfshRule.execute(startServerWithOptions("--J=@" + LINUX_ARGUMENT_FILE_RELATIVE_PATH));

    GfshScript.of(connectToLocator)
        .and(TRAVERSE_INACCESSIBLE_OBJECT)
        .expectExitCode(0) // Because the argument file opens java.math.
        .execute(gfshRule);
  }

  private String startServerWithOptions(String... option) {
    Stream<String> baseCommand = Stream.of(
        "start",
        "server",
        "--name=server",
        "--server-port=" + serverPort,
        "--locators=" + locatorString);
    Stream<String> options = Stream.of(option);

    return Stream.concat(baseCommand, options)
        .collect(joining(" "));
  }
}
