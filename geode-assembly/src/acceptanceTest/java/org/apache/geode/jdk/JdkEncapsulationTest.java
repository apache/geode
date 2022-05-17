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
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.condition.OS.LINUX;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

import org.apache.geode.rules.JarWithClassesRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

/**
 * Test several ways to make normally inaccessible JDK packages accessible on JDK 17.
 */
@EnableRuleMigrationSupport
@EnabledForJreRange(min = JRE.JAVA_17)
public class JdkEncapsulationTest {
  private static final String TRAVERSE_BIG_DECIMAL = String.join(" ",
      "execute", "function", "--id=" + TraverseInaccessibleJdkObject.ID);

  private static final Path LINUX_ARGUMENT_FILE_RELATIVE_PATH =
      Paths.get(System.getenv("GEODE_HOME"), "config", "open-all-jdk-packages-linux-openjdk-17")
          .toAbsolutePath().normalize();

  @Rule
  public JarWithClassesRule jarRule = new JarWithClassesRule();

  @Rule
  public GfshRule gfshRule = new GfshRule();
  private String connectToLocator;
  private int serverPort;
  private String locatorString;

  @BeforeEach
  void startLocatorWithObjectTraverserFunction() throws IOException {
    int[] availablePorts = getRandomAvailableTCPPorts(2);
    int locatorPort = availablePorts[0];
    serverPort = availablePorts[1];
    locatorString = "localhost[" + locatorPort + "]";
    connectToLocator = "connect --locator=" + locatorString;

    String startLocator = String.join(" ",
        "start",
        "locator",
        "--name=locator",
        "--port=" + locatorPort,
        "--http-service-port=0");

    Path jarPath =
        jarRule.createJarWithClasses("traverse-object.jar", TraverseInaccessibleJdkObject.class);
    String deployObjectTraverserFunction = String.join(" ",
        "deploy",
        "--jar=" + jarPath);
    gfshRule.execute(startLocator, deployObjectTraverserFunction);
  }

  // If this test fails, it means the object we're trying to traverse has no inaccessible fields,
  // and so is not useful for the other tests. If it fails, update TraverseInaccessibleJdkObject
  // to use a type that actually has inaccessible fields.
  @Test
  void cannotMakeInaccessibleFieldsAccessibleByDefault() {
    gfshRule.execute(startServerWithOptions()); // No options

    GfshExecution execution = GfshScript.of(connectToLocator)
        .and(TRAVERSE_BIG_DECIMAL)
        .expectExitCode(1) // Because java.math is not opened by default.
        .execute(gfshRule);

    assertThat(execution.getOutputText())
        .as("result of traversing %s", TraverseInaccessibleJdkObject.OBJECT.getClass())
        .contains("Exception: java.lang.reflect.InaccessibleObjectException");
  }

  @Test
  void canMakeFieldsAccessibleOnExplicitlyOpenedPackages() {
    String opensOptionFormat = "--J=--add-opens=%s/%s=ALL-UNNAMED";
    String objectPackage = TraverseInaccessibleJdkObject.OBJECT.getClass().getPackage().getName();
    String objectModule = TraverseInaccessibleJdkObject.MODULE;
    String opensOption = String.format(opensOptionFormat, objectModule, objectPackage);
    gfshRule.execute(startServerWithOptions(opensOption));

    GfshScript.of(connectToLocator)
        .and(TRAVERSE_BIG_DECIMAL)
        .expectExitCode(0) // Because we explicitly opened java.math.
        .execute(gfshRule);
  }

  @EnabledOnOs(LINUX)
  @Test
  void canMakeFieldsAccessibleOnPackagesOpenedByArgumentFile() {
    gfshRule.execute(startServerWithOptions("--J=@" + LINUX_ARGUMENT_FILE_RELATIVE_PATH));

    GfshScript.of(connectToLocator)
        .and(TRAVERSE_BIG_DECIMAL)
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
