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

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.jdk.ReflectEncapsulatedJdkObject.OBJECT;
import static org.apache.geode.test.util.JarUtils.createJarWithClasses;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.version.JavaVersions;

/**
 * Test several ways to make encapsulated types in JDK packages accessible for reflection on JDK 17.
 */
public class JdkEncapsulationTest {
  @Rule(order = 0)
  public final FolderRule folderRule = new FolderRule();

  @Rule(order = 1)
  public final GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  private String startServer;
  private GfshScript reflectEncapsulatedJdkObject;

  @BeforeClass
  public static void enableTheseTestsOnlyOnJdk17AndLater() {
    assumeThat(JavaVersions.current().specificationVersion())
        .isGreaterThanOrEqualTo(17);
  }

  @Before
  public void startLocatorAndDeployReflectionFunction() throws IOException {
    Path jarPath = folderRule.getFolder().toPath().resolve("reflect-encapsulated-jdk-object.jar");
    createJarWithClasses(jarPath, ReflectEncapsulatedJdkObject.class);

    int locatorPort = getRandomAvailableTCPPort();
    String locators = "localhost[" + locatorPort + "]";

    startServer = "start server --name=server --disable-default-server --locators=" + locators;
    reflectEncapsulatedJdkObject = GfshScript
        .of("connect --locator=" + locators)
        .and("execute function --id=" + ReflectEncapsulatedJdkObject.ID);

    GfshScript
        .of("start locator --port=" + locatorPort)
        .and("deploy --jar=" + jarPath)
        .execute(gfshRule);
  }

  /**
   * If this test fails, it means the object we're trying to reflect has no inaccessible fields,
   * and so is not useful for the other tests. If it fails, update
   * {@link ReflectEncapsulatedJdkObject} to use a type that has inaccessible fields.
   */
  @Test
  public void cannotMakeEncapsulatedFieldsAccessibleByDefault() {
    gfshRule.execute(startServer); // No JDK options

    String reflectionResult = reflectEncapsulatedJdkObject
        .expectExitCode(1) // Because we did not open any JDK packages.
        .execute(gfshRule)
        .getOutputText();

    assertThat(reflectionResult)
        .as("result of reflecting %s", OBJECT.getClass())
        .contains("Exception: java.lang.reflect.InaccessibleObjectException");
  }

  @Test
  public void canMakeEncapsulatedFieldsAccessibleInExplicitlyOpenedPackages() {
    String objectPackage = OBJECT.getClass().getPackage().getName();
    String objectModule = ReflectEncapsulatedJdkObject.MODULE;

    String openThePackageOfTheEncapsulatedJdkObject =
        String.format(" --J=--add-opens=%s/%s=ALL-UNNAMED", objectModule, objectPackage);

    gfshRule.execute(startServer + openThePackageOfTheEncapsulatedJdkObject);

    reflectEncapsulatedJdkObject
        .expectExitCode(0) // Because we opened the encapsulated object's package.
        .execute(gfshRule);
  }

  @Test
  public void canMakeEncapsulatedFieldsAccessibleInPackagesOpenedByArgumentFile() {
    // A few of the packages opened by this argument file are specific to Linux JDKs. Running this
    // test on other operating systems might emit some warnings, but they are harmless.
    String argumentFileName = "open-all-jdk-packages-linux-openjdk-17";
    Path argumentFilePath = Paths.get(System.getenv("GEODE_HOME"), "config", argumentFileName)
        .toAbsolutePath().normalize();

    String useArgumentFile = " --J=@" + argumentFilePath;

    gfshRule.execute(startServer + useArgumentFile);

    reflectEncapsulatedJdkObject
        .expectExitCode(0) // Because the argument file opens all JDK packages.
        .execute(gfshRule);
  }
}
