/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.modules;


import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class DeployJarAcceptanceTest {

  @ClassRule
  public static GfshRule gfshRule = new GfshRule();

  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();

  private static File jarFile;
  private static File jarFileV2;
  private static File anotherJarFile;

  @BeforeClass
  public static void setup() throws IOException {
    File stagingDir = stagingTempDir.newFolder("staging");
    jarFile = new File(stagingDir, "myJar-1.0.jar");
    jarFileV2 = new File(stagingDir, "myJar-2.0.jar");
    anotherJarFile = new File(stagingDir, "anotherJar-1.0.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(jarFile, "SomeClass");
    jarBuilder.buildJarFromClassNames(jarFileV2, "SomeClass", "SomeClassVersionTwo");
    jarBuilder.buildJarFromClassNames(anotherJarFile, "SomeOtherClass");

    GfshScript
        .of("start locator --name=locator", "configure pdx --read-serialized=true",
            "start server --name=server --locators=localhost[10334]")
        .execute(gfshRule);
  }

  @After
  public void teardown() {
    System.out.println(GfshScript.of("connect --locator=localhost[10334]", "undeploy")
        .execute(gfshRule).getOutputText());
  }

  @Test
  public void testDeployJar() throws IOException {
    GfshScript.of("connect --locator=localhost[10334]",
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of("connect --locator=localhost[10334]", "list deployed")
        .execute(gfshRule).getOutputText()).contains(jarFile.getName()).contains("JAR Location");
  }

  @Test
  public void testDeployJarWithDeploymentName() throws IOException {
    GfshScript.of("connect --locator=localhost[10334]",
        "deploy --name=myDeployment --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of("connect --locator=localhost[10334]", "list deployed")
        .execute(gfshRule).getOutputText()).contains("myDeployment").contains("JAR Location");
  }

  @Test
  public void testUndeployJar() throws IOException {
    GfshScript.of("connect --locator=localhost[10334]",
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(
        GfshScript.of("connect --locator=localhost[10334]",
            "undeploy --jar=" + jarFile.getName())
            .execute(gfshRule).getOutputText()).contains(jarFile.getName())
                .contains("Un-Deployed From JAR Location");

    assertThat(GfshScript.of("connect --locator=localhost[10334]", "list deployed")
        .execute(gfshRule).getOutputText()).doesNotContain(jarFile.getName());
  }

  @Test
  public void testUndeployWithNothingDeployed() {
    assertThat(
        GfshScript.of("connect --locator=localhost[10334]",
            "undeploy --jar=" + jarFile.getName())
            .execute(gfshRule).getOutputText()).contains(jarFile.getName() + " not deployed");
  }

  @Test
  public void testRedeployNewJar() throws IOException {
    GfshScript.of("connect --locator=localhost[10334]",
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(
        GfshScript.of("connect --locator=localhost[10334]",
            "undeploy --jar=" + jarFile.getName())
            .execute(gfshRule).getOutputText()).contains(jarFile.getName())
                .contains("Un-Deployed From JAR Location");

    assertThat(GfshScript.of("connect --locator=localhost[10334]", "list deployed")
        .execute(gfshRule).getOutputText()).doesNotContain(jarFile.getName());

    GfshScript
        .of("connect --locator=localhost[10334]",
            "deploy --jar=" + anotherJarFile.getCanonicalPath())
        .execute(gfshRule);
    assertThat(GfshScript.of("connect --locator=localhost[10334]", "list deployed")
        .execute(gfshRule).getOutputText()).contains(anotherJarFile.getName());
  }

  @Test
  public void testUpdateJar() throws IOException {
    GfshScript.of("connect --locator=localhost[10334]",
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    GfshScript.of("connect --locator=localhost[10334]",
        "deploy --jar=" + jarFileV2.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of("connect --locator=localhost[10334]",
        "list deployed").execute(gfshRule).getOutputText()).contains(jarFileV2.getName())
            .doesNotContain(jarFile.getName());
  }

  @Test
  public void testDeployMultipleJars() throws IOException {
    GfshScript.of("connect --locator=localhost[10334]",
        "deploy --jar=" + jarFile.getCanonicalPath(),
        "deploy --jar=" + anotherJarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of("connect --locator=localhost[10334]",
        "list deployed").execute(gfshRule).getOutputText()).contains(jarFile.getName())
            .contains(anotherJarFile.getName());
  }
}
