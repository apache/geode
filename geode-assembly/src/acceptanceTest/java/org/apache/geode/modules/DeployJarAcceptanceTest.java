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


import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
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
    System.out.println(GfshScript.of(getLocatorGFSHConnectionString(), "undeploy")
        .execute(gfshRule).getOutputText());
  }

  private String getLocatorGFSHConnectionString() {
    return "connect --locator=localhost[10334]";
  }

  @Test
  public void testDeployJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).contains(jarFile.getName()).contains("JAR Location");
  }

  @Test
  public void testDeployExistingJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).contains(jarFile.getName()).contains("JAR Location");

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule).getOutputText())
            .contains("Already deployed");

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).contains(jarFile.getName()).contains("JAR Location");
  }

  @Test
  public void testUndeployJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(),
            "undeploy --jar=" + jarFile.getName())
            .execute(gfshRule).getOutputText()).contains(jarFile.getName())
                .contains("Un-Deployed From JAR Location");

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).doesNotContain(jarFile.getName());
  }

  @Test
  public void testUndeployWithNothingDeployed() {
    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(),
            "undeploy --jar=" + jarFile.getName())
            .execute(gfshRule).getOutputText()).contains(jarFile.getName() + " not deployed");
  }

  @Test
  public void testRedeployNewJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(),
            "undeploy --jar=" + jarFile.getName())
            .execute(gfshRule).getOutputText()).contains(jarFile.getName())
                .contains("Un-Deployed From JAR Location");

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).doesNotContain(jarFile.getName());

    GfshScript
        .of(getLocatorGFSHConnectionString(),
            "deploy --jar=" + anotherJarFile.getCanonicalPath())
        .execute(gfshRule);
    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).contains(anotherJarFile.getName());
  }

  @Test
  public void testUpdateJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + jarFile.getCanonicalPath()).execute(gfshRule);

    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + jarFileV2.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "list deployed").execute(gfshRule).getOutputText()).contains(jarFileV2.getName())
            .doesNotContain(jarFile.getName());
  }

  @Test
  public void testDeployMultipleJars() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + jarFile.getCanonicalPath(),
        "deploy --jar=" + anotherJarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "list deployed").execute(gfshRule).getOutputText()).contains(jarFile.getName())
            .contains(anotherJarFile.getName());
  }

  @Test
  public void testDeployFunction() throws IOException {
    JarBuilder jarBuilder = new JarBuilder();
    File source = loadTestResource("/example/test/function/ExampleFunction.java");

    File outputJar = new File(stagingTempDir.newFolder(), "function.jar");
    jarBuilder.buildJar(outputJar, source);

    GfshScript.of(getLocatorGFSHConnectionString(), "deploy --jars=" + outputJar.getCanonicalPath())
        .execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list functions").execute(gfshRule)
        .getOutputText()).contains("ExampleFunction");

    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=ExampleFunction")
            .execute(gfshRule)
            .getOutputText()).contains("SUCCESS");
  }

  @Test
  public void testDeployAndUndeployFunction() throws IOException {
    JarBuilder jarBuilder = new JarBuilder();
    File source = loadTestResource("/example/test/function/ExampleFunction.java");

    File outputJar = new File(stagingTempDir.newFolder(), "function.jar");
    jarBuilder.buildJar(outputJar, source);

    GfshScript.of(getLocatorGFSHConnectionString(), "deploy --jars=" + outputJar.getCanonicalPath())
        .execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list functions").execute(gfshRule)
        .getOutputText()).contains("ExampleFunction");

    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=ExampleFunction")
            .execute(gfshRule)
            .getOutputText()).contains("SUCCESS");

    GfshScript
        .of(getLocatorGFSHConnectionString(), "undeploy --jars=" + outputJar.getName())
        .execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list functions").execute(gfshRule)
        .getOutputText()).doesNotContain("ExampleFunction");


  }

  @Test
  public void testDeployPojo() throws IOException {
    JarBuilder jarBuilder = new JarBuilder();
    File functionSource = loadTestResource("/example/test/function/PojoFunction.java");
    File pojoSource = loadTestResource("/example/test/pojo/ExamplePojo.java");

    File outputJar = new File(stagingTempDir.newFolder(), "functionAndPojo.jar");
    jarBuilder.buildJar(outputJar, pojoSource, functionSource);

    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(),
            "create disk-store --name=ExampleDiskStore --dir="
                + stagingTempDir.newFolder().getCanonicalPath())
        .execute(gfshRule).getOutputText());

    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(),
            "create region --name=/ExampleRegion --type=REPLICATE_PERSISTENT --disk-store=ExampleDiskStore")
        .execute(gfshRule).getOutputText());

    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jars=" + outputJar.getAbsolutePath())
        .execute(gfshRule));

    System.out.println(
        GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=PojoFunction")
            .execute(gfshRule).getOutputText());

    assertThat(GfshScript
        .of(getLocatorGFSHConnectionString(), "query --query='SELECT * FROM /ExampleRegion'")
        .execute(gfshRule).getOutputText()).contains("John");

    GfshScript.of(getLocatorGFSHConnectionString(), "stop server --name=server").execute(gfshRule);

    GfshScript.of(getLocatorGFSHConnectionString(),
        "start server --name=server --locators=localhost[10334]  --server-port=40404 --http-service-port=9090 --start-rest-api")
        .execute(gfshRule);

    assertThat(GfshScript
        .of(getLocatorGFSHConnectionString(), "query --query='SELECT * FROM /ExampleRegion'")
        .execute(gfshRule).getOutputText()).contains("John");
  }

  private File loadTestResource(String fileName) {
    String filePath =
        createTempFileFromResource(getClass(), fileName).getAbsolutePath();
    assertThat(filePath).isNotNull();

    return new File(filePath);
  }
}
