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

import static java.nio.file.Files.createDirectories;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class DeployJarAcceptanceTest {

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  private Path jarFile;
  private Path jarFileV2;
  private Path anotherJarFile;
  private Path stagingTempDir;

  @Before
  public void setUp()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    Path rootFolder = folderRule.getFolder().toPath().toAbsolutePath();
    stagingTempDir = rootFolder;

    Path stagingDir = createDirectories(rootFolder.resolve("staging"));
    jarFile = stagingDir.resolve("myJar-1.0.jar");
    jarFileV2 = stagingDir.resolve("myJar-2.0.jar");
    anotherJarFile = stagingDir.resolve("anotherJar-1.0.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(jarFile.toFile(), "SomeClass");
    jarBuilder.buildJarFromClassNames(jarFileV2.toFile(), "SomeClass", "SomeClassVersionTwo");
    jarBuilder.buildJarFromClassNames(anotherJarFile.toFile(), "SomeOtherClass");

    GfshScript
        .of("start locator --name=locator",
            "configure pdx --read-serialized=true",
            "start server --name=server --locators=localhost[10334]")
        .execute(gfshRule);
  }

  @After
  public void tearDown()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    System.out.println(GfshScript
        .of(connectCommand(),
            "undeploy")
        .execute(gfshRule)
        .getOutputText());
  }

  @Test
  public void testDeployJar() {
    GfshScript
        .of(connectCommand(),
            "deploy --jar=" + jarFile)
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "list deployed")
        .execute(gfshRule).getOutputText())
            .contains(jarFile.toFile().getName())
            .contains("JAR Location");
  }

  @Test
  public void testDeployExistingJar() {
    GfshScript
        .of(connectCommand(),
            "deploy --jar=" + jarFile)
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "list deployed")
        .execute(gfshRule)
        .getOutputText())
            .contains(jarFile.toFile().getName())
            .contains("JAR Location");

    assertThat(GfshScript
        .of(connectCommand(),
            "deploy --jar=" + jarFile)
        .execute(gfshRule)
        .getOutputText())
            .contains("Already deployed");

    assertThat(GfshScript
        .of(connectCommand(),
            "list deployed")
        .execute(gfshRule)
        .getOutputText())
            .contains(jarFile.toFile().getName())
            .contains("JAR Location");
  }

  @Test
  public void testUndeployJar() {
    GfshScript
        .of(connectCommand(),
            "deploy --jar=" + jarFile)
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "undeploy --jar=" + jarFile.toFile().getName())
        .execute(gfshRule)
        .getOutputText())
            .contains(jarFile.toFile().getName())
            .contains("Un-Deployed From JAR Location");

    assertThat(GfshScript
        .of(connectCommand(),
            "list deployed")
        .execute(gfshRule)
        .getOutputText())
            .doesNotContain(jarFile.toFile().getName());
  }

  @Test
  public void testUndeployWithNothingDeployed() {
    assertThat(GfshScript
        .of(connectCommand(),
            "undeploy --jar=" + jarFile.toFile().getName())
        .execute(gfshRule).getOutputText())
            .contains(jarFile.toFile().getName() + " not deployed");
  }

  @Test
  public void testRedeployNewJar() {
    GfshScript
        .of(connectCommand(),
            "deploy --jar=" + jarFile)
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "undeploy --jar=" + jarFile.toFile().getName())
        .execute(gfshRule)
        .getOutputText())
            .contains(jarFile.toFile().getName())
            .contains("Un-Deployed From JAR Location");

    assertThat(GfshScript
        .of(connectCommand(),
            "list deployed")
        .execute(gfshRule)
        .getOutputText())
            .doesNotContain(jarFile.toFile().getName());

    GfshScript
        .of(connectCommand(),
            "deploy --jar=" + anotherJarFile)
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "list deployed")
        .execute(gfshRule)
        .getOutputText())
            .contains(anotherJarFile.toFile().getName());
  }

  @Test
  public void testUpdateJar() {
    GfshScript
        .of(connectCommand(),
            "deploy --jar=" + jarFile)
        .execute(gfshRule);

    GfshScript
        .of(connectCommand(),
            "deploy --jar=" + jarFileV2)
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "list deployed")
        .execute(gfshRule)
        .getOutputText())
            .contains(jarFileV2.toFile().getName())
            .doesNotContain(jarFile.toFile().getName());
  }

  @Test
  public void testDeployMultipleJars() {
    GfshScript
        .of(connectCommand(),
            "deploy --jar=" + jarFile,
            "deploy --jar=" + anotherJarFile)
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "list deployed")
        .execute(gfshRule)
        .getOutputText())
            .contains(jarFile.toFile().getName())
            .contains(anotherJarFile.toFile().getName());
  }

  @Test
  public void testDeployFunction() throws IOException {
    Path source = loadTestResource("/example/test/function/ExampleFunction.java");
    Path outputJar = stagingTempDir.resolve("function.jar").toAbsolutePath();

    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJar(outputJar.toFile(), source.toFile());

    GfshScript
        .of(connectCommand(),
            "deploy --jars=" + outputJar)
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "list functions")
        .execute(gfshRule)
        .getOutputText())
            .contains("ExampleFunction");

    assertThat(GfshScript
        .of(connectCommand(),
            "execute function --id=ExampleFunction")
        .execute(gfshRule)
        .getOutputText())
            .contains("SUCCESS");
  }

  @Test
  public void testDeployAndUndeployFunction() throws IOException {
    Path source = loadTestResource("/example/test/function/ExampleFunction.java");
    Path outputJar = stagingTempDir.resolve("function.jar");

    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJar(outputJar.toFile(), source.toFile());

    GfshScript
        .of(connectCommand(),
            "deploy --jars=" + outputJar)
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "list functions")
        .execute(gfshRule)
        .getOutputText())
            .contains("ExampleFunction");

    assertThat(GfshScript
        .of(connectCommand(),
            "execute function --id=ExampleFunction")
        .execute(gfshRule)
        .getOutputText())
            .contains("SUCCESS");

    GfshScript
        .of(connectCommand(),
            "undeploy --jars=" + outputJar.toFile().getName())
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "list functions")
        .execute(gfshRule)
        .getOutputText())
            .doesNotContain("ExampleFunction");
  }

  @Test
  public void testDeployPojo() throws IOException {
    Path functionSource = loadTestResource("/example/test/function/PojoFunction.java");
    Path pojoSource = loadTestResource("/example/test/pojo/ExamplePojo.java");
    Path outputJar = stagingTempDir.resolve("functionAndPojo.jar");

    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJar(outputJar.toFile(), pojoSource.toFile(), functionSource.toFile());

    Path folderForExampleDiskStore =
        createDirectories(stagingTempDir.resolve("folderForExampleDiskStore")).toAbsolutePath();

    System.out.println(GfshScript
        .of(connectCommand(),
            "create disk-store --name=ExampleDiskStore --dir=" + folderForExampleDiskStore)
        .execute(gfshRule)
        .getOutputText());

    System.out.println(GfshScript
        .of(connectCommand(),
            "create region --name=/ExampleRegion --type=REPLICATE_PERSISTENT --disk-store=ExampleDiskStore")
        .execute(gfshRule).getOutputText());

    System.out.println(GfshScript
        .of(connectCommand(),
            "deploy --jars=" + outputJar)
        .execute(gfshRule));

    System.out.println(GfshScript
        .of(connectCommand(),
            "execute function --id=PojoFunction")
        .execute(gfshRule)
        .getOutputText());

    assertThat(GfshScript
        .of(connectCommand(),
            "query --query='SELECT * FROM /ExampleRegion'")
        .execute(gfshRule)
        .getOutputText())
            .contains("John");

    // 1: issue stop command
    GfshExecution execution = GfshScript
        .of(connectCommand(),
            "stop server --name=server")
        .execute(gfshRule);

    // 2: await termination of process
    execution.serverStopper().awaitStop("server");

    GfshScript
        .of(connectCommand(),
            "start server --name=server --locators=localhost[10334] --server-port=40404 --http-service-port=9090 --start-rest-api")
        .execute(gfshRule);

    assertThat(GfshScript
        .of(connectCommand(),
            "query --query='SELECT * FROM /ExampleRegion'")
        .execute(gfshRule)
        .getOutputText())
            .contains("John");
  }

  private String connectCommand() {
    return "connect --locator=localhost[10334]";
  }

  private Path loadTestResource(String fileName) {
    return createTempFileFromResource(getClass(), fileName).toPath().toAbsolutePath();
  }
}
