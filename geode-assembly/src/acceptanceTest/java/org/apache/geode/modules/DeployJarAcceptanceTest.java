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
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class DeployJarAcceptanceTest extends AbstractDockerizedAcceptanceTest {
  private static final JarBuilder jarBuilder = new JarBuilder();
  private static File myJarV1;
  private static File myJarV2;
  private static File anotherJarFile;
  private static File functionJar;
  private static File pojoFunctionJar;
  private static File pojoJar;

  public DeployJarAcceptanceTest(String launchCommand) throws IOException, InterruptedException {
    launch(launchCommand);
  }

  @BeforeClass
  public static void setup() throws IOException {
    File stagingDir = stagingTempDir.newFolder("staging");
    myJarV1 = new File(stagingDir, "myJar-1.0.jar");
    myJarV2 = new File(stagingDir, "myJar-2.0.jar");
    anotherJarFile = new File(stagingDir, "anotherJar-1.0.jar");
    functionJar = new File(stagingDir, "function.jar");
    File functionSource = loadTestResource("/example/test/function/ExampleFunction.java");
    File pojoFunctionSource = loadTestResource("/example/test/function/PojoFunction.java");
    File pojoSource = loadTestResource("/version1/example/test/pojo/ExamplePojo.java");
    pojoJar = new File(stagingTempDir.newFolder(), "pojo.jar");
    pojoFunctionJar = new File(stagingTempDir.newFolder(), "pojo-function.jar");
    jarBuilder.buildJar(pojoJar, "example.test.pojo.ExamplePojo", pojoSource);
    jarBuilder.buildJar(pojoFunctionJar, "example.test.function.PojoFunction", pojoFunctionSource);
    jarBuilder.buildJar(functionJar, functionSource);
    jarBuilder.buildJarFromClassNames(myJarV1, "SomeClass");
    jarBuilder.buildJarFromClassNames(myJarV2, "SomeClass", "SomeClassVersionTwo");
    jarBuilder.buildJarFromClassNames(anotherJarFile, "SomeOtherClass");


  }

  // @Override
  // public String getLocatorGFSHConnectionString() {
  // return "connect";
  // }

  @After
  public void teardown() {
    System.out.println(GfshScript.of(getLocatorGFSHConnectionString(), "undeploy")
        .execute(gfshRule).getOutputText());
  }

  @Test
  public void testDeployJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + myJarV1.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).contains(myJarV1.getName()).contains("JAR Location");
  }

  @Test
  public void testDeployExistingJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + myJarV1.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).contains(myJarV1.getName()).contains("JAR Location");

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + myJarV1.getCanonicalPath()).execute(gfshRule).getOutputText())
            .contains("Already deployed");

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).contains(myJarV1.getName()).contains("JAR Location");
  }

  @Test
  public void testUndeployJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + myJarV1.getCanonicalPath()).execute(gfshRule);

    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(),
            "undeploy --jar=" + myJarV1.getName())
            .execute(gfshRule).getOutputText()).contains(myJarV1.getName())
                .contains("Un-Deployed From JAR Location");

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).doesNotContain(myJarV1.getName());
  }

  @Test
  public void testUndeployWithNothingDeployed() {
    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(),
            "undeploy --jar=" + myJarV1.getName())
            .execute(gfshRule).getOutputText()).contains(myJarV1.getName() + " not deployed");
  }

  @Test
  public void testRedeployNewJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + myJarV1.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "undeploy --jar=" + myJarV1.getName())
        .execute(gfshRule).getOutputText()).contains(myJarV1.getName())
            .contains("Un-Deployed From JAR Location");

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).doesNotContain(myJarV1.getName());

    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + anotherJarFile.getCanonicalPath())
        .execute(gfshRule);
    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed")
        .execute(gfshRule).getOutputText()).contains(anotherJarFile.getName());
  }

  @Test
  public void testUpdateJar() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + myJarV1.getCanonicalPath()).execute(gfshRule);

    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + myJarV2.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "list deployed").execute(gfshRule).getOutputText()).contains(myJarV2.getName())
            .doesNotContain(myJarV1.getName());
  }

  @Test
  public void testDeployMultipleJars() throws IOException {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + myJarV1.getCanonicalPath(),
        "deploy --jar=" + anotherJarFile.getCanonicalPath()).execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "list deployed").execute(gfshRule).getOutputText()).contains(myJarV1.getName())
            .contains(anotherJarFile.getName());
  }

  @Test
  public void testDeployFunction() throws IOException {
    GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jars=" + functionJar.getCanonicalPath())
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

    GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jars=" + functionJar.getCanonicalPath())
        .execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list functions").execute(gfshRule)
        .getOutputText()).contains("ExampleFunction");

    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=ExampleFunction")
            .execute(gfshRule)
            .getOutputText()).contains("SUCCESS");

    GfshScript.of(getLocatorGFSHConnectionString(), "undeploy --jars=" + functionJar.getName())
        .execute(gfshRule);

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list functions").execute(gfshRule)
        .getOutputText()).doesNotContain("ExampleFunction");
  }

  @Test
  public void testDeployPojo() throws IOException, InterruptedException {
    File functionSource = loadTestResource("/example/test/function/PojoFunction.java");
    File pojoSource = loadTestResource("/version1/example/test/pojo/ExamplePojo.java");

    File outputJar = new File(stagingTempDir.newFolder(), "functionAndPojo.jar");
    jarBuilder.buildJar(outputJar, pojoSource, functionSource);

    System.out.println(GfshScript.of(getLocatorGFSHConnectionString(),
        "create disk-store --name=ExampleDiskStore --dir=store")
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

    shutdownServersAndWait();

    Thread thread = new Thread(() -> {
      try {
        runGfshCommandInContainer("connect", getServer1StartCommand() + getCurrentLaunchCommand());
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
    });
    thread.setDaemon(true);
    thread.start();

    runGfshCommandInContainer("connect", getServer2StartCommand() + getCurrentLaunchCommand());

    GeodeAwaitility.await().pollDelay(5, TimeUnit.SECONDS).pollInterval(5, TimeUnit.SECONDS)
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> GfshScript.of(getLocatorGFSHConnectionString(), "list regions")
            .execute(gfshRule).getOutputText().contains("ExampleRegion"));

    assertThat(GfshScript
        .of(getLocatorGFSHConnectionString(), "query --query='SELECT * FROM /ExampleRegion'")
        .execute(gfshRule).getOutputText()).contains("John");

    GfshScript.of(getLocatorGFSHConnectionString(), "destroy region --name=/ExampleRegion")
        .execute(gfshRule);
  }

  private void shutdownServersAndWait() {
    System.out.println(
        GfshScript.of(getLocatorGFSHConnectionString(), "shutdown")
            .execute(gfshRule).getOutputText());

    GeodeAwaitility.await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
        .until(() -> !GfshScript.of(getLocatorGFSHConnectionString(), "list members")
            .execute(gfshRule).getOutputText().contains("server"));
  }

  @Test
  public void testClassesNotAccessibleAfterUndeploy() {
    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(),
            "create region --name=/ExampleRegion --type=PARTITION")
        .execute(gfshRule).getOutputText());

    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jars=" + pojoJar.getAbsolutePath())
        .execute(gfshRule));

    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jars=" + pojoFunctionJar.getAbsolutePath())
        .execute(gfshRule));

    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=PojoFunction")
            .execute(gfshRule).getOutputText()).contains("SUCCESS");

    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(), "undeploy --jars=" + pojoJar.getName())
        .execute(gfshRule));

    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(), "undeploy --jars=" + pojoFunctionJar.getName())
        .execute(gfshRule));

    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jars=" + pojoFunctionJar.getAbsolutePath())
        .execute(gfshRule));

    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=PojoFunction")
        .expectExitCode(1)
        .execute(gfshRule).getOutputText())
            .contains("java.lang.NoClassDefFoundError: example/test/pojo/ExamplePojo");

    GfshScript.of(getLocatorGFSHConnectionString(), "destroy region --name=/ExampleRegion")
        .execute(gfshRule);
  }

  @Test
  public void testSpringVersionsDoNotConflict() {
    String jarPath = System.getenv("DEPLOY_TEST_SPRING_JAR");

    assertThat(jarPath).isNotNull();

    System.out.println(GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jar=" + jarPath)
        .execute(gfshRule).getOutputText());

    if (isModular()) {
      assertThat(GfshScript
          .of(getLocatorGFSHConnectionString(), "execute function --id=" + "SpringFunction")
          .execute(gfshRule).getOutputText()).contains("Salutations, Earth");
    }
  }

  @Test
  public void testDeployFailsWhenFunctionIsInExcludedPath() throws IOException {
    File excludedFunctionSource = loadTestResource("/org/apache/geode/ExcludedFunction.java");
    File includedFunctionSource = loadTestResource("/example/test/function/ExampleFunction.java");
    File functionJar = new File(stagingTempDir.newFolder(), "function.jar");
    jarBuilder.buildJar(functionJar, excludedFunctionSource, includedFunctionSource);

    GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jars=" + functionJar.getAbsolutePath())
        .execute(gfshRule);

    if (isModular()) {
      assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list deployed").execute(gfshRule)
          .getOutputText()).doesNotContain("function");

      assertThat(GfshScript
          .of(getLocatorGFSHConnectionString(), "list functions")
          .execute(gfshRule).getOutputText())
              .doesNotContain("ExcludedFunction")
              .doesNotContain("ExampleFunction");
    }
  }

  @Test
  public void testUpdateFunctionVersion() throws IOException {
    File functionVersion1Source =
        loadTestResource("/version1/example/test/function/VersionedFunction.java");
    File functionVersion2Source =
        loadTestResource("/version2/example/test/function/VersionedFunction.java");

    File functionVersion1Jar = new File(stagingTempDir.newFolder(), "function-1.0.0.jar");
    File functionVersion2Jar = new File(stagingTempDir.newFolder(), "function-2.0.0.jar");

    jarBuilder.buildJar(functionVersion1Jar, functionVersion1Source);
    jarBuilder.buildJar(functionVersion2Jar, functionVersion2Source);

    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + functionVersion1Jar.getAbsolutePath()).execute(gfshRule);
    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=VersionedFunction")
            .execute(gfshRule).getOutputText()).contains("Version1");

    GfshScript.of(getLocatorGFSHConnectionString(),
        "deploy --jar=" + functionVersion2Jar.getAbsolutePath()).execute(gfshRule);
    assertThat(
        GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=VersionedFunction")
            .execute(gfshRule).getOutputText()).contains("Version2");
  }

  @Test
  public void testUpdatePojoVersion() throws IOException {
    File pojoVersion1Source = loadTestResource("/version1/example/test/pojo/ExamplePojo.java");
    File pojoVersion2Source = loadTestResource("/version2/example/test/pojo/ExamplePojo.java");

    File pojoVersion1Jar = new File(stagingTempDir.newFolder(), "pojo-1.0.0.jar");
    File pojoVersion2Jar = new File(stagingTempDir.newFolder(), "pojo-2.0.0.jar");

    jarBuilder.buildJar(pojoVersion1Jar, pojoVersion1Source);
    jarBuilder.buildJar(pojoVersion2Jar, pojoVersion2Source);

    GfshScript.of(getLocatorGFSHConnectionString(),
        "create region --name=/ExampleRegion --type=PARTITION")
        .execute(gfshRule);

    GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jar=" + pojoVersion1Jar.getAbsolutePath())
        .execute(gfshRule);
    GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jar=" + pojoFunctionJar.getAbsolutePath())
        .execute(gfshRule);
    GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=PojoFunction")
        .execute(gfshRule);
    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "query --query=\"SELECT version FROM /ExampleRegion\"").execute(gfshRule).getOutputText())
            .contains("Version1");

    GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jar=" + pojoVersion2Jar.getAbsolutePath())
        .execute(gfshRule);

    GfshScript.of(getLocatorGFSHConnectionString(), "execute function --id=PojoFunction")
        .execute(gfshRule);
    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "query --query=\"SELECT version FROM /ExampleRegion\"").execute(gfshRule).getOutputText())
            .contains("Version2");

    GfshScript.of(getLocatorGFSHConnectionString(), "destroy region --name=/ExampleRegion")
        .execute(gfshRule);
  }

  @Test
  public void testCannotDeployModulesThatStartWithGeode() throws IOException {
    File source = loadTestResource("/example/test/pojo/ExamplePojo.java");

    File geodeCoreJar = new File(stagingTempDir.newFolder(), "geode-core.jar");

    jarBuilder.buildJar(geodeCoreJar, source);

    assertThat(GfshScript
        .of(getLocatorGFSHConnectionString(), "deploy --jars=" + geodeCoreJar.getAbsolutePath())
        .expectExitCode(1)
        .execute(gfshRule).getOutputText())
            .contains("Jar names may not start with \"geode-\"");

    assertThat(GfshScript
        .of(getLocatorGFSHConnectionString(), "list deployed").execute(gfshRule).getOutputText())
            .doesNotContain("geode-core");
  }



  private static File loadTestResource(String fileName) {
    String filePath =
        createTempFileFromResource(DeployJarAcceptanceTest.class, fileName).getAbsolutePath();
    assertThat(filePath).isNotNull();

    return new File(filePath);
  }
}
