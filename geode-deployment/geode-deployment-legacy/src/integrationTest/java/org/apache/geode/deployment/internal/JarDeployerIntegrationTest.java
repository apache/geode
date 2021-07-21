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

package org.apache.geode.deployment.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.time.Instant;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.JarBuilder;


public class JarDeployerIntegrationTest {

  @ClassRule
  public static TemporaryFolder stagedTempDir = new TemporaryFolder();

  @Rule
  public TemporaryFolder deployTempDir = new TemporaryFolder();

  private JarDeployer jarDeployer;

  private static File stagedDir;
  private static File plainJarVersion1, plainJarVersion1b, plainJarVersion2, semanticJarVersion1,
      semanticJarVersion2, semanticJarVersion1b, semanticJarVersion1c;

  private File deployedDir;

  @BeforeClass
  public static void createStagedJars() throws IOException {
    stagedDir = stagedTempDir.getRoot();
    JarBuilder jarBuilder = new JarBuilder();
    plainJarVersion1 = new File(stagedTempDir.newFolder("v1"), "abc.jar");
    jarBuilder.buildJar(plainJarVersion1, createClassContent("version1", "Abc"));
    plainJarVersion2 = new File(stagedTempDir.newFolder("v2"), "abc.jar");
    jarBuilder.buildJar(plainJarVersion2, createClassContent("version2", "Abc"));
    plainJarVersion1b = new File(stagedDir, "abc-1.0.jar");
    jarBuilder.buildJar(plainJarVersion1b, createClassContent("version1b", "Abc"));

    semanticJarVersion1 = new File(stagedDir, "def-1.0.jar");
    jarBuilder.buildJar(semanticJarVersion1, createClassContent("version1", "Def"));
    semanticJarVersion2 = new File(stagedDir, "def-1.1.jar");
    jarBuilder.buildJar(semanticJarVersion2, createClassContent("version2", "Def"));

    semanticJarVersion1b = new File(stagedTempDir.newFolder("v1b"), "def-1.0.jar");
    jarBuilder.buildJar(semanticJarVersion1b, createClassContent("version1b", "Def"));
    semanticJarVersion1c = new File(stagedTempDir.newFolder("v1c"), "def.jar");
    jarBuilder.buildJar(semanticJarVersion1c, createClassContent("version1c", "Def"));
  }

  @Before
  public void before() throws IOException {
    deployedDir = deployTempDir.getRoot();
    jarDeployer = new JarDeployer(deployedDir);
  }

  @Test
  public void deployABC() throws Exception {
    // deploy first version of abc.jar
    DeployedJar deployedJar = jarDeployer.deploy(plainJarVersion1);
    assertThat(deployedJar).isNotNull();
    assertThat(deployedJar.getFile()).hasName("abc.v1.jar");
    assertThat(deployedJar.getArtifactId()).isEqualTo("abc");
    assertThat(deployedDir.list()).containsExactly("abc.v1.jar");
    assertThat(jarDeployer.getDeployedJars()).containsOnlyKeys("abc");
    assertThat(jarDeployer.getDeployedJars().get("abc")).isEqualTo(deployedJar);
    assertThat(getVersion("jddunit.function.Abc")).isEqualTo("version1");

    // deploy 2nd version of abc.jar
    deployedJar = jarDeployer.deploy(plainJarVersion2);
    assertThat(deployedJar).isNotNull();
    assertThat(deployedJar.getFile()).hasName("abc.v2.jar");
    assertThat(deployedJar.getArtifactId()).isEqualTo("abc");
    assertThat(deployedDir.list()).containsExactlyInAnyOrder("abc.v1.jar", "abc.v2.jar");
    assertThat(jarDeployer.getDeployedJars()).containsOnlyKeys("abc");
    assertThat(jarDeployer.getDeployedJars().get("abc")).isEqualTo(deployedJar);
    assertThat(getVersion("jddunit.function.Abc")).isEqualTo("version2");
  }

  @Test
  public void deployABC_mixed() throws Exception {
    // deploy abc.jar
    jarDeployer.deploy(plainJarVersion1);

    // deploy abc-1.0.jar
    DeployedJar deployedJar = jarDeployer
        .deploy(plainJarVersion1b);
    assertThat(deployedJar).isNotNull();
    assertThat(deployedJar.getFile()).hasName("abc-1.0.v2.jar");
    assertThat(deployedJar.getArtifactId()).isEqualTo("abc");
    assertThat(deployedDir.list()).containsExactlyInAnyOrder("abc.v1.jar", "abc-1.0.v2.jar");
    assertThat(jarDeployer.getDeployedJars()).containsOnlyKeys("abc");
    assertThat(jarDeployer.getDeployedJars().get("abc")).isEqualTo(deployedJar);
    assertThat(getVersion("jddunit.function.Abc")).isEqualTo("version1b");
  }

  @Test
  public void deployDEF() throws Exception {
    // deploy first version of def.jar
    DeployedJar deployedJar = jarDeployer.deploy(semanticJarVersion1);
    assertThat(deployedJar).isNotNull();
    assertThat(deployedJar.getFile()).hasName("def-1.0.v1.jar");
    assertThat(deployedJar.getArtifactId()).isEqualTo("def");
    assertThat(deployedDir.list()).containsExactly("def-1.0.v1.jar");
    assertThat(jarDeployer.getDeployedJars()).containsOnlyKeys("def");
    assertThat(jarDeployer.getDeployedJars().get("def")).isEqualTo(deployedJar);
    assertThat(getVersion("jddunit.function.Def")).isEqualTo("version1");

    // deploy second version of def.jar
    deployedJar = jarDeployer.deploy(semanticJarVersion2);
    assertThat(deployedJar).isNotNull();
    assertThat(deployedJar.getFile()).hasName("def-1.1.v2.jar");
    assertThat(deployedJar.getArtifactId()).isEqualTo("def");
    assertThat(deployedDir.list()).containsExactlyInAnyOrder("def-1.0.v1.jar", "def-1.1.v2.jar");
    assertThat(jarDeployer.getDeployedJars()).containsOnlyKeys("def");
    assertThat(jarDeployer.getDeployedJars().get("def")).isEqualTo(deployedJar);
    assertThat(getVersion("jddunit.function.Def")).isEqualTo("version2");
  }

  @Test
  public void deployDEF_mixed() throws Exception {
    // deploy first version of def-1.0.jar
    jarDeployer.deploy(semanticJarVersion1);

    // deploy second version of def-1.0.jar with a different content
    DeployedJar deployedJar =
        jarDeployer.deploy(semanticJarVersion1b);
    assertThat(deployedJar).isNotNull();
    assertThat(deployedJar.getFile()).hasName("def-1.0.v2.jar");
    assertThat(deployedJar.getArtifactId()).isEqualTo("def");
    assertThat(deployedDir.list()).containsExactlyInAnyOrder("def-1.0.v1.jar", "def-1.0.v2.jar");
    assertThat(getVersion("jddunit.function.Def")).isEqualTo("version1b");

    // deploy def.jar

    deployedJar = jarDeployer.deploy(semanticJarVersion1c);
    assertThat(deployedJar).isNotNull();
    assertThat(deployedJar.getFile()).hasName("def.v3.jar");
    assertThat(deployedJar.getArtifactId()).isEqualTo("def");
    assertThat(deployedDir.list()).containsExactlyInAnyOrder("def-1.0.v1.jar", "def-1.0.v2.jar",
        "def.v3.jar");
    assertThat(jarDeployer.getDeployedJars()).containsOnlyKeys("def");
    assertThat(jarDeployer.getDeployedJars().get("def")).isEqualTo(deployedJar);
    assertThat(getVersion("jddunit.function.Def")).isEqualTo("version1c");
  }

  @Test
  public void undeploy() throws Exception {
    // deploy abc.jar
    jarDeployer.deploy(plainJarVersion1);
    // deploy def.jar
    jarDeployer.deploy(semanticJarVersion1c);
    // deploy def-1.0.jar
    jarDeployer.deploy(semanticJarVersion1);

    Deployment deployment = new Deployment("def-1.0.jar", "test", Instant.now().toString());
    jarDeployer.undeploy(deployment.getFileName());

    // do not verify this on window's machine since it can not remove a file that a process has
    // open
    if (!SystemUtils.isWindows()) {
      assertThat(deployedDir.list()).containsExactly("abc.v1.jar");
    }
    assertThatThrownBy(() -> ClassPathLoader.getLatest().forName("jddunit.function.Def"))
        .isInstanceOf(ClassNotFoundException.class);
  }

  private String getVersion(String classname) throws Exception {
    Class<?> def = ClassPathLoader.getLatest().forName(classname);
    assertThat(def).isNotNull();
    return (String) def.getMethod("getId").invoke(def.newInstance());
  }

  private static String createClassContent(String version, String functionName) {
    return "package jddunit.function;" + "import org.apache.geode.cache.execute.Function;"
        + "import org.apache.geode.cache.execute.FunctionContext;" + "public class "
        + functionName + " implements Function {" + "public boolean hasResult() {return true;}"
        + "public String getId() {return \"" + version + "\";}"
        + "public void execute(FunctionContext context) {context.getResultSender().lastResult(\""
        + version + "\");}}";
  }
}
