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

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Paths;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class DeploySemanticVersionJarDUnitTest {
  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();
  private MemberVM locator0, locator1, server2;
  private static File stagedDir;
  private static File semanticJarVersion1, semanticJarVersion2, semanticJarVersion3,
      semanticJarVersion1b, semanticJarVersion1c;

  @BeforeClass
  public static void beforeClass() throws Exception {
    stagedDir = stagingTempDir.getRoot();
    JarBuilder jarBuilder = new JarBuilder();
    semanticJarVersion1 = new File(stagedDir, "def-1.0.jar");
    jarBuilder.buildJar(semanticJarVersion1, createClassContent("version1", "Def"));
    semanticJarVersion2 = new File(stagedDir, "def-1.1.jar");
    jarBuilder.buildJar(semanticJarVersion2, createClassContent("version2", "Def"));
    semanticJarVersion3 = new File(stagedDir, "def-1.2.jar");
    jarBuilder.buildJar(semanticJarVersion3, createClassContent("version3", "Def"));

    semanticJarVersion1b = new File(stagingTempDir.newFolder("v1b"), "def-1.0.jar");
    jarBuilder.buildJar(semanticJarVersion1b, createClassContent("version1b", "Def"));
    semanticJarVersion1c = new File(stagingTempDir.newFolder("v1c"), "def.jar");
    jarBuilder.buildJar(semanticJarVersion1c, createClassContent("version1c", "Def"));
  }

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    locator0 = cluster.startLocatorVM(0);
    locator1 = cluster.startLocatorVM(1, locator0.getPort());
    server2 = cluster.startServerVM(2, locator0.getPort(), locator1.getPort());
    gfsh.connectAndVerify(locator0);
  }

  private static String createClassContent(String version, String functionName) {
    return "package jddunit.function;" + "import org.apache.geode.cache.execute.Function;"
        + "import org.apache.geode.cache.execute.FunctionContext;" + "public class "
        + functionName + " implements Function {" + "public boolean hasResult() {return true;}"
        + "public String getId() {return \"" + functionName + "\";}"
        + "public String getVersion() {return \"" + version + "\";}"
        + "public void execute(FunctionContext context) {context.getResultSender().lastResult(\""
        + version + "\");}}";
  }

  @Test
  public void deploy() {
    gfsh.executeAndAssertThat("deploy --jar=" + semanticJarVersion1.getAbsolutePath())
        .statusIsSuccess();
    MemberVM.invokeInEveryMember(() -> {
      assertThat(Paths.get(".").resolve("cluster_config").resolve("cluster").toFile().list())
          .containsExactly("def-1.0.jar");
      Set<String> deployedJars = getDeployedJarsFromClusterConfig();
      assertThat(deployedJars).containsExactly("def-1.0.jar");
    }, locator0, locator1);

    assertThat(server2.getWorkingDir().list()).containsExactly(semanticJarVersion1.getName());
    server2.invoke(() -> verifyLoadAndHasVersion("def", "jddunit.function.Def", "version1"));

    gfsh.executeAndAssertThat("deploy --jar=" + semanticJarVersion2.getAbsolutePath())
        .statusIsSuccess();
    MemberVM.invokeInEveryMember(() -> {
      assertThat(Paths.get(".").resolve("cluster_config").resolve("cluster").toFile().list())
          .containsExactly("def-1.1.jar");
      Set<String> deployedJars = getDeployedJarsFromClusterConfig();
      assertThat(deployedJars).containsExactly("def-1.1.jar");
    }, locator0, locator1);

    assertThat(server2.getWorkingDir().list())
        .containsExactlyInAnyOrder(semanticJarVersion1.getName(), semanticJarVersion2.getName());
    server2.invoke(() -> verifyLoadAndHasVersion("def", "jddunit.function.Def", "version2"));

    MemberVM server3 = cluster.startServerVM(3, locator0.getPort(), locator1.getPort());
    assertThat(server3.getWorkingDir().list())
        .containsExactlyInAnyOrder(semanticJarVersion2.getName());
    server3.invoke(() -> verifyLoadAndHasVersion("def", "jddunit.function.Def", "version2"));

    server3.stop(false);
    gfsh.executeAndAssertThat("deploy --jar=" + semanticJarVersion3.getAbsolutePath())
        .statusIsSuccess();
    MemberVM.invokeInEveryMember(() -> {
      assertThat(Paths.get(".").resolve("cluster_config").resolve("cluster").toFile().list())
          .containsExactly("def-1.2.jar");
      Set<String> deployedJars = getDeployedJarsFromClusterConfig();
      assertThat(deployedJars).containsExactly("def-1.2.jar");
    }, locator0, locator1);
    assertThat(server2.getWorkingDir().list()).containsExactlyInAnyOrder(
        semanticJarVersion1.getName(), semanticJarVersion2.getName(),
        semanticJarVersion3.getName());
    server2.invoke(() -> verifyLoadAndHasVersion("def", "jddunit.function.Def", "version3"));

    // restart server3
    server3 = cluster.startServerVM(3, locator0.getPort(), locator1.getPort());
    assertThat(server3.getWorkingDir().list()).containsExactly(semanticJarVersion3.getName());
    server3.invoke(() -> verifyLoadAndHasVersion("def", "jddunit.function.Def", "version3"));
  }

  static Set<String> getDeployedJarsFromClusterConfig() {
    InternalConfigurationPersistenceService cps =
        ClusterStartupRule.getLocator().getConfigurationPersistenceService();
    return cps.getConfiguration("cluster").getJarNames();
  }

  private static void verifyLoadAndHasVersion(String artifactId, String className, String version)
      throws Exception {
    assertThat(ClassPathLoader.getLatest().getJarDeployer()
        .getDeployedJar(artifactId)).isNotNull();
    Class<?> klass = ClassPathLoader.getLatest().forName(className);
    assertThat(klass).isNotNull();
    assertThat(klass.getMethod("getVersion").invoke(klass.newInstance())).isEqualTo(version);
  }
}
