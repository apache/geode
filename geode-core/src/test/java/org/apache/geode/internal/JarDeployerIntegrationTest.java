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

package org.apache.geode.internal;


import static org.apache.geode.internal.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Category(IntegrationTest.class)
public class JarDeployerIntegrationTest {
  private ClassBuilder classBuilder;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  JarDeployer jarDeployer;

  @Before
  public void setup() {
    classBuilder = new ClassBuilder();
    jarDeployer = new JarDeployer(temporaryFolder.getRoot());
  }

  private byte[] createJarWithClass(String className) throws IOException {
    String stringBuilder = "package integration.parent;" + "public class " + className + " {}";

    return this.classBuilder.createJarFromClassContent("integration/parent/" + className,
        stringBuilder);
  }

  @Test
  public void testFileVersioning() throws Exception {
    String jarName = "JarDeployerIntegrationTest.jar";

    byte[] firstJarBytes = createJarWithClass("ClassA");

    // First deploy of the JAR file
    DeployedJar firstDeployedJar = jarDeployer.deployWithoutRegistering(jarName, firstJarBytes);

    assertThat(firstDeployedJar.getFile()).exists().hasBinaryContent(firstJarBytes);
    assertThat(firstDeployedJar.getFile().getName()).contains(".v1.").doesNotContain(".v2.");

    // Now deploy an updated JAR file and make sure that the next version of the JAR file
    // was created
    byte[] secondJarBytes = createJarWithClass("ClassB");

    DeployedJar secondDeployedJar = jarDeployer.deployWithoutRegistering(jarName, secondJarBytes);
    File secondDeployedJarFile = new File(secondDeployedJar.getFileCanonicalPath());

    assertThat(secondDeployedJarFile).exists().hasBinaryContent(secondJarBytes);
    assertThat(secondDeployedJarFile.getName()).contains(".v2.").doesNotContain(".v1.");

    File[] sortedOldJars = jarDeployer.findSortedOldVersionsOfJar(jarName);
    assertThat(sortedOldJars).hasSize(2);
    assertThat(sortedOldJars[0].getName()).contains(".v2.");
    assertThat(sortedOldJars[1].getName()).contains(".v1.");
    assertThat(jarDeployer.findDistinctDeployedJars()).hasSize(1);
  }

  @Test
  public void testDeployToInvalidDirectory() throws Exception {
    final File alternateDir = new File(temporaryFolder.getRoot(), "JarDeployerDUnit");
    alternateDir.delete();

    final JarDeployer jarDeployer = new JarDeployer(alternateDir);

    final CyclicBarrier barrier = new CyclicBarrier(2);
    final byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDTID");

    // Test to verify that deployment fails if the directory doesn't exist.
    assertThatThrownBy(() -> {
      jarDeployer.deployWithoutRegistering("JarDeployerIntegrationTest.jar", jarBytes);
    }).isInstanceOf(IOException.class);

    // Test to verify that deployment succeeds if the directory doesn't
    // initially exist, but is then created while the JarDeployer is looping
    // looking for a valid directory.
    Future<Boolean> done = Executors.newSingleThreadExecutor().submit(() -> {
      barrier.await();
      jarDeployer.deployWithoutRegistering("JarDeployerIntegrationTest.jar", jarBytes);
      return true;
    });

    barrier.await();
    Thread.sleep(500);
    alternateDir.mkdir();
    assertThat(done.get(2, TimeUnit.MINUTES)).isTrue();
  }

  @Test
  public void testVersionNumberCreation() throws Exception {
    File versionedName = jarDeployer.getNextVersionedJarFile("myJar.jar");
    assertThat(versionedName.getName()).isEqualTo("myJar.v1.jar");

    byte[] jarBytes = this.classBuilder.createJarFromName("ClassA");
    File deployedJarFile = jarDeployer.deployWithoutRegistering("myJar.jar", jarBytes).getFile();

    assertThat(deployedJarFile.getName()).isEqualTo("myJar.v1.jar");

    File secondDeployedJarFile =
        jarDeployer.deployWithoutRegistering("myJar.jar", jarBytes).getFile();

    assertThat(secondDeployedJarFile.getName()).isEqualTo("myJar.v2.jar");
  }

  @Test
  public void testVersionNumberMatcher() throws IOException {
    int version =
        jarDeployer.extractVersionFromFilename(temporaryFolder.newFile("MyJar.v1.jar").getName());

    assertThat(version).isEqualTo(1);
  }

  @Test
  public void testRenamingOfOldJarFiles() throws Exception {
    File deployDir = jarDeployer.getDeployDirectory();

    File jarAVersion1 = new File(deployDir, "vf.gf#myJarA.jar#1");
    this.classBuilder.writeJarFromName("ClassA", jarAVersion1);

    File jarAVersion2 = new File(deployDir, "vf.gf#myJarA.jar#2");
    this.classBuilder.writeJarFromName("ClassA", jarAVersion2);

    File jarBVersion2 = new File(deployDir, "vf.gf#myJarB.jar#2");
    this.classBuilder.writeJarFromName("ClassB", jarBVersion2);

    File jarBVersion3 = new File(deployDir, "vf.gf#myJarB.jar#3");
    this.classBuilder.writeJarFromName("ClassB", jarBVersion3);

    Set<File> deployedJarsBeforeRename = Stream
        .of(jarAVersion1, jarAVersion2, jarBVersion2, jarBVersion3).collect(Collectors.toSet());

    jarDeployer.renameJarsWithOldNamingConvention();

    deployedJarsBeforeRename.forEach(oldJar -> assertThat(oldJar).doesNotExist());

    File renamedJarAVersion1 = new File(deployDir, "myJarA.v1.jar");
    File renamedJarAVersion2 = new File(deployDir, "myJarA.v2.jar");
    File renamedJarBVersion2 = new File(deployDir, "myJarB.v2.jar");
    File renamedJarBVersion3 = new File(deployDir, "myJarB.v3.jar");
    Set<File> expectedJarsAfterRename = Stream
        .of(renamedJarAVersion1, renamedJarAVersion2, renamedJarBVersion2, renamedJarBVersion3)
        .collect(Collectors.toSet());

    Set<File> actualJarsInDeployDir =
        Stream.of(jarDeployer.getDeployDirectory().listFiles()).collect(Collectors.toSet());

    assertThat(actualJarsInDeployDir).isEqualTo(expectedJarsAfterRename);
  }

  @Test
  public void testOldJarNameMatcher() throws Exception {
    File deployDir = jarDeployer.getDeployDirectory();

    File jarAVersion1 = new File(deployDir, "vf.gf#myJarA.jar#1");
    this.classBuilder.writeJarFromName("ClassA", jarAVersion1);

    File jarAVersion2 = new File(deployDir, "vf.gf#myJarA.jar#2");
    this.classBuilder.writeJarFromName("ClassA", jarAVersion2);

    File jarBVersion2 = new File(deployDir, "vf.gf#myJarB.jar#2");
    this.classBuilder.writeJarFromName("ClassB", jarBVersion2);

    File jarBVersion3 = new File(deployDir, "vf.gf#myJarB.jar#3");
    this.classBuilder.writeJarFromName("ClassB", jarBVersion3);

    Set<File> jarsWithOldNamingConvention = Stream
        .of(jarAVersion1, jarAVersion2, jarBVersion2, jarBVersion3).collect(Collectors.toSet());

    jarsWithOldNamingConvention.forEach(
        jarFile -> assertThat(jarDeployer.isOldNamingConvention(jarFile.getName())).isTrue());

    Set<File> foundJarsWithOldNamingConvention = jarDeployer.findJarsWithOldNamingConvention();
    assertThat(foundJarsWithOldNamingConvention).isEqualTo(jarsWithOldNamingConvention);
  }


}
