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

package org.apache.geode.deployment.internal.modules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.deployment.internal.modules.service.ModuleService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.test.compiler.JarBuilder;

public class ModularJarDeploymentServiceTest {

  private static File myJar1;
  private static File myJar2;

  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws IOException {
    JarBuilder jarBuilder = new JarBuilder();
    myJar1 = new File(stagingTempDir.newFolder(), "myJar1.jar");
    myJar2 = new File(stagingTempDir.newFolder(), "myJar2.jar");
    jarBuilder.buildJarFromClassNames(myJar1, "Class1");
    jarBuilder.buildJarFromClassNames(myJar2, "Class2");
  }

  private void cleanupTestDir(String testName) throws IOException {
    if (Paths.get(System.getProperty("user.dir")).resolve(testName).toFile().exists()) {
      Files.walk(Paths.get(System.getProperty("user.dir")).resolve(testName))
          .map(Path::toFile)
          .sorted((o1, o2) -> -o1.compareTo(o2))
          .forEach(File::delete);
    }
  }

  @Test
  public void testRegister() throws IOException {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);
    when(moduleService.moduleExists(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);
    modularJarDeploymentService.reinitializeWithWorkingDirectory(
        Paths.get(System.getProperty("user.dir"), "testRegister").toFile());
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(deployment);
    assertThat(serviceResult.isSuccessful()).isTrue();
    cleanupTestDir("testRegister");
  }

  @Test
  public void testRegisterWithFileOnly() throws IOException {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);
    when(moduleService.moduleExists(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);
    modularJarDeploymentService.reinitializeWithWorkingDirectory(
        Paths.get(System.getProperty("user.dir"), "testRegisterWithFileOnly").toFile());

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(myJar1);

    assertThat(serviceResult.isSuccessful()).isTrue();
    cleanupTestDir("testRegisterWithFileOnly");
  }

  @Test
  public void testRegisterWithNullFile() {
    File myJar = null;

    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(myJar);
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage()).contains("Jar file may not be null");
  }

  @Test
  public void testRegisterWithNullDeployment() {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);

    Deployment deployment = null;

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(deployment);
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage()).contains("Deployment may not be null");
  }

  @Test
  public void testRegisterWithNoJarFileSet() {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);
    Deployment deployment = new Deployment("myJar.jar", "test", Instant.now().toString());

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(deployment);
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage())
        .contains("Cannot deploy Deployment without jar file");
  }

  @Test
  public void testUnregisterByName() {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);
    when(moduleService.unregisterModule(any())).thenReturn(true);
    when(moduleService.moduleExists(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    modularJarDeploymentService.deploy(deployment);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.undeploy("myJar1.jar");

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments).isEmpty();
  }

  @Test
  public void testUnregisterByNameWithMultipleJarsDeployed() throws IOException {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);
    when(moduleService.unregisterModule(any())).thenReturn(true);
    when(moduleService.moduleExists(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);

    modularJarDeploymentService.reinitializeWithWorkingDirectory(
        Paths.get(System.getProperty("user.dir"), "testUnregisterByNameWithMultipleJarsDeployed")
            .toFile());
    Deployment deployment1 = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment1.setFile(myJar1);

    Deployment deployment2 = new Deployment(myJar2.getName(), "test", Instant.now().toString());
    deployment2.setFile(myJar2);

    modularJarDeploymentService.deploy(deployment1);
    modularJarDeploymentService.deploy(deployment2);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeploy("myJar1.jar");

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(1);
    assertThat(deployments.get(0).getFileName()).contains("myJar2");

    cleanupTestDir("testUnregisterByNameWithMultipleJarsDeployed");
  }

  @Test
  public void testUnregisterByFileNameWithMultipleJarsDeployed() throws IOException {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);
    when(moduleService.unregisterModule(any())).thenReturn(true);
    when(moduleService.moduleExists(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);

    modularJarDeploymentService.reinitializeWithWorkingDirectory(Paths
        .get(System.getProperty("user.dir"), "testUnregisterByFileNameWithMultipleJarsDeployed")
        .toFile());
    Deployment deployment1 = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment1.setFile(myJar1);

    Deployment deployment2 = new Deployment(myJar2.getName(), "test", Instant.now().toString());
    deployment2.setFile(myJar2);

    modularJarDeploymentService.deploy(deployment1);
    modularJarDeploymentService.deploy(deployment2);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeploy(myJar1.getName());

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(1);
    assertThat(deployments.get(0).getFileName()).contains("myJar2");

    cleanupTestDir("testUnregisterByFileNameWithMultipleJarsDeployed");
  }

  @Test
  public void testUnregisterByFileName() throws IOException {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);
    when(moduleService.unregisterModule(any())).thenReturn(true);
    when(moduleService.moduleExists(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);
    modularJarDeploymentService.reinitializeWithWorkingDirectory(
        Paths.get(System.getProperty("user.dir"), "testUnregisterByName").toFile());
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    modularJarDeploymentService.deploy(deployment);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeploy(myJar1.getName());

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments).isEmpty();

    cleanupTestDir("testUnregisterByName");
  }

  @Test
  public void testUnregisterByNameWithNothingDeployed() {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.unregisterModule(any())).thenReturn(false);
    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeploy("myJar");

    assertThat(serviceResult.isSuccessful()).isFalse();
  }

  @Test
  public void testUnregisterByFileNameWithNothingDeployed() {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.unregisterModule(any())).thenReturn(false);
    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeploy("myJar.jar");

    assertThat(serviceResult.isSuccessful()).isFalse();
  }

  @Test
  public void testListDeployed() {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);
    when(moduleService.moduleExists(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    modularJarDeploymentService.deploy(deployment);

    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(1);
    assertThat(deployments.get(0).getFileName()).contains("myJar");
  }

  @Test
  public void testListDeployedWithNothingDeployed() {
    ModuleService moduleService = mock(ModuleService.class);
    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);

    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(0);
  }

  @Test
  public void testGetDeployment() throws IOException {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);
    when(moduleService.moduleExists(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);
    modularJarDeploymentService.reinitializeWithWorkingDirectory(
        Paths.get(System.getProperty("user.dir"), "testGetDeployment").toFile());
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    modularJarDeploymentService.deploy(deployment);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.getDeployed("myJar1.jar");
    assertThat(serviceResult.isSuccessful()).isTrue();
    assertThat(serviceResult.getMessage().getFileName()).isEqualTo("myJar1.jar");

    cleanupTestDir("testGetDeployment");
  }

  @Test
  public void testGetDeploymentNotDeployed() {
    ModuleService moduleService = mock(ModuleService.class);
    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.getDeployed("myJar");
    assertThat(serviceResult.isSuccessful()).isFalse();
  }

  @Test
  public void testCloseUnregistersAllDeployedJars() {
    ModuleService moduleService = mock(ModuleService.class);
    when(moduleService.linkModule(any(), any(), anyBoolean())).thenReturn(true);
    when(moduleService.unregisterModule(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(moduleService);
    Deployment deployment1 = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment1.setFile(myJar1);

    Deployment deployment2 = new Deployment(myJar2.getName(), "test", Instant.now().toString());
    deployment2.setFile(myJar2);

    modularJarDeploymentService.deploy(deployment1);
    modularJarDeploymentService.deploy(deployment2);

    modularJarDeploymentService.close();

    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments).isEmpty();
  }
}
