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

package org.apache.geode.deployment.internal.modular;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.deployment.internal.modules.service.DeploymentService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.test.compiler.JarBuilder;

public class ModularJarDeploymentServiceTest {

  private static File myJar1;
  private static File myJar2;

  private static final JarBuilder jarBuilder = new JarBuilder();

  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws IOException {
    myJar1 = new File(stagingTempDir.newFolder(), "myJar1.jar");
    myJar2 = new File(stagingTempDir.newFolder(), "myJar2.jar");
    jarBuilder.buildJarFromClassNames(myJar1, "Class1");
    jarBuilder.buildJarFromClassNames(myJar2, "Class2");
  }

  @Test
  public void testRegister() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(deployment);
    assertThat(serviceResult.isSuccessful()).isTrue();
  }

  @Test
  public void testRegisterWithFileOnly() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(myJar1);
    assertThat(serviceResult.isSuccessful()).isTrue();
  }

  @Test
  public void testRegisterWithNullFile() {
    File myJar = null;

    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(myJar);
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage()).contains("Jar file may not be null");
  }

  @Test
  public void testRegisterWithNullDeployment() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);

    Deployment deployment = null;

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(deployment);
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage()).contains("Deployment may not be null");
  }

  @Test
  public void testRegisterWithNoJarFileSet() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
    Deployment deployment = new Deployment("myJar.jar", "test", Instant.now().toString());

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.deploy(deployment);
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage())
        .contains("Cannot deploy Deployment without jar file");
  }

  @Test
  public void testUnregisterByName() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);
    when(deploymentService.unregisterModule(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    modularJarDeploymentService.deploy(deployment);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeployByDeploymentName("myJar1");

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments).isEmpty();
  }

  @Test
  public void testUnregisterByNameWithMultipleJarsDeployed() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);
    when(deploymentService.unregisterModule(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
    Deployment deployment1 = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment1.setFile(myJar1);

    Deployment deployment2 = new Deployment(myJar2.getName(), "test", Instant.now().toString());
    deployment2.setFile(myJar2);

    modularJarDeploymentService.deploy(deployment1);
    modularJarDeploymentService.deploy(deployment2);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeployByDeploymentName("myJar1");

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(1);
    assertThat(deployments.get(0).getDeploymentName()).contains("myJar2");
  }

  @Test
  public void testUnregisterByFileNameWithMultipleJarsDeployed() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);
    when(deploymentService.unregisterModule(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
    Deployment deployment1 = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment1.setFile(myJar1);

    Deployment deployment2 = new Deployment(myJar2.getName(), "test", Instant.now().toString());
    deployment2.setFile(myJar2);

    modularJarDeploymentService.deploy(deployment1);
    modularJarDeploymentService.deploy(deployment2);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeployByFileName(myJar1.getName());

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(1);
    assertThat(deployments.get(0).getDeploymentName()).contains("myJar2");
  }

  @Test
  public void testUnregisterByFileName() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);
    when(deploymentService.unregisterModule(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    modularJarDeploymentService.deploy(deployment);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeployByFileName(myJar1.getName());

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments).isEmpty();
  }

  @Test
  public void testUnregisterByNameWithNothingDeployed() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.unregisterModule(any())).thenReturn(false);
    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeployByDeploymentName("myJar");

    assertThat(serviceResult.isSuccessful()).isFalse();
  }

  @Test
  public void testUnregisterByFileNameWithNothingDeployed() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.unregisterModule(any())).thenReturn(false);
    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);

    ServiceResult<Deployment> serviceResult =
        modularJarDeploymentService.undeployByDeploymentName("myJar.jar");

    assertThat(serviceResult.isSuccessful()).isFalse();
  }

  @Test
  public void testListDeployed() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    modularJarDeploymentService.deploy(deployment);

    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(1);
    assertThat(deployments.get(0).getDeploymentName()).contains("myJar");
  }

  @Test
  public void testListDeployedWithNothingDeployed() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);

    List<Deployment> deployments = modularJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(0);
  }

  @Test
  public void testGetDeployment() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    modularJarDeploymentService.deploy(deployment);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.getDeployed("myJar1");
    assertThat(serviceResult.isSuccessful()).isTrue();
    assertThat(serviceResult.getMessage().getDeploymentName()).isEqualTo("myJar1");
  }

  @Test
  public void testGetDeploymentNotDeployed() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);

    ServiceResult<Deployment> serviceResult = modularJarDeploymentService.getDeployed("myJar");
    assertThat(serviceResult.isSuccessful()).isFalse();
  }

  @Test
  public void testReinitializeWithWorkingDirectory() throws IOException {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    // shouldn't throw an exception since nothing is deployed.
    modularJarDeploymentService.reinitializeWithWorkingDirectory(stagingTempDir.newFolder());

    modularJarDeploymentService.deploy(deployment);

    // should throw an exception because there is a module deployed.
    assertThatThrownBy(() -> modularJarDeploymentService
        .reinitializeWithWorkingDirectory(stagingTempDir.newFolder()))
            .isInstanceOf(RuntimeException.class).hasMessageContaining(
                "Cannot reinitialize working directory with existing deployments. Please undeploy first.");
  }

  @Test
  public void testCloseUnregistersAllDeployedJars() {
    DeploymentService deploymentService = mock(DeploymentService.class);
    when(deploymentService.registerModule(any(), any(), any())).thenReturn(true);
    when(deploymentService.unregisterModule(any())).thenReturn(true);

    ModularJarDeploymentService modularJarDeploymentService =
        new ModularJarDeploymentService(deploymentService);
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
