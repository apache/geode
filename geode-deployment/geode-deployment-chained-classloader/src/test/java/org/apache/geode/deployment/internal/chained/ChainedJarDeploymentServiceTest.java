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
package org.apache.geode.deployment.internal.chained;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.test.compiler.JarBuilder;

public class ChainedJarDeploymentServiceTest {
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

  @Test
  public void testRegister() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    ServiceResult<Deployment> serviceResult = chainedJarDeploymentService.deploy(deployment);
    assertThat(serviceResult.isSuccessful()).isTrue();
  }

  @Test
  public void testRegisterWithFileOnly() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();

    ServiceResult<Deployment> serviceResult = chainedJarDeploymentService.deploy(myJar1);
    assertThat(serviceResult.isSuccessful()).isTrue();
  }

  @Test
  public void testRegisterWithNullFile() {
    File myJar = null;

    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();

    ServiceResult<Deployment> serviceResult = chainedJarDeploymentService.deploy(myJar);
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage()).contains("Jar file may not be null");
  }

  @Test
  public void testRegisterWithNullDeployment() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();

    Deployment deployment = null;

    ServiceResult<Deployment> serviceResult = chainedJarDeploymentService.deploy(deployment);
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage()).contains("Deployment may not be null");
  }

  @Test
  public void testRegisterWithNoJarFileSet() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment = new Deployment("myJar.jar", "test", Instant.now().toString());

    ServiceResult<Deployment> serviceResult = chainedJarDeploymentService.deploy(deployment);
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage())
        .contains("Cannot deploy Deployment without jar file");
  }

  @Test
  public void testUnregisterByName() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    chainedJarDeploymentService.deploy(deployment);

    ServiceResult<Deployment> serviceResult = chainedJarDeploymentService.undeploy("myJar1.jar");

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = chainedJarDeploymentService.listDeployed();
    assertThat(deployments).isEmpty();
  }

  @Test
  public void testUnregisterByNameWithMultipleJarsDeployed() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment1 = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment1.setFile(myJar1);

    Deployment deployment2 = new Deployment(myJar2.getName(), "test", Instant.now().toString());
    deployment2.setFile(myJar2);

    chainedJarDeploymentService.deploy(deployment1);
    chainedJarDeploymentService.deploy(deployment2);

    ServiceResult<Deployment> serviceResult = chainedJarDeploymentService.undeploy("myJar1.jar");

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = chainedJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(1);
    assertThat(deployments.get(0).getFileName()).contains("myJar2");
  }

  @Test
  public void testUnregisterByFileNameWithMultipleJarsDeployed() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment1 = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment1.setFile(myJar1);

    Deployment deployment2 = new Deployment(myJar2.getName(), "test", Instant.now().toString());
    deployment2.setFile(myJar2);

    chainedJarDeploymentService.deploy(deployment1);
    chainedJarDeploymentService.deploy(deployment2);

    ServiceResult<Deployment> serviceResult =
        chainedJarDeploymentService.undeploy(myJar1.getName());

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = chainedJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(1);
    assertThat(deployments.get(0).getFileName()).contains("myJar2");
  }

  @Test
  public void testUnregisterByFileName() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    chainedJarDeploymentService.deploy(deployment);

    ServiceResult<Deployment> serviceResult =
        chainedJarDeploymentService.undeploy(myJar1.getName());

    assertThat(serviceResult.isSuccessful()).isTrue();
    List<Deployment> deployments = chainedJarDeploymentService.listDeployed();
    assertThat(deployments).isEmpty();
  }

  @Test
  public void testUnregisterByNameWithNothingDeployed() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();

    ServiceResult<Deployment> serviceResult =
        chainedJarDeploymentService.undeploy("myJar");

    assertThat(serviceResult.isSuccessful()).isFalse();
  }

  @Test
  public void testUnregisterByFileNameWithNothingDeployed() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();

    ServiceResult<Deployment> serviceResult =
        chainedJarDeploymentService.undeploy("myJar.jar");

    assertThat(serviceResult.isSuccessful()).isFalse();
  }

  @Test
  public void testListDeployed() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    chainedJarDeploymentService.deploy(deployment);

    List<Deployment> deployments = chainedJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(1);
    assertThat(deployments.get(0).getFileName()).contains("myJar");
  }

  @Test
  public void testListDeployedWithNothingDeployed() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();

    List<Deployment> deployments = chainedJarDeploymentService.listDeployed();
    assertThat(deployments.size()).isEqualTo(0);
  }

  @Test
  public void testGetDeployment() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    chainedJarDeploymentService.deploy(deployment);

    ServiceResult<Deployment> serviceResult = chainedJarDeploymentService.getDeployed("myJar1.jar");
    assertThat(serviceResult.isSuccessful()).isTrue();
    assertThat(serviceResult.getMessage().getFileName()).isEqualTo("myJar1.jar");
  }

  @Test
  public void testGetDeploymentNotDeployed() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();

    ServiceResult<Deployment> serviceResult = chainedJarDeploymentService.getDeployed("myJar");
    assertThat(serviceResult.isSuccessful()).isFalse();
  }

  @Test
  public void testReinitializeWithWorkingDirectory() throws IOException {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment.setFile(myJar1);

    // shouldn't throw an exception since nothing is deployed.
    chainedJarDeploymentService.reinitializeWithWorkingDirectory(stagingTempDir.newFolder());

    chainedJarDeploymentService.deploy(deployment);

    // should throw an exception because there is a module deployed.
    assertThatThrownBy(() -> chainedJarDeploymentService
        .reinitializeWithWorkingDirectory(stagingTempDir.newFolder()))
            .isInstanceOf(RuntimeException.class).hasMessageContaining(
                "Cannot reinitialize working directory with existing deployments. Please undeploy first.");
  }

  @Test
  public void testCloseUnregistersAllDeployedJars() {
    ChainedJarDeploymentService chainedJarDeploymentService = new ChainedJarDeploymentService();
    Deployment deployment1 = new Deployment(myJar1.getName(), "test", Instant.now().toString());
    deployment1.setFile(myJar1);

    Deployment deployment2 = new Deployment(myJar2.getName(), "test", Instant.now().toString());
    deployment2.setFile(myJar2);

    chainedJarDeploymentService.deploy(deployment1);
    chainedJarDeploymentService.deploy(deployment2);

    chainedJarDeploymentService.close();

    List<Deployment> deployments = chainedJarDeploymentService.listDeployed();
    assertThat(deployments).isEmpty();
  }
}
