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

package org.apache.geode.management.internal.configuration.realizers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.DeployedJar;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.runtime.DeploymentInfo;

public class DeploymentRealizerTest {
  private DeploymentRealizer realizer;
  private Map<String, DeployedJar> deployed;
  private Deployment deployment;
  private DeployedJar deployedJar;


  @Before
  public void before() throws Exception {
    realizer = spy(new DeploymentRealizer());
    deployed = new HashMap<>();
    doReturn(deployed).when(realizer).getDeployedJars();
    deployment = new Deployment();
    deployedJar = mock(DeployedJar.class);
  }

  @Test
  public void getWithJarNotFound() {
    deployment.setFileName("a.jar");
    DeploymentInfo deploymentInfo = realizer.get(deployment, null);
    assertThat(deploymentInfo.getJarLocation()).isEqualTo(DeploymentRealizer.JAR_NOT_DEPLOYED);
  }

  @Test
  public void getWithJarFound() {
    deployment.setFileName("a.jar");
    when(deployedJar.getFile()).thenReturn(new File("/test/a.jar"));
    deployed.put("a", deployedJar);
    DeploymentInfo deploymentInfo = realizer.get(deployment, null);
    assertThat(Paths.get(deploymentInfo.getJarLocation()).toUri())
        .isEqualTo(Paths.get("/test/a.jar").toUri());
  }

  @Test
  public void getDateString() {
    assertThat(realizer.getDateString(1576029093330L)).isEqualTo("2019-12-11T01:51:33.330Z");
  }

  @Test
  public void existsReturnsFalse() {
    assertThat(realizer.exists(any(), any())).isFalse();
  }

  @Test
  public void updateIsNotImplemented() {
    assertThatThrownBy(() -> realizer.update(any(), any()))
        .hasMessageContaining("Not implemented")
        .isInstanceOf(NotImplementedException.class);
  }

  @Test
  public void deleteIsNotImplemented() {
    assertThatThrownBy(() -> realizer.delete(any(), any()))
        .hasMessageContaining("Not implemented")
        .isInstanceOf(NotImplementedException.class);
  }

  @Test
  public void createWithAlreadyExistingJar() throws Exception {
    doReturn(null).when(realizer).deploy(any(File.class));
    deployment.setFile(new File("/test/test.jar"));

    RealizationResult realizationResult = realizer.create(deployment, null);

    assertThat(realizationResult.getMessage()).contains("Already deployed");
    assertThat(realizationResult.isSuccess()).isTrue();
  }

  @Test
  public void createWithNewJar() throws Exception {
    String canonicalPath = "/home/sweet/home/test/test.jar";
    doReturn(canonicalPath).when(deployedJar).getFileCanonicalPath();
    doReturn(deployedJar).when(realizer).deploy(any(File.class));
    deployment.setFile(new File("/test/test.jar"));

    RealizationResult realizationResult = realizer.create(deployment, null);

    assertThat(realizationResult.getMessage()).contains(canonicalPath);
    assertThat(realizationResult.isSuccess()).isTrue();
  }

  @Test
  public void createWithFailedDeploy() throws IOException, ClassNotFoundException {
    String eMessage = "test runtime exception";
    RuntimeException runtimeException = new RuntimeException(eMessage);
    doThrow(runtimeException).when(realizer).deploy(any(File.class));
    deployment.setFile(new File("/test/test.jar"));

    assertThatThrownBy(() -> realizer.create(deployment, null))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(eMessage);
  }

  @Test
  public void createWithFailedGetForCanonicalPath() throws Exception {
    String eMessage = "error getting canonical path";
    IOException ioException = new IOException(eMessage);
    doThrow(ioException).when(deployedJar).getFileCanonicalPath();
    doReturn(deployedJar).when(realizer).deploy(any(File.class));
    deployment.setFile(new File("/test/test.jar"));

    assertThatThrownBy(() -> realizer.create(deployment, null))
        .isInstanceOf(IOException.class)
        .hasMessageContaining(eMessage);
  }

  @Test
  public void alreadyDeployed() throws Exception {
    doReturn(null).when(realizer).deploy(any());
    RealizationResult realizationResult = realizer.create(deployment, null);
    assertThat(realizationResult.isSuccess()).isTrue();
    assertThat(realizationResult.getMessage()).isEqualTo("Already deployed");
  }
}
