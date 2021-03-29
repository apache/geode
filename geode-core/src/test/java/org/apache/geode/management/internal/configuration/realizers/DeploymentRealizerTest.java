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
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.runtime.DeploymentInfo;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

public class DeploymentRealizerTest {
  private DeploymentRealizer realizer;
  private Deployment deployment;

  @Before
  public void before() throws Exception {
    realizer = spy(new DeploymentRealizer());
    deployment = new Deployment("a.jar", "test", Instant.now().toString());
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
    deployment.setFile(new File("/test/a.jar"));
    doReturn(Success.of(deployment)).when(realizer).getDeployed(any());
    DeploymentInfo deploymentInfo = realizer.get(deployment, null);
    assertThat(Paths.get(deploymentInfo.getJarLocation()).toUri())
        .isEqualTo(Paths.get("/test/a.jar").toUri());
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
    doReturn(Success.of(null)).when(realizer).deploy(any(Deployment.class));
    deployment.setFile(new File("/test/test.jar"));

    RealizationResult realizationResult = realizer.create(deployment, null);

    assertThat(realizationResult.getMessage()).contains("Already deployed");
    assertThat(realizationResult.isSuccess()).isTrue();
  }

  @Test
  public void createWithNewJar() throws Exception {
    String canonicalPath = "/home/sweet/home/test/test.jar";
    Deployment expectedDeployment = new Deployment("test.jar", "by", "time");
    File file = new File(canonicalPath);
    expectedDeployment.setFile(file);
    deployment = new Deployment(canonicalPath, "test", Instant.now().toString());
    deployment.setFile(new File("/test/test.jar"));
    doReturn(Success.of(expectedDeployment)).when(realizer).deploy(any(Deployment.class));

    RealizationResult realizationResult = realizer.create(deployment, null);

    assertThat(realizationResult.getMessage()).contains(file.getCanonicalPath());
    assertThat(realizationResult.isSuccess()).isTrue();
  }

  @Test
  public void createWithFailedDeploy() throws IOException, ClassNotFoundException {
    String eMessage = "test runtime exception";
    RuntimeException runtimeException = new RuntimeException(eMessage);
    doThrow(runtimeException).when(realizer).deploy(any(Deployment.class));
    deployment.setFile(new File("/test/test.jar"));

    assertThatThrownBy(() -> realizer.create(deployment, null))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(eMessage);
  }

  @Test
  public void createWithFailedGetForCanonicalPath() throws Exception {
    String eMessage = "error getting canonical path";
    doReturn(Failure.of(eMessage)).when(realizer).deploy(any(Deployment.class));
    deployment.setFile(new File("/test/test.jar"));

    RealizationResult realizationResult = realizer.create(deployment, null);
    assertThat(realizationResult.isSuccess()).isFalse();
    assertThat(realizationResult.getMessage()).contains(eMessage);
  }

  @Test
  public void alreadyDeployed() throws Exception {
    doReturn(Success.of(null)).when(realizer).deploy(any());
    RealizationResult realizationResult = realizer.create(deployment, null);
    assertThat(realizationResult.isSuccess()).isTrue();
    assertThat(realizationResult.getMessage()).contains("Already deployed");
  }
}
