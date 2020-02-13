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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.DeployedJar;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.runtime.DeploymentInfo;

public class DeploymentRealizerTest {
  private DeploymentRealizer realizer;
  private Map<String, DeployedJar> deployed;
  private Deployment deployment;

  @Before
  public void before() throws Exception {
    realizer = spy(new DeploymentRealizer());
    deployed = new HashMap<>();
    doReturn(deployed).when(realizer).getDeployedJars();
    deployment = new Deployment();
  }

  @Test
  public void jarNotFound() throws Exception {
    deployment.setFileName("a.jar");
    DeploymentInfo deploymentInfo = realizer.get(deployment, null);
    assertThat(deploymentInfo.getJarLocation()).isEqualTo(DeploymentRealizer.JAR_NOT_DEPLOYED);
  }

  @Test
  public void jarFound() throws Exception {
    deployment.setFileName("a.jar");
    DeployedJar deployedJar = mock(DeployedJar.class);
    when(deployedJar.getFile()).thenReturn(new File("/test/a.jar"));
    deployed.put("a", deployedJar);
    DeploymentInfo deploymentInfo = realizer.get(deployment, null);
    assertThat(Paths.get(deploymentInfo.getJarLocation()).toUri())
        .isEqualTo(Paths.get("/test/a.jar").toUri());
  }

  @Test
  public void getDateString() throws Exception {
    assertThat(realizer.getDateString(1576029093330L)).isEqualTo("2019-12-11T01:51:33.330Z");
  }
}
