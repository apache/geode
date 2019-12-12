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

package org.apache.geode.management.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;

public class DeploymentTest {
  private ObjectMapper mapper = GeodeJsonMapper.getMapper();
  private Deployment deployment;

  @Before
  public void before() throws Exception {
    deployment = new Deployment();
  }

  @Test
  public void remembersTimeDeployed() {
    deployment.setTimeDeployed("timeDeployed");
    assertThat(deployment.getTimeDeployed()).isEqualTo("timeDeployed");
  }

  @Test
  public void remembersDeployedBy() {
    deployment.setDeployedBy("deployedBy");
    assertThat(deployment.getDeployedBy()).isEqualTo("deployedBy");
  }

  @Test
  public void remembersJarFileName() {
    deployment.setJarFileName("jarFileName");
    assertThat(deployment.getJarFileName()).isEqualTo("jarFileName");
  }

  @Test
  public void idSetByJarFileName() {
    deployment.setJarFileName("jarFileName");
    assertThat(deployment.getId()).isEqualTo("jarFileName");
  }

  @Test
  public void serializeToJson() throws Exception {
    deployment.setGroup("group1");
    deployment.setJarFileName("jarFileName");
    deployment.setDeployedBy("deployedBy");
    deployment.setTimeDeployed("timeDeployed");
    String json = mapper.writeValueAsString(deployment);
    Deployment newValue = mapper.readValue(json, Deployment.class);
    assertThat(newValue).isEqualToComparingFieldByField(deployment);
  }

  @Test
  public void notShowId() throws Exception {
    deployment.setJarFileName("abc.jar");
    String json = mapper.writeValueAsString(deployment);
    assertThat(json).doesNotContain("id");
  }

  @Test
  public void selfLinkUsesJarFileName() {
    deployment.setJarFileName("jarFileName");
    assertThat(deployment.getLinks().getSelf()).isEqualTo("/deployments/jarFileName");
  }

  @Test
  public void listLinkUsesJarFileName() {
    assertThat(deployment.getLinks().getList()).isEqualTo("/deployments");
  }

  @Test
  public void linksHasOnlySelfAndList() {
    deployment.setJarFileName("jarFileName");
    assertThat(deployment.getLinks().getLinks()).containsOnlyKeys("self", "list");
  }
}
