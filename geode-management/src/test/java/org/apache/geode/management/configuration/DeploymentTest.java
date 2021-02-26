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
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;

public class DeploymentTest {
  private ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Test
  public void remembersDeployedTime() {
    Deployment deployment = new Deployment("name.jar", "test", "deployedTime");
    assertThat(deployment.getDeployedTime()).isEqualTo("deployedTime");
  }

  @Test
  public void remembersDeployedBy() {
    Deployment deployment = new Deployment("name.jar", "deployedBy", "deployedTime");
    assertThat(deployment.getDeployedBy()).isEqualTo("deployedBy");
  }

  @Test
  public void remembersJarFileName() {
    Deployment deployment = new Deployment("jarFileName", "test", "deployedTime");
    deployment.setFileName("jarFileName");
    assertThat(deployment.getFileName()).isEqualTo("jarFileName");
  }

  @Test
  public void idSetByJarFileName() {
    Deployment deployment = new Deployment("jarFileName", "test", "deployedTime");
    assertThat(deployment.getId()).isEqualTo("jarFileName");
  }

  @Test
  public void jsonSerializationRoundTrip() throws Exception {
    Deployment deployment = new Deployment("jarFileName", "deployedBy", "deployedTime");
    deployment.setGroup("group1");
    String json = mapper.writeValueAsString(deployment);
    Deployment newValue = mapper.readValue(json, Deployment.class);
    assertThat(newValue).usingRecursiveComparison().isEqualTo(deployment);
  }

  @Test
  public void notShowId() throws Exception {
    Deployment deployment = new Deployment("abc.jar", "test", "deployedTime");
    String json = mapper.writeValueAsString(deployment);
    assertThat(json).doesNotContain("id");
  }

  @Test
  public void selfLinkUsesJarFileName() {
    Deployment deployment = new Deployment("jarFileName", "test", "deployedTime");
    assertThat(deployment.getLinks().getSelf()).isEqualTo("/deployments/jarFileName");
  }

  @Test
  public void listLinkUsesJarFileName() {
    Deployment deployment = new Deployment("jarFileName", "test", "deployedTime");
    assertThat(deployment.getLinks().getList()).isEqualTo("/deployments");
  }

  @Test
  public void linksHasOnlySelfAndList() {
    Deployment deployment = new Deployment("jarFileName", "test", "deployedTime");
    assertThat(deployment.getLinks().getLinks()).containsOnlyKeys("self", "list");
  }
}
