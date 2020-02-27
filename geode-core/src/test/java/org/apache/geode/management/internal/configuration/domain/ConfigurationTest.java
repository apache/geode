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

package org.apache.geode.management.internal.configuration.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.DataSerializer;
import org.apache.geode.management.configuration.Deployment;

public class ConfigurationTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private Configuration configuration;

  @Before
  public void before() throws Exception {
    configuration = new Configuration("cluster");
  }

  @Test
  public void setInvalidCacheXmlFile() throws IOException {
    File file = folder.newFile("test.xml");
    FileUtils.writeStringToFile(file, "invalid xml content", "UTF-8");
    assertThatThrownBy(() -> configuration.setCacheXmlFile(file)).isInstanceOf(IOException.class)
        .hasMessageContaining("Unable to parse");
  }

  @Test
  public void remembersDeployment() {
    Deployment deployment = new Deployment();
    deployment.setFileName("jar1");
    configuration.putDeployment(deployment);
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(deployment);
  }

  @Test
  public void remembersNewestDeploymentWithSameArtifactId() {
    Deployment deployment1 = new Deployment();
    deployment1.setFileName("abc-1.0.jar");
    configuration.putDeployment(deployment1);
    Deployment deployment2 = new Deployment();
    deployment2.setFileName("abc-2.0.jar");
    configuration.putDeployment(deployment2);
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(deployment2);
  }

  @Test
  public void remembersAllDeploymentsWithDifferentArtifactIds() {
    Deployment deployment1 = new Deployment();
    deployment1.setFileName("abc-1.0.jar");
    configuration.putDeployment(deployment1);
    Deployment deployment2 = new Deployment();
    deployment2.setFileName("def-2.0.jar");
    configuration.putDeployment(deployment2);
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(deployment1, deployment2);
  }

  @Test
  public void getJarNamesReturnsJarNamesFromAllCurrentDeployments() {
    String originalAbcJarName = "abc-1.0.jar";
    Deployment deployment1 = new Deployment(originalAbcJarName, null, null);
    configuration.putDeployment(deployment1);

    String updatedAbcJarName = "abc-2.0.jar";
    // Replace original abc with new version
    configuration.putDeployment(new Deployment(updatedAbcJarName, null, null));

    String defJarName = "def-1.0.jar";
    Deployment deployment3 = new Deployment(defJarName, null, null);
    configuration.putDeployment(deployment3);

    assertThat(configuration.getJarNames())
        .containsExactlyInAnyOrder(updatedAbcJarName, defJarName);
  }

  @Test
  public void dataSerializationRoundTrip() throws IOException, ClassNotFoundException {
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(outBytes);

    Configuration originalConfig = new Configuration();
    originalConfig.putDeployment(new Deployment("jarName1.jar", "deployedBy1", "deployedTime1"));
    originalConfig.putDeployment(new Deployment("jarName2.jar", "deployedBy2", "deployedTime2"));
    originalConfig.putDeployment(new Deployment("jarName3.jar", "deployedBy3", "deployedTime3"));

    DataSerializer.writeObject(originalConfig, dataOut);
    dataOut.flush();

    ByteArrayInputStream inBytes = new ByteArrayInputStream(outBytes.toByteArray());
    DataInput dataIn = new DataInputStream(inBytes);

    Configuration deserializedConfig = DataSerializer.readObject(dataIn);

    assertThat(deserializedConfig)
        .isEqualTo(originalConfig);
  }
}
