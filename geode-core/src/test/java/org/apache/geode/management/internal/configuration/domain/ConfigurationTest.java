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
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
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
    deployment.setJarFileName("jar1");
    configuration.addDeployment(deployment);
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(deployment);
  }

  @Test
  public void remembersNewestDeploymentWithSameArtifactId() {
    Deployment deployment1 = new Deployment();
    deployment1.setJarFileName("abc-1.0.jar");
    configuration.addDeployment(deployment1);
    Deployment deployment2 = new Deployment();
    deployment2.setJarFileName("abc-2.0.jar");
    configuration.addDeployment(deployment2);
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(deployment2);
  }

  @Test
  public void remembersAllDeploymentsWithDifferentArtifactIds() {
    Deployment deployment1 = new Deployment();
    deployment1.setJarFileName("abc-1.0.jar");
    configuration.addDeployment(deployment1);
    Deployment deployment2 = new Deployment();
    deployment2.setJarFileName("def-2.0.jar");
    configuration.addDeployment(deployment2);
    assertThat(configuration.getDeployments()).containsExactlyInAnyOrder(deployment1, deployment2);
  }

  @Test
  public void getJarNamesReturnsJarNamesFromAllCurrentDeployments() {
    String originalAbcJarName = "abc-1.0.jar";
    Deployment deployment1 = new Deployment(originalAbcJarName);
    configuration.addDeployment(deployment1);

    String updatedAbcJarName = "abc-2.0.jar";
    // Replace original abc with new version
    configuration.addDeployment(new Deployment(updatedAbcJarName));

    String defJarName = "def-1.0.jar";
    Deployment deployment3 = new Deployment(defJarName);
    configuration.addDeployment(deployment3);

    assertThat(configuration.getJarNames())
        .containsExactlyInAnyOrder(updatedAbcJarName, defJarName);
  }

  @Test
  public void dataSerializationRoundTrip() throws IOException, ClassNotFoundException {
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(outBytes);

    Configuration config = new Configuration();
    config.addDeployment(new Deployment("jarName1.jar", "deployedBy1", "timeDeployed1"));
    config.addDeployment(new Deployment("jarName2.jar", "deployedBy2", "timeDeployed2"));
    config.addDeployment(new Deployment("jarName3.jar", "deployedBy3", "timeDeployed3"));

    DataSerializer.writeObject(config, dataOut);
    dataOut.flush();

    ByteArrayInputStream inBytes = new ByteArrayInputStream(outBytes.toByteArray());
    DataInput dataIn = new DataInputStream(inBytes);

    assertEquals(config, DataSerializer.readObject(dataIn));
  }

  @Ignore("wip")
  @Test
  public void dataSerializationDeserializesOldFormat() throws IOException, ClassNotFoundException {
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(outBytes);

    Configuration config = new Configuration();
    config.addDeployment(new Deployment("jarName1.jar", "deployedBy1", "timeDeployed1"));
    config.addDeployment(new Deployment("jarName2.jar", "deployedBy2", "timeDeployed2"));
    config.addDeployment(new Deployment("jarName3.jar", "deployedBy3", "timeDeployed3"));

    writeConfigInOldFormat(config, dataOut);
    dataOut.flush();

    ByteArrayInputStream inBytes = new ByteArrayInputStream(outBytes.toByteArray());
    DataInput dataIn = new DataInputStream(inBytes);

    assertEquals(config, DataSerializer.readObject(dataIn));
  }

  private static void writeConfigInOldFormat(Configuration config, DataOutput out)
      throws IOException {
    DataSerializer.writeString(config.getConfigName(), out);
    DataSerializer.writeString(config.getCacheXmlFileName(), out);
    DataSerializer.writeString(config.getCacheXmlContent(), out);
    DataSerializer.writeString(config.getPropertiesFileName(), out);
    DataSerializer.writeProperties(config.getGemfireProperties(), out);
    DataSerializer.writeHashSet(new HashSet<>(config.getJarNames()), out);
  }
}
