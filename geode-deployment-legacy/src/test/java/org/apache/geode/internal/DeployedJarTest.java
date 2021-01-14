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
package org.apache.geode.internal;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.compiler.JarBuilder;

public class DeployedJarTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File jarFile;

  @Test
  public void doesNotThrowIfFileIsValidJar() throws IOException {
    createJarFile("test.v1.jar");
    assertThatCode(() -> new DeployedJar(jarFile))
        .doesNotThrowAnyException();
  }

  @Test
  public void throwsIfFileIsNotValidJarFile() throws Exception {
    createJarFile("test.v1.jar");
    Files.write(jarFile.toPath(), "INVALID_JAR_CONTENT".getBytes());

    assertThatThrownBy(() -> new DeployedJar(jarFile))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void throwsIfFileIsEmptyFile() throws Exception {
    File emptyFile = createEmptyFile("empty.jar");

    assertThatThrownBy(() -> new DeployedJar(emptyFile))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getDeployedFileName() throws Exception {
    // deployed file name should always have .v# appended to it
    createJarFile("test.jar");
    assertThatThrownBy(() -> new DeployedJar(jarFile).getDeployedFileName())
        .isInstanceOf(IllegalStateException.class);

    createJarFile("test.v1.jar");
    assertThat(new DeployedJar(jarFile).getDeployedFileName()).isEqualTo("test.jar");

    createJarFile("test-1.0.v2.jar");
    assertThat(new DeployedJar(jarFile).getDeployedFileName()).isEqualTo("test-1.0.jar");

    createJarFile("test-1.1.v3.jar");
    assertThat(new DeployedJar(jarFile).getDeployedFileName()).isEqualTo("test-1.1.jar");
  }

  void createJarFile(String jarName) throws IOException {
    jarFile = new File(temporaryFolder.getRoot(), jarName);
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(jarFile, "ExpectedClass");
  }

  File createEmptyFile(String fileName) {
    return new File(temporaryFolder.getRoot(), fileName);
  }
}
