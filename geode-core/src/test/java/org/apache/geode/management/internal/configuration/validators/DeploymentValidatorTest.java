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

package org.apache.geode.management.internal.configuration.validators;

import static org.apache.geode.management.internal.CacheElementOperation.CREATE;
import static org.apache.geode.management.internal.CacheElementOperation.DELETE;
import static org.apache.geode.management.internal.CacheElementOperation.UPDATE;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.JarBuilder;

public class DeploymentValidatorTest {
  private DeploymentValidator deploymentValidator;
  private static File goodJar, noJar, tempDir;
  private Deployment deployment;

  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void stage() throws IOException {
    tempDir = tempFolder.newFolder("tempDir");
    goodJar = buildValidJar();
    noJar = buildInvalidJar();
  }

  @Before
  public void init() {
    deploymentValidator = new DeploymentValidator();
    deployment = new Deployment();
  }

  @Test
  public void validateCreateWithCorrectJarFile() {
    deployment.setFile(goodJar);

    assertThatCode(() -> deploymentValidator.validate(CREATE, deployment))
        .doesNotThrowAnyException();
  }


  @Test
  public void validateCreateWithIncorrectJarFile() {
    deployment.setFile(noJar);

    assertThatThrownBy(() -> deploymentValidator.validate(CREATE, deployment))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("File does not contain valid JAR content:");
  }

  @Test
  public void validateUpdateIsNotImplemented() {
    assertThatCode(() -> deploymentValidator.validate(UPDATE, deployment))
        .doesNotThrowAnyException();
  }

  @Test
  public void validateDeleteIsNotImplemented() {
    assertThatCode(() -> deploymentValidator.validate(DELETE, deployment))
        .doesNotThrowAnyException();
  }

  private static File buildValidJar() throws IOException {
    File jarFile = new File(tempDir, "valid.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(jarFile, "Class1");
    return jarFile;
  }

  private static File buildInvalidJar() {
    return new File(tempDir, "no.jar");
  }
}
