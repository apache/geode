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


import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.compiler.JarBuilder;

public class DeployedJarJUnitTest {
  private static final String JAR_NAME = "test.jar";
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private JarBuilder jarBuilder;
  private File jarFile;
  private byte[] expectedJarBytes;

  @Before
  public void setup() throws Exception {
    jarBuilder = new JarBuilder();
    jarFile = new File(temporaryFolder.getRoot(), JAR_NAME);
    jarBuilder.buildJarFromClassNames(jarFile, "ExpectedClass");
    expectedJarBytes = FileUtils.readFileToByteArray(jarFile);
  }

  @Test
  public void validJarContentDoesNotThrow() throws Exception {
    new DeployedJar(jarFile, JAR_NAME);
  }

  @Test
  public void invalidContentThrowsException() throws Exception {
    givenInvalidJarBytes();

    assertThatThrownBy(() -> new DeployedJar(jarFile, JAR_NAME))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private byte[] givenInvalidJarBytes() throws IOException {
    byte[] invalidJarBytes = "INVALID JAR CONTENT".getBytes();
    FileUtils.writeByteArrayToFile(jarFile, invalidJarBytes);

    return invalidJarBytes;
  }
}
