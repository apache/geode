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
package org.apache.geode.test.compiler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JavaCompilerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void compileSingleClass() throws Exception {
    File implementsFunctionSourceFile = getFileFromTestResources("AbstractClass.java");
    String classContents = FileUtils.readFileToString(implementsFunctionSourceFile, "UTF-8");

    List<CompiledSourceCode> compiledSourceCodes = new JavaCompiler().compile(classContents);

    assertThat(compiledSourceCodes).hasSize(1);
  }

  @Test
  public void compileTwoDependentClasses() throws Exception {
    File sourceFileOne = getFileFromTestResources("AbstractClass.java");
    File sourceFileTwo = getFileFromTestResources("ConcreteClass.java");

    List<CompiledSourceCode> compiledSourceCodes =
        new JavaCompiler().compile(sourceFileOne, sourceFileTwo);

    assertThat(compiledSourceCodes).hasSize(2);
  }

  @Test
  public void invalidSourceThrowsException() throws IOException {
    JavaCompiler javaCompiler = new JavaCompiler();
    String sourceCode = "public class foo {this is not valid java source code}";
    assertThatThrownBy(() -> javaCompiler.compile(sourceCode)).isInstanceOf(Exception.class);
  }

  private File getFileFromTestResources(String fileName) throws URISyntaxException {
    URL resourceFileURL = getClass().getResource(fileName);
    assertThat(resourceFileURL).isNotNull();

    URI resourceUri = resourceFileURL.toURI();
    File file = new File(resourceUri);

    assertThat(file).exists();
    return file;
  }
}
