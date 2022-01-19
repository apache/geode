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
package org.apache.geode.test.junit.rules;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

public class JarFileRule extends SerializableExternalResource {

  private final transient TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final transient ClassBuilder classBuilder = new ClassBuilder();

  private final String className;
  private final String jarName;
  private File jarFile;
  boolean makeJarLarge;

  public JarFileRule(String className, String jarName) {
    this(className, jarName, false);
  }

  public JarFileRule(String className, String jarName, boolean makeJarLarge) {
    this.className = className;
    this.jarName = jarName;
    this.makeJarLarge = makeJarLarge;
  }

  @Override
  protected void before() throws IOException {
    temporaryFolder.create();
    jarFile = temporaryFolder.newFile(jarName);

    if (makeJarLarge) {
      classBuilder.writeJarFromContent(className,
          "public class " + className + "{" + "String test = \""
              + RandomStringUtils.randomAlphanumeric(10000) + "\";" + "String test2 = \""
              + RandomStringUtils.randomAlphanumeric(10000) + "\";" + "String test3 = \""
              + RandomStringUtils.randomAlphanumeric(10000) + "\";" + "String test4 = \""
              + RandomStringUtils.randomAlphanumeric(10000) + "\";" + "}",
          jarFile);
    } else {
      classBuilder.writeJarFromName(className, jarFile);
    }
  }

  @Override
  protected void after() {
    temporaryFolder.delete();
  }

  public File getJarFile() {
    assertThat(jarFile).exists();
    return jarFile;
  }

  public String getJarName() {
    return jarName;
  }

}
