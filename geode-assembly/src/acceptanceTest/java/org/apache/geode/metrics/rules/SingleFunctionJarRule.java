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
package org.apache.geode.metrics.rules;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.rules.ExternalResource;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.rules.accessible.AccessibleTemporaryFolder;

public class SingleFunctionJarRule extends ExternalResource {

  private final AccessibleTemporaryFolder temporaryFolder;
  private final String jarName;
  private final Class<? extends Function> functionClass;

  private Path jarPath;

  public SingleFunctionJarRule(String jarName, Class<? extends Function> functionClass) {
    temporaryFolder = new AccessibleTemporaryFolder();
    this.functionClass = functionClass;
    this.jarName = jarName;
  }

  @Override
  protected void before() throws Throwable {
    temporaryFolder.before();
    jarPath = newJar(jarName, functionClass);
  }

  @Override
  protected void after() {
    temporaryFolder.after();
  }

  public String absolutePath() {
    return jarPath.toAbsolutePath().toString();
  }

  public String deployCommand() {
    return "deploy --jar=" + absolutePath();
  }

  private Path newJar(String jarName, Class<? extends Function> functionClass) throws IOException {
    File jar = temporaryFolder.newFile(jarName);
    new ClassBuilder().writeJarFromClass(functionClass, jar);
    return jar.toPath();
  }
}
