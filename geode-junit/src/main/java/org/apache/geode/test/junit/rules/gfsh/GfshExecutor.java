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
package org.apache.geode.test.junit.rules.gfsh;

import java.io.File;
import java.nio.file.Path;

import org.apache.geode.test.version.VmConfiguration;

public interface GfshExecutor {

  GfshExecution execute(String... commands);

  GfshExecution execute(GfshScript gfshScript);

  GfshExecution execute(Path workingDir, String... commands);

  GfshExecution execute(File workingDir, String... commands);

  GfshExecution execute(Path workingDir, GfshScript gfshScript);

  GfshExecution execute(File workingDir, GfshScript gfshScript);

  interface Builder {

    GfshExecutor.Builder withJavaHome(Path javaHome);

    GfshExecutor.Builder withGeodeVersion(String geodeVersion);

    GfshExecutor.Builder withVmConfiguration(VmConfiguration vmConfiguration);

    GfshExecutor.Builder withGfshJvmOptions(String... option);

    GfshExecutor build(Path dir);
  }
}
