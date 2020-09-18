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

package org.apache.geode.test;

import static java.util.Objects.isNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Establishes the test root directory for files created by a test.
 * <p>
 * In a JVM launched directly by Gradle, the test root directory is named after the Gradle test
 * worker, and is a subdirectory in the JVM's working directory.
 * <p>
 * In a JVM not launched directly by Gradle (such as a DUnit child VM), the test root directory
 * is the JVM's working directory.
 */
public class TestRootDirectory {
  private static final String TEST_ROOT_DIR_PREFIX = "test-worker-jvm-";
  private static Path root;

  public static Path path() {
    if (isNull(root)) {
      root = createRootDir();
    }
    return root;
  }

  public static File file() {
    return path().toFile();
  }

  private static Path createRootDir() {
    Path currentWorkingDirectory = Paths.get(".").normalize().toAbsolutePath();
    String workerName = System.getProperty("org.gradle.test.worker");
    if (isNull(workerName)) {
      // This JVM is not a Gradle test worker JVM.
      return currentWorkingDirectory;
    }
    Path workerTestRoot = currentWorkingDirectory.resolve(TEST_ROOT_DIR_PREFIX + workerName);
    try {
      return Files.createDirectories(workerTestRoot);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
