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

import static java.lang.System.identityHashCode;
import static java.util.Objects.isNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Establishes the test root directory for files created by a test.
 * <p>
 * In a JVM launched directly by Gradle, the test root directory is named after the Gradle test
 * worker, and is a subdirectory of the JVM's working directory.
 * <p>
 * In a JVM not launched directly by Gradle (such as a DUnit child VM), the test root directory
 * is the JVM's working directory.
 */
public class TestRootDirectory {
  private static final String TEST_ROOT_DIR_PREFIX = "test-worker-jvm-";
  private static final Path PATH = create();

  /**
   * Returns the path to this JVM's test root directory.
   */
  public static Path path() {
    return PATH;
  }

  /**
   * Returns a path uniquely identified by the owner and resolved against this JVM's
   * {@link #path test root directory}. The path name identifies the owner based on its simple
   * class name and identity hash code. This method merely returns a path, and does not create a
   * file or directory. The owner (or caller) is responsible for all use of the path.
   *
   * @param owner the owner of the path
   * @return a path uniquely identified the owner within this JVM
   */
  public static Path pathOwnedBy(Object owner) {
    String name = String.format("%s-%x", owner.getClass().getSimpleName(), identityHashCode(owner));
    return path().resolve(name);
  }

  /**
   * Returns the path to a directory uniquely identified by the owner and resolved against this
   * JVM's {@link #path test root directory}. The path name identifies the owner based on its
   * simple class name and identity hash code. The directory is created if it does not already
   * exist. If the directory exists, this method simply returns a path to it. The owner (or
   * caller) is responsible for all use of the directory.
   *
   * @param owner the owner of the directory
   * @return the path to the created directory
   */
  public static Path directoryOwnedBy(Object owner) {
    return createDirectories(pathOwnedBy(owner));
  }

  private static Path create() {
    Path jvmWorkingDirectory = Paths.get(".").toAbsolutePath().normalize();
    String workerName = System.getProperty("org.gradle.test.worker");
    if (isNull(workerName)) {
      // This JVM is not a Gradle test worker JVM.
      return jvmWorkingDirectory;
    }
    Path workerTestRoot = jvmWorkingDirectory.resolve(TEST_ROOT_DIR_PREFIX + workerName);
    return createDirectories(workerTestRoot);
  }

  private static Path createDirectories(Path dir) {
    try {
      return Files.createDirectories(dir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
