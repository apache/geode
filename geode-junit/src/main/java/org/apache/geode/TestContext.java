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

package org.apache.geode;

import static java.lang.System.identityHashCode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The context for files and directories created by a test.
 */
public class TestContext {
  private static final String RUNNER_DIR_PREFIX = "test-runner-";
  private static Path runnerDirectory;
  private static Path directory;

  /**
   * Returns the path to this JVM's test context directory. When tests create files or
   * directories, they should normally place the files and directories either within this test
   * context directory or in a directory created by the system's temporary file facility.
   *
   * <p>
   * The location of the test context directory depends on the nature of the JVM:
   * <ul>
   * <li>
   * If the JVM is a Gradle test worker, its test context directory is named after the Gradle
   * test worker and resolved against the JVM's working directory.
   * </li>
   * <li>
   * If the JVM is not a Gradle test worker, its test context directory is its working
   * directory.
   * </li>
   * </ul>
   * <p>
   *
   * @return the absolute, normalized path to this JVM's test context directory
   */
  public static Path directory() {
    if (directory == null) {
      directory = isTestRunnerJVM() ? createContextDirectory() : jvmWorkingDirectory();
    }
    return directory;
  }

  /**
   * Returns a path uniquely identified by the owner and resolved against this JVM's
   * {@link #directory() test context directory}. The path name identifies the owner based on its
   * simple class name and identity hash code. This method merely returns a path, and does not
   * create a file or directory. The owner (or caller) is responsible for all use of the path.
   *
   * @param owner the owner of the path
   * @return an absolute, normalized path uniquely identified by the owner
   */
  public static Path pathOwnedBy(Object owner) {
    return directory().resolve(ownerName(owner));
  }

  /**
   * Returns the path to a directory uniquely identified by the owner and resolved against this
   * JVM's {@link #directory() test context directory}. The path name identifies the owner based
   * on its simple class name and identity hash code. The directory is created if it does not
   * already exist. If the directory exists, this method simply returns a path to it. The owner (or
   * caller) is responsible for all use of the directory.
   *
   * @param owner the owner of the directory
   * @return the absolute, normalized path to the subdirectory
   */
  public static Path subdirectoryOwnedBy(Object owner) {
    return createDirectory(pathOwnedBy(owner));
  }

  public static boolean isTestRunnerJVM() {
    return workerName() != null;
  }

  /**
   * Returns the path to the runner directory. The runner directory is the test context directory
   * of the Gradle test worker JVM that is running the current test class. Most test code should
   * use {@link #directory()} instead of this method.
   *
   * <p>
   * The location of the runner directory depends on the nature of the JVM:
   * <ul>
   * <li>
   * In a Gradle test worker JVM, the runner directory is the same as the test context
   * directory.
   * </li>
   * <li>
   * If the JVM is not a Gradle test worker, its runner directory is the working directory's
   * nearest ancestor that is a runner directory, if there is such an ancestor.
   * </li>
   * <li>
   * If the JVM is not a Gradle test worker and its working directory is not contained in a
   * runner directory, it has no runner directory.
   * </li>
   * </ul>
   *
   * @return the absolute, normalized path to this JVM's test runner directory
   * @throws IllegalStateException if this JVM has no test runner directory
   */
  public static Path runnerDirectory() {
    if (runnerDirectory == null) {
      runnerDirectory = isTestRunnerJVM() ? directory() : findRunnerDirectory();
    }
    return runnerDirectory;
  }

  private static Path createContextDirectory() {
    return createDirectory(workerPath());
  }

  private static Path createDirectory(Path dir) {
    try {
      return Files.createDirectories(dir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Path findRunnerDirectory() {
    Path jvmWorkingDirectory = jvmWorkingDirectory();
    Path candidate = jvmWorkingDirectory.getParent();
    while (candidate != null) {
      if (candidate.getFileName().toString().startsWith(RUNNER_DIR_PREFIX)) {
        return candidate;
      }
      candidate = candidate.getParent();
    }
    // This JVM is not a test runner and its working directory is not contained in a runner
    // directory.
    throw new IllegalStateException("This JVM has no test runner directory");
  }

  private static Path jvmWorkingDirectory() {
    return Paths.get(".").toAbsolutePath().normalize();
  }

  private static String ownerName(Object owner) {
    return String.format("%s-%x", owner.getClass().getSimpleName(), identityHashCode(owner));
  }

  private static String workerName() {
    return System.getProperty("org.gradle.test.worker");
  }

  private static Path workerPath() {
    return jvmWorkingDirectory().resolve(RUNNER_DIR_PREFIX + workerName());
  }
}
