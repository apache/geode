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
  private static Path runnerContextDirectory;
  private static Path contextDirectory;

  /**
   * Returns the path to this JVM's test context directory. When tests create files or
   * directories, they should normally place the files and directories either within this test
   * context directory or in a directory created by the system's temporary file facility.
   *
   * <p>
   * The location of the test context directory depends on the nature of the JVM:
   * <ul>
   * <li>
   * If the JVM is a Gradle test worker, its test context directory is named after the Gradle test
   * worker and resolved against the JVM's working directory.
   * </li>
   * <li>
   * If the JVM is not a Gradle test worker, its test context directory is its working directory.
   * </li>
   * </ul>
   * <p>
   *
   * @return the absolute, normalized path to this JVM's test context directory
   */
  public static Path contextDirectory() {
    if (contextDirectory == null) {
      contextDirectory = isTestRunnerJVM() ? createContextDirectory() : jvmWorkingDirectory();
    }
    return contextDirectory;
  }

  /**
   * Creates a directory at the path formed by joining this JVM's {@link #contextDirectory() test
   * context directory} with an element that uniquely identifies the owner. The element that
   * identifies the owner is formed from the owner's simple class name and identity hash code.
   * If the directory already exists, this method simply returns the path to it.
   *
   * @param owner the owner of the directory
   * @return the absolute, normalized path to the subdirectory
   */
  public static Path createContextSubdirectory(Object owner) {
    return createDirectory(pathIdentifiedBy(owner));
  }

  public static boolean isTestRunnerJVM() {
    return workerName() != null;
  }

  /**
   * Returns the path to the test runner context directory. The test runner context directory is
   * the test context directory of the Gradle test worker JVM that is running the current test
   * class. Test code and most test framework code should use {@link #contextDirectory()} instead
   * of this method.
   *
   * <p>
   * The location of the test runner context directory depends on the nature of the JVM:
   * <ul>
   * <li>
   * In a Gradle test worker JVM, the test runner context directory is the same as the test context
   * directory.
   * </li>
   * <li>
   * If the JVM is not a Gradle test worker, its test runner context directory is the working
   * directory's nearest ancestor that is a test runner context directory, if there is such an
   * ancestor.
   * </li>
   * <li>
   * If the JVM is not a Gradle test worker and its working directory is not contained in a
   * runner directory, it has no test runner context directory.
   * </li>
   * </ul>
   *
   * @return the absolute, normalized path to this JVM's test runner context directory
   * @throws IllegalStateException if this JVM has no test runner context directory
   */
  public static Path runnerContextDirectory() {
    if (runnerContextDirectory == null) {
      runnerContextDirectory =
          isTestRunnerJVM() ? contextDirectory() : findRunnerContextDirectory();
    }
    return runnerContextDirectory;
  }

  private static Path contextDirectoryPath() {
    return jvmWorkingDirectory().resolve(RUNNER_DIR_PREFIX + workerName());
  }

  private static Path createContextDirectory() {
    return createDirectory(contextDirectoryPath());
  }

  private static Path createDirectory(Path dir) {
    try {
      return Files.createDirectories(dir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Path findRunnerContextDirectory() {
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

  private static Path pathIdentifiedBy(Object owner) {
    return contextDirectory().resolve(ownerName(owner));
  }

  private static String workerName() {
    return System.getProperty("org.gradle.test.worker");
  }
}
