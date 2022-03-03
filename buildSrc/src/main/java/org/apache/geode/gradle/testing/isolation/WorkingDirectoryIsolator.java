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
 *
 */

package org.apache.geode.gradle.testing.isolation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.gradle.api.UncheckedIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkingDirectoryIsolator implements Consumer<ProcessBuilder> {
  private final Logger log = LoggerFactory.getLogger(this.getClass());

  private static final AtomicInteger WORKER_ID = new AtomicInteger();
  private static final Pattern GRADLE_WORKER_CLASSPATH_FILE_PATTERN =
      Pattern.compile("^@.*gradle-worker-classpath.*txt$");

  private static final Pattern RELATIVE_PARENT_DIRECTORY_PATTERN =
      Pattern.compile("(?<![/\\\\])(\\.\\.[/\\\\])");
  private static final String PROPERTIES_FILE_NAME = "gemfire.properties";

  /**
   * Each test task gives all of its test workers the same working directory. Because
   * Geode tests cannot tolerate this when run in parallel, we give each test worker its
   * own unique working directory.
   */
  @Override
  public void accept(ProcessBuilder processBuilder) {
    String subdirectory = String.format("test-worker-%06d", WORKER_ID.getAndIncrement());
    Path originalWorkingDirectory = processBuilder.directory().toPath();
    Path newWorkingDirectory = originalWorkingDirectory.resolve(subdirectory);

    try {
      Files.createDirectories(newWorkingDirectory);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    processBuilder.directory(newWorkingDirectory.toFile());

    Path originalPropertiesFile = originalWorkingDirectory.resolve(PROPERTIES_FILE_NAME);
    if (Files.exists(originalPropertiesFile)) {
      Path newPropertiesFile = newWorkingDirectory.resolve(PROPERTIES_FILE_NAME);
      copy(originalPropertiesFile, newPropertiesFile);
    }

    // If the command specifies a gradle worker classpath file that exists, copy the file to the
    // unique working directory and update the command line argument to refer to the new location.
    List<String> command = processBuilder.command();
    findGradleWorkerClasspathArg(command)
        .ifPresent(i -> updateGradleWorkerClasspathFile(command, i, newWorkingDirectory));

    upgradeRelativePaths(command);

    log.debug("WorkingDirectoryIsolator updated command. New Working directory: {}, Command: {}", newWorkingDirectory, command);
  }

  /**
   * Replace all occurrences of "../" that are not proceeded by a / in the command with "../../"
   * to match the new working directory of the process. This fixes issues with java agent commands
   * like jacoco that pass relative paths, eg "-agentlib:../tmp/jacocoXXX=../jacoco/test.txt
   * @param command the command line to upgrade. It will be modified in place.
   */
  private void upgradeRelativePaths(List<String> command) {
    for(int i =0 ; i < command.size(); i++) {
      String element = command.get(i);
      final Matcher matcher = RELATIVE_PARENT_DIRECTORY_PATTERN.matcher(element);
      //TO be platform independent, we will capture the syntax used (../ or ..\) and
      //simply duplicate that in the command line string to become (../../ or ..\..\)
      element = RELATIVE_PARENT_DIRECTORY_PATTERN.matcher(element).replaceAll("$1$1");
      command.set(i, element);
    }
  }

  private void updateGradleWorkerClasspathFile(List<String> command, int argIndex, Path directory) {
    String originalClasspathFileArg = command.get(argIndex);
    Matcher matcher = GRADLE_WORKER_CLASSPATH_FILE_PATTERN
        .matcher(originalClasspathFileArg);
    matcher.matches();
    Path originalClasspathFile = Paths.get(matcher.group().substring(1));
    Path newClasspathFile = directory.resolve("gradle-worker-classpath.txt");
    copy(originalClasspathFile, newClasspathFile);
    String newClasspathFileArg = "@" + newClasspathFile;
    command.set(argIndex, newClasspathFileArg);
  }

  private static void copy(Path source, Path dest) {
    try {
      Files.copy(source, dest);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static OptionalInt findGradleWorkerClasspathArg(List<String> command) {
    return IntStream.range(0, command.size())
        .filter(i -> GRADLE_WORKER_CLASSPATH_FILE_PATTERN.matcher(command.get(i)).matches())
        .findFirst();
  }
}
