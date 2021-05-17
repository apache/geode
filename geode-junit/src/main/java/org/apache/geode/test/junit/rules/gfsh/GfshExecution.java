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

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.geode.test.junit.rules.gfsh.internal.ProcessLogger;

public class GfshExecution {

  private static final String DOUBLE_QUOTE = "\"";
  private static final String SCRIPT_TIMEOUT_FAILURE_MESSAGE =
      "Process started by [%s] did not exit after %s %s";
  private static final String SCRIPT_EXIT_VALUE_DESCRIPTION =
      "Exit value from process started by [%s]";

  private static final Predicate<File> IS_RUNNING_SERVER_DIRECTORY =
      directory -> stream(directory.list())
          .anyMatch(filename -> filename.endsWith("server.pid"));

  private static final Predicate<File> IS_SERVER_DIRECTORY = directory -> stream(directory.list())
      .anyMatch(filename -> filename.endsWith("server.pid") || filename.contains("server.status"));

  private static final Predicate<File> IS_RUNNING_LOCATOR_DIRECTORY =
      directory -> stream(directory.list())
          .anyMatch(filename -> filename.endsWith("locator.pid"));

  private static final Predicate<File> IS_LOCATOR_DIRECTORY = directory -> stream(directory.list())
      .anyMatch(
          filename -> filename.endsWith("locator.pid") || filename.contains("locator.status"));

  private final Process process;
  private final File workingDir;
  private final ProcessLogger processLogger;

  protected GfshExecution(Process process, File workingDir) {
    this.process = process;
    this.workingDir = workingDir;

    processLogger = new ProcessLogger(process, workingDir.getName());
    processLogger.start();
  }

  public String getOutputText() {
    return processLogger.getOutputText();
  }

  /**
   * Note that this is the working directory of gfsh itself. If your script started a server or
   * locator, this will be the parent directory of that member's working directory.
   */
  public File getWorkingDir() {
    return workingDir;
  }

  public Process getProcess() {
    return process;
  }

  void awaitTermination(GfshScript script)
      throws InterruptedException, TimeoutException, ExecutionException {
    boolean exited = process.waitFor(script.getTimeout(), script.getTimeoutTimeUnit());

    try {
      assertThat(exited)
          .withFailMessage(SCRIPT_TIMEOUT_FAILURE_MESSAGE, script, script.getTimeout(),
              script.getTimeoutTimeUnit())
          .isTrue();
      assertThat(process.exitValue())
          .as(SCRIPT_EXIT_VALUE_DESCRIPTION, script)
          .isEqualTo(script.getExpectedExitValue());
    } catch (AssertionError error) {
      printLogFiles();
      throw error;
    } finally {
      processLogger.awaitTermination(script.getTimeout(), script.getTimeoutTimeUnit());
      processLogger.close();
    }
  }

  /**
   * this only kills the process of "gfsh -e command", it does not kill the child processes started
   * by this command.
   */
  void killProcess() {
    process.destroyForcibly();
    if (process.isAlive()) {
      // process may not terminate immediately after destroyForcibly
      boolean exited;
      try {
        exited = process.waitFor(getTimeout().toMinutes(), MINUTES);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (!exited) {
        throw new RuntimeException("failed to destroy the process of " + workingDir.getName());
      }
    }
  }

  String[] getStopMemberCommands() {
    Stream<String> stopServers = getServerDirs(true)
        .stream()
        .map(f -> "stop server --dir=" + quoteArgument(f.toString()));
    Stream<String> stopLocators = getLocatorDirs(true)
        .stream()
        .map(f -> "stop locator --dir=" + quoteArgument(f.toString()));
    return concat(stopServers, stopLocators)
        .toArray(String[]::new);
  }

  private void printLogFiles() {
    System.out.println(
        "Printing contents of all log files found in " + workingDir.getAbsolutePath());
    List<File> logFiles = findLogFiles();

    for (File logFile : logFiles) {
      System.out.println("Contents of " + logFile.getAbsolutePath());
      try (BufferedReader br = new BufferedReader(new InputStreamReader(
          new FileInputStream(logFile), Charset.defaultCharset()))) {
        String line;
        while ((line = br.readLine()) != null) {
          System.out.println(line);
        }
      } catch (IOException e) {
        System.out.println("Unable to print log due to: " + getStackTrace(e));
      }
    }
  }

  private List<File> getServerDirs(boolean isRunning) {
    Predicate<File> predicate = isRunning ? IS_RUNNING_SERVER_DIRECTORY : IS_SERVER_DIRECTORY;
    return findPotentialMemberDirectories()
        .stream()
        .filter(predicate)
        .collect(toList());
  }

  private List<File> getLocatorDirs(boolean isRunning) {
    Predicate<File> predicate = isRunning ? IS_RUNNING_LOCATOR_DIRECTORY : IS_LOCATOR_DIRECTORY;
    return findPotentialMemberDirectories()
        .stream()
        .filter(predicate)
        .collect(toList());
  }

  private List<File> findPotentialMemberDirectories() {
    File[] directories = workingDir.listFiles(File::isDirectory);

    assertThat(directories)
        .as("List of directories under " + workingDir.getAbsolutePath())
        .isNotNull();

    List<File> potentialMemberDirectories = new ArrayList<>(asList(directories));
    potentialMemberDirectories.add(workingDir);

    return potentialMemberDirectories;
  }

  private List<File> findLogFiles() {
    List<File> servers = getServerDirs(false);
    List<File> locators = getLocatorDirs(false);

    return concat(servers.stream(), locators.stream())
        .flatMap(GfshExecution::findLogFiles)
        .collect(toList());
  }

  private static Stream<File> findLogFiles(File memberDir) {
    return stream(memberDir.listFiles())
        .filter(File::isFile)
        .filter(file -> file.getName().toLowerCase().endsWith(".log"));
  }

  private static String quoteArgument(String argument) {
    if (!argument.startsWith(DOUBLE_QUOTE)) {
      argument = DOUBLE_QUOTE + argument;
    }
    if (!argument.endsWith(DOUBLE_QUOTE)) {
      argument = argument + DOUBLE_QUOTE;
    }
    return argument;
  }
}
