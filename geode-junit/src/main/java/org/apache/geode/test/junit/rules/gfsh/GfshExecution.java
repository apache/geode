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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.geode.internal.process.ProcessUtils.readPid;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.test.junit.rules.gfsh.internal.ProcessLogger;

public class GfshExecution {
  private static final String DOUBLE_QUOTE = "\"";
  private static final String SCRIPT_TIMEOUT_FAILURE_MESSAGE =
      "Process started by [%s] did not exit after %s %s";
  private static final String SCRIPT_EXIT_VALUE_DESCRIPTION =
      "Exit value from process started by [%s]";

  private final Process process;
  private final File workingDir;
  private final ProcessLogger processLogger;

  protected GfshExecution(Process process, File workingDir) {
    this.process = process;
    this.workingDir = workingDir.getAbsoluteFile();

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
    return this.process;
  }

  public List<File> getServerDirs() {
    File[] potentialMemberDirectories = workingDir.listFiles(File::isDirectory);

    Predicate<File> isServerDir = (File directory) -> Arrays.stream(directory.list())
        .anyMatch(filename -> filename.endsWith("server.pid"));

    return Arrays.stream(potentialMemberDirectories).filter(isServerDir)
        .collect(toList());
  }

  public List<File> getLocatorDirs() {
    File[] potentialMemberDirectories = workingDir.listFiles(File::isDirectory);

    Predicate<File> isLocatorDir = (File directory) -> Arrays.stream(directory.list())
        .anyMatch(filename -> filename.endsWith("locator.pid"));

    return Arrays.stream(potentialMemberDirectories).filter(isLocatorDir)
        .collect(toList());
  }

  public Iterable<Path> getLocatorPidFiles(ProcessType processType) throws IOException {
    return Files.list(workingDir.toPath())
        .filter(Files::isDirectory)
        .map(path -> path.resolve(processType.getPidFileName()))
        .filter(Files::exists)
        .collect(toSet());
  }

  public void printLogFiles() {
    System.out
        .println("Printing contents of all log files found in " + workingDir.getAbsolutePath());
    List<File> logFiles = findLogFiles();

    for (File logFile : logFiles) {
      System.out.println("Contents of " + logFile.getAbsolutePath());
      try (BufferedReader br =
          new BufferedReader(new InputStreamReader(new FileInputStream(logFile)))) {
        String line;
        while ((line = br.readLine()) != null) {
          System.out.println(line);
        }
      } catch (IOException ignored) {
        System.out.println("Unable to print log due to: " + ExceptionUtils.getStackTrace(ignored));
      }
    }
  }

  private List<File> findLogFiles() {
    List<File> servers = getServerDirs();
    List<File> locators = getLocatorDirs();

    return Stream.concat(servers.stream(), locators.stream()).flatMap(this::findLogFiles)
        .collect(toList());
  }

  private Stream<File> findLogFiles(File memberDir) {
    return Arrays.stream(memberDir.listFiles()).filter(File::isFile)
        .filter(file -> file.getName().toLowerCase().endsWith(".log"));
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
      boolean exited = false;
      try {
        exited = process.waitFor(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      if (!exited) {
        throw new RuntimeException("failed to destroy the process of " + workingDir.getName());
      }
    }
  }

  String[] getStopMemberCommands() {
    Stream<String> stopServers =
        getServerDirs().stream().map(dir -> getStopServerCommand(dir.toString()));
    Stream<String> stopLocators =
        getLocatorDirs().stream().map(dir -> getStopLocatorCommand(dir.toString()));
    return Stream.concat(stopServers, stopLocators).toArray(String[]::new);
  }

  String getStopServerCommand(String serverName) {
    return "stop server --dir=" + getSubDir(serverName);
  }

  String getStopLocatorCommand(String locatorName) {
    return "stop locator --dir=" + getSubDir(locatorName);
  }

  Path getPidFilePath(String processName, ProcessType processType) {
    return getSubDir(processName).resolve(processType.getPidFileName());
  }

  int getPid(String processName, ProcessType processType) throws IOException {
    Path pidFilePath = getPidFilePath(processName, processType);
    return readPid(pidFilePath.toFile());
  }

  private Path getSubDir(String subDirName) {
    return getWorkingDir().toPath().resolve(subDirName);
  }

  private String quoteArgument(String argument) {
    if (!argument.startsWith(DOUBLE_QUOTE)) {
      argument = DOUBLE_QUOTE + argument;
    }

    if (!argument.endsWith(DOUBLE_QUOTE)) {
      argument = argument + DOUBLE_QUOTE;
    }

    return argument;
  }
}
