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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Charsets;
import org.apache.commons.lang.exception.ExceptionUtils;

import org.apache.geode.test.junit.rules.gfsh.internal.ProcessLogger;

public class GfshExecution {
  private final Process process;
  private final File workingDir;
  private final ProcessLogger processLogger;

  protected GfshExecution(Process process, File workingDir) {
    this.process = process;
    this.workingDir = workingDir;
    this.processLogger = new ProcessLogger(process, workingDir.getName());
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
        .collect(Collectors.toList());
  }

  public List<File> getLocatorDirs() {
    File[] potentialMemberDirectories = workingDir.listFiles(File::isDirectory);

    Predicate<File> isLocatorDir = (File directory) -> Arrays.stream(directory.list())
        .anyMatch(filename -> filename.endsWith("locator.pid"));

    return Arrays.stream(potentialMemberDirectories).filter(isLocatorDir)
        .collect(Collectors.toList());
  }

  public void printLogFiles() {
    System.out
        .println("Printing contents of all log files found in " + workingDir.getAbsolutePath());
    List<File> logFiles = findLogFiles();

    for (File logFile : logFiles) {
      System.out.println("Contents of " + logFile.getAbsolutePath());
      try (BufferedReader br =
          new BufferedReader(new InputStreamReader(new FileInputStream(logFile), Charsets.UTF_8))) {
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
        .collect(Collectors.toList());
  }

  private Stream<File> findLogFiles(File memberDir) {
    return Arrays.stream(memberDir.listFiles()).filter(File::isFile)
        .filter(file -> file.getName().toLowerCase().endsWith(".log"));
  }
}
