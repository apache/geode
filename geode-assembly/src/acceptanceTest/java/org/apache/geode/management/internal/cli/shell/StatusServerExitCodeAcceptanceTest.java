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
package org.apache.geode.management.internal.cli.shell;

import static java.lang.System.lineSeparator;
import static java.util.Arrays.stream;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.management.internal.cli.shell.StatusServerExitCodeAcceptanceTest.DirectoryTree.printDirectoryTree;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.process.PidFile;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

/**
 * See also org.apache.geode.management.internal.cli.shell.StatusLocatorExitCodeAcceptanceTest
 */
public class StatusServerExitCodeAcceptanceTest {

  private static final String LOCATOR_NAME = "myLocator";
  private static final String SERVER_NAME = "myServer";

  private static int locatorPort;
  private static Path toolsJar;
  private static int serverPid;
  private static Path serverDir;
  private static Path rootPath;
  private static String connectCommand;

  @ClassRule
  public static GfshRule gfshRule = new GfshRule();

  @BeforeClass
  public static void startCluster() throws IOException {
    rootPath = gfshRule.getTemporaryFolder().getRoot().toPath();
    locatorPort = getRandomAvailablePort(SOCKET);

    GfshExecution execution = GfshScript.of(
        "start locator --name=" + LOCATOR_NAME + " --port=" + locatorPort,
        "start server --disable-default-server --name=" + SERVER_NAME)
        .execute(gfshRule);

    assertThat(execution.getProcess().exitValue())
        .isZero();

    serverPid = readPidFile(SERVER_NAME, "server.pid");
    serverDir = rootPath.resolve(SERVER_NAME).toAbsolutePath();

    connectCommand = "connect --locator=[" + locatorPort + "]";
  }

  @BeforeClass
  public static void setUpJavaTools() {
    String javaHome = System.getProperty("java.home");
    assertThat(javaHome)
        .as("System.getProperty(\"java.home\")")
        .isNotNull();

    Path javaHomeFile = new File(javaHome).toPath();
    assertThat(javaHomeFile).exists();

    System.out.println(printDirectoryTree(javaHomeFile.toFile()));

    String toolsPath = javaHomeFile.toFile().getName().equalsIgnoreCase("jre")
        ? ".." + File.separator + "lib" + File.separator + "tools.jar"
        : "lib" + File.separator + "tools.jar";
    toolsJar = javaHomeFile.resolve(toolsPath);
    assertThat(toolsJar).exists();
  }

  @Test
  public void statusCommandWithInvalidOptionValueShouldFail() {
    String commandWithBadPid = "status server --pid=-1";

    GfshScript.of(commandWithBadPid)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectDirShouldFail() {
    String commandWithWrongDir = "status server --dir=.";

    GfshScript.of(commandWithWrongDir)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectNameShouldFail() {
    String commandWithWrongName = "status server --name=some-server-name";

    GfshScript.of(commandWithWrongName)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectPidShouldFail() {
    String commandWithWrongPid = "status server --pid=100";

    GfshScript.of(commandWithWrongPid)
        .withName("test-frame")
        .addToClasspath(toolsJar.toFile().getAbsolutePath())
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void onlineStatusCommandShouldFailWhenNotConnected_server_name() {
    String statusCommand = "status server --name=" + SERVER_NAME;

    GfshScript.of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_server_name() {
    String statusCommand = "status server --name=" + SERVER_NAME;

    GfshScript.of(connectCommand, statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_server_dir() {
    String statusCommand = "status server --dir=" + serverDir;

    GfshScript.of(connectCommand, statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_server_pid() {
    String statusCommand = "status server --pid=" + serverPid;

    GfshScript.of(connectCommand, statusCommand)
        .withName("test-frame")
        .addToClasspath(toolsJar.toFile().getAbsolutePath())
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_server_dir() {
    String statusCommand = "status server --dir=" + serverDir;

    GfshScript.of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_server_pid() {
    String statusCommand = "status server --pid=" + serverPid;

    GfshScript.of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .addToClasspath(toolsJar.toFile().getAbsolutePath())
        .execute(gfshRule);
  }

  private static int readPidFile(String memberName, String pidFileEndsWith) throws IOException {
    File directory = rootPath.resolve(memberName).toFile();
    File[] files = directory.listFiles();

    assertThat(files)
        .as(String.format("Expected directory ('%s') for member '%s'.", directory, memberName))
        .isNotNull();

    File pidFile = stream(files)
        .filter(file -> file.getName().endsWith(pidFileEndsWith))
        .findFirst()
        .orElseThrow(() -> new RuntimeException(String
            .format("Expected member '%s' to have pid file but could not find it.", memberName)));

    return new PidFile(pidFile).readPid();
  }

  public static class DirectoryTree {

    /**
     * Pretty print the directory tree and its file names.
     */
    public static String printDirectoryTree(File folder) {
      if (!folder.isDirectory()) {
        throw new IllegalArgumentException("folder is not a Directory");
      }
      int indent = 0;
      StringBuilder sb = new StringBuilder();
      printDirectoryTree(folder, indent, sb);
      return sb.toString();
    }

    private static void printDirectoryTree(File folder, int indent, StringBuilder sb) {
      if (!folder.isDirectory()) {
        throw new IllegalArgumentException("folder is not a Directory");
      }
      sb.append(getIndentString(indent));
      sb.append("+--");
      sb.append(folder.getName());
      sb.append("/");
      sb.append(lineSeparator());
      for (File file : folder.listFiles()) {
        if (file.isDirectory()) {
          printDirectoryTree(file, indent + 1, sb);
        } else {
          printFile(file, indent + 1, sb);
        }
      }
    }

    private static void printFile(File file, int indent, StringBuilder sb) {
      sb.append(getIndentString(indent));
      sb.append("+--");
      sb.append(file.getName());
      sb.append(lineSeparator());
    }

    private static String getIndentString(int indent) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < indent; i++) {
        sb.append("|  ");
      }
      return sb.toString();
    }
  }
}
