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

import static java.io.File.pathSeparator;
import static java.util.Collections.synchronizedList;
import static org.apache.geode.internal.lang.SystemUtils.isWindows;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.version.VersionManager;

/**
 * The {@code GfshRule} allows a test to execute Gfsh commands via the actual (fully-assembled) gfsh
 * binaries. Each call to {@link GfshRule#execute(GfshScript)} will invoke the given gfsh script in
 * a forked JVM. The {@link GfshRule#after()} method will attempt to clean up all forked JVMs.
 *
 * <p>
 * If you want to debug into the gfsh or the locator/servers started using this rule, you can do:
 *
 * <pre>
 * GfshScript.of("start locator", 30000).and("start server", 30001).withDebugPort(30002).execute
 * </pre>
 *
 * This will set the gfsh to be debuggable at port 30002, and the locator started to be debuggable
 * at port 30000, and the server to be debuggable at 30001
 */
public class GfshRule extends ExternalResource {

  private final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private List<GfshExecution> gfshExecutions;
  private Path gfsh;
  private final String version;

  public GfshRule() {
    this(null);
  }

  public GfshRule(String version) {
    this.version = version;
  }

  @Override
  protected void before() throws IOException {
    gfsh = findGfsh();
    assertThat(gfsh).exists();

    gfshExecutions = synchronizedList(new ArrayList<>());
    temporaryFolder.create();
  }

  /**
   * Attempts to stop any started servers/locators via pid file and tears down any remaining gfsh
   * JVMs.
   */
  @Override
  protected void after() {
    // Copy the gfshExecutions list because stopMembers will add more executions
    // This would not include the "stopMemberQuietly" executions
    ((Iterable<GfshExecution>) new ArrayList<>(gfshExecutions))
        .forEach(this::stopMembers);

    // This will include the "stopMemberQuietly" executions
    try {
      gfshExecutions
          .forEach(GfshExecution::killProcess);
    } finally {
      temporaryFolder.delete();
    }
  }

  private Path findGfsh() {
    Path geodeHome;
    if (version == null) {
      geodeHome = new RequiresGeodeHome().getGeodeHome().toPath();
    } else {
      geodeHome = Paths.get(VersionManager.getInstance().getInstall(version));
    }

    if (isWindows()) {
      return geodeHome.resolve("bin/gfsh.bat");
    }
    return geodeHome.resolve("bin/gfsh");
  }

  public TemporaryFolder getTemporaryFolder() {
    return temporaryFolder;
  }

  public Path getGfshPath() {
    return gfsh;
  }

  public GfshExecution execute(String... commands) {
    return execute(GfshScript.of(commands));
  }

  public GfshExecution execute(File workingDir, String... commands) {
    return execute(GfshScript.of(commands), workingDir);
  }

  /**
   * this will allow you to specify a gfsh workingDir when executing the script
   * this is usually helpful if:
   * 1. you would start a different gfsh session but would
   * like to remain in the same working dir as your previous one, (You can get your gfsh session's
   * working dir by using GfshExecution.getWorkingDir)
   * 2. you already prepared the workingdir with some initial setup.
   *
   * This way, this workingDir will be managed by the gfshRule and stop all the processes that
   * exists in this working dir when tests finish
   */
  public GfshExecution execute(GfshScript gfshScript, File workingDir) {
    System.out.println("Executing " + gfshScript);
    try {
      int debugPort = gfshScript.getDebugPort();
      Process process = toProcessBuilder(gfshScript, gfsh, workingDir, debugPort).start();
      GfshExecution gfshExecution = new GfshExecution(process, workingDir);
      gfshExecutions.add(gfshExecution);
      gfshExecution.awaitTermination(gfshScript);
      return gfshExecution;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public GfshExecution execute(GfshScript gfshScript) {
    try {
      return execute(gfshScript, temporaryFolder.getRoot());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ProcessBuilder toProcessBuilder(GfshScript gfshScript, Path gfshPath, File workingDir,
      int gfshDebugPort) {
    List<String> commandsToExecute = new ArrayList<>();

    if (isWindows()) {
      commandsToExecute.add("cmd.exe");
      commandsToExecute.add("/c");
    }
    commandsToExecute.add(gfshPath.toAbsolutePath().toString());

    for (DebuggableCommand command : gfshScript.getCommands()) {
      if (command.debugPort > 0) {
        commandsToExecute.add("-e " + command.command
            + " --J='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address="
            + command.debugPort + "'");
      } else {
        commandsToExecute.add("-e " + command.command);
      }
    }

    ProcessBuilder processBuilder = new ProcessBuilder(commandsToExecute);
    processBuilder.directory(workingDir);

    List<String> extendedClasspath = gfshScript.getExtendedClasspath();
    Map<String, String> environmentMap = processBuilder.environment();
    if (!extendedClasspath.isEmpty()) {
      String classpathKey = "CLASSPATH";
      String existingJavaArgs = environmentMap.get(classpathKey);
      String specified = String.join(pathSeparator, extendedClasspath);
      String newValue =
          String.format("%s%s", existingJavaArgs == null ? "" : existingJavaArgs + pathSeparator,
              specified);
      environmentMap.put(classpathKey, newValue);
    }
    if (gfshDebugPort > 0) {
      environmentMap.put("JAVA_ARGS",
          "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=" + gfshDebugPort);
    }

    return processBuilder;
  }

  /**
   * this will stop the server that's been started in this gfsh execution
   */
  public void stopServer(GfshExecution execution, String serverName) {
    String command = "stop server --dir="
        + execution.getWorkingDir().toPath().resolve(serverName).toAbsolutePath();
    execute(GfshScript.of(command).withName("Stop-server-" + serverName));
  }

  /**
   * this will stop the locator that's been started in this gfsh execution
   */
  public void stopLocator(GfshExecution execution, String locatorName) {
    String command = "stop locator --dir="
        + execution.getWorkingDir().toPath().resolve(locatorName).toAbsolutePath();
    execute(GfshScript.of(command).withName("Stop-locator-" + locatorName));
  }

  private void stopMembers(GfshExecution gfshExecution) {
    String[] stopMemberScripts = gfshExecution.getStopMemberCommands();
    if (stopMemberScripts.length == 0) {
      return;
    }
    execute(GfshScript.of(stopMemberScripts).withName("Stop-Members"));
  }

  public static String startServerCommand(String name, String hostname, int port,
      int connectedLocatorPort) {
    String command = "start server --name=" + name
        + " --server-port=" + port
        + " --locators=" + hostname + "[" + connectedLocatorPort + "]";
    return command;
  }

  public static String startLocatorCommand(String name, String hostname, int port, int jmxPort,
      int httpPort,
      int connectedLocatorPort) {
    String command = "start locator --name=" + name
        + " --port=" + port
        + " --http-service-port=" + httpPort;
    if (connectedLocatorPort > 0) {
      command += " --locators=" + hostname + "[" + connectedLocatorPort + "]";
    }
    command += " --J=-Dgemfire.jmx-manager-port=" + jmxPort;
    return command;
  }
}
