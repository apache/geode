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
import static org.apache.geode.internal.process.ProcessType.LOCATOR;
import static org.apache.geode.internal.process.ProcessType.SERVER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder.When.ALWAYS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.version.VersionManager;

/**
 * The {@code GfshRule} allows a test to execute Gfsh commands via the actual (fully-assembled) gfsh
 * binaries. Each call to {@link GfshRule#execute(GfshScript)} will invoke the given gfsh script in
 * a forked JVM. The {@link GfshRule#after()} method will attempt to clean up all forked JVMs.
 *
 * if you want to debug into the gfsh or the locator/servers started using this rule, you can do:
 * GfshScript.of("start locator", 30000).and("start server", 30001).withDebugPort(30002).execute
 *
 * this will set the gfsh to be debuggable at port 30002, and the locator started to be debuggable
 * at port 30000, and the server to be debuggable at 30001
 */
public class GfshRule implements TestRule {
  private final SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
      .when(ALWAYS)
      .copyTo(new File("."));
  private List<GfshExecution> gfshExecutions;
  private Path gfsh;
  private String version;

  public GfshRule() {}

  public GfshRule(String version) {
    this.version = version;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before(description.getMethodName());
        try {
          base.evaluate();
        } finally {
          after();
        }
      }
    };
  }

  protected void before(String methodName) throws Throwable {
    gfsh = findGfsh();
    assertThat(gfsh).exists();

    gfshExecutions = Collections.synchronizedList(new ArrayList<>());
    temporaryFolder.before(methodName);
  }

  /**
   * Attempts to stop any started servers/locators via pid file and tears down any remaining gfsh
   * JVMs.
   */
  protected void after() {
    // Copy the gfshExecutions list because stopMembers will add more executions
    // This would not include the "stopMemberQuietly" executions
    ArrayList<GfshExecution> executionsWithMembersToStop = new ArrayList<>(gfshExecutions);
    executionsWithMembersToStop.forEach(this::stopMembers);

    // This will include the "stopMemberQuietly" executions
    try {
      gfshExecutions.forEach(GfshExecution::killProcess);
    } finally {
      temporaryFolder.after();
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
    } else {
      return geodeHome.resolve("bin/gfsh");
    }
  }

  private boolean isWindows() {
    return System.getProperty("os.name").toLowerCase().contains("win");
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
      File workingDir = new File(temporaryFolder.getRoot(), gfshScript.getName());
      workingDir.mkdirs();
      return execute(gfshScript, workingDir);
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
    Path serverWorkingDir =
        execution.getWorkingDir().toPath().resolve(serverName).toAbsolutePath();
    String command = "stop server --dir=" + serverWorkingDir;
    execute(GfshScript.of(command).withName("Stop-server-" + serverName));

    Path serverPidFile = serverWorkingDir.resolve(SERVER.getPidFileName());
    assertThat(serverPidFile).doesNotExist();
    await().untilAsserted(() -> assertThat(serverPidFile).doesNotExist());
  }

  /**
   * this will stop the lcoator that's been started in this gfsh execution
   */
  public void stopLocator(GfshExecution execution, String locatorName) {
    Path locatorWorkingDir =
        execution.getWorkingDir().toPath().resolve(locatorName).toAbsolutePath();
    String command = "stop locator --dir=" + locatorWorkingDir;
    execute(GfshScript.of(command).withName("Stop-locator-" + locatorName));

    Path locatorPidFile = locatorWorkingDir.resolve(LOCATOR.getPidFileName());
    assertThat(locatorPidFile).doesNotExist();
    await().untilAsserted(() -> assertThat(locatorPidFile).doesNotExist());
  }

  private void stopMembers(GfshExecution gfshExecution) {
    String[] stopMemberScripts = gfshExecution.getStopMemberCommands();
    if (stopMemberScripts.length == 0) {
      return;
    }
    execute(GfshScript.of(stopMemberScripts).withName("Stop-Members"));
  }

  public static String startServerCommand(String name, int port, int connectedLocatorPort) {
    String command = "start server --name=" + name
        + " --server-port=" + port
        + " --locators=localhost[" + connectedLocatorPort + "]";
    return command;
  }

  public static String startLocatorCommand(String name, int port, int jmxPort, int httpPort,
      int connectedLocatorPort) {
    String command = "start locator --name=" + name
        + " --port=" + port
        + " --http-service-port=" + httpPort;
    if (connectedLocatorPort > 0) {
      command += " --locators=localhost[" + connectedLocatorPort + "]";
    }
    command += " --J=-Dgemfire.jmx-manager-port=" + jmxPort;
    return command;
  }
}
