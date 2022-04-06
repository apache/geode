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
import static java.util.stream.Collectors.toSet;
import static org.apache.geode.internal.process.ProcessType.LOCATOR;
import static org.apache.geode.internal.process.ProcessType.SERVER;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder.When.FAILS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.ProcessUtils;
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
 *
 * TODO:
 * - change top-level dir for saved test results to use test class name
 * - remove stopLocator/stopServer
 * - kill any locator/server processes that are still alive
 */
public class GfshRule implements TestRule {
  private final SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder()
      .when(FAILS)
      .copyTo(new File("."));

  private final List<Throwable> errors = new ArrayList<>();

  private List<GfshExecution> gfshExecutions;
  private Path gfsh;
  private String version;

  public GfshRule() {
    // nothing
  }

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
        } catch (Throwable e) {
          errors.add(e);
        } finally {
          after();
        }

        MultipleFailureException.assertEmpty(errors);
      }
    };
  }

  private void before(String methodName) throws Throwable {
    gfsh = findGfsh();
    assertThat(gfsh).exists();

    gfshExecutions = Collections.synchronizedList(new ArrayList<>());
    temporaryFolder.before(methodName);
  }

  /**
   * Attempts to stop any started servers/locators via pid file and tears down any remaining gfsh
   * JVMs.
   */
  private void after() {
    // Copy the gfshExecutions list because stopMembers will add more executions
    // This would not include the "stopMemberQuietly" executions
    Iterable<GfshExecution> executionsWithMembersToStop = new ArrayList<>(gfshExecutions);
    executionsWithMembersToStop.forEach(this::stopMembers);

    // This will include the "stopMemberQuietly" executions
    try {
      gfshExecutions.forEach(GfshExecution::killProcess);
    } finally {
      temporaryFolder.after();
    }
  }

  public TemporaryFolder getTemporaryFolder() {
    return temporaryFolder;
  }

  public Path getGfshPath() {
    return gfsh;
  }

  public GfshExecution execute(String... commands) {
    try {
      return execute(GfshScript.of(commands));
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
  GfshExecution execute(GfshScript gfshScript, File workingDir)
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    System.out.println("Executing " + gfshScript);

    int debugPort = gfshScript.getDebugPort();
    Process process = createProcessBuilder(gfshScript, gfsh, workingDir, debugPort).start();
    GfshExecution gfshExecution = new GfshExecution(process, workingDir);
    gfshExecutions.add(gfshExecution);
    gfshExecution.awaitTermination(gfshScript);
    return gfshExecution;
  }

  private Path findGfsh() {
    Path geodeHome;
    if (version == null) {
      geodeHome = new RequiresGeodeHome().getGeodeHome().toPath();
    } else {
      geodeHome = Paths.get(VersionManager.getInstance().getInstall(version));
    }

    if (SystemUtils.isWindows()) {
      return geodeHome.resolve("bin").resolve("gfsh.bat");
    }
    return geodeHome.resolve("bin").resolve("gfsh");
  }

  private ProcessBuilder createProcessBuilder(GfshScript gfshScript, Path gfshPath, File workingDir,
      int gfshDebugPort) {
    List<String> commandsToExecute = new ArrayList<>();

    if (SystemUtils.isWindows()) {
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

  private void stopMembers(GfshExecution gfshExecution) throws UncheckedIOException {
    Set<Integer> serverPids = getPidFilesUnchecked(gfshExecution, SERVER).stream()
        .map(pidFile -> ProcessUtils.readPid(pidFile.toFile()))
        .collect(toSet());

    getPidFilesUnchecked(gfshExecution, SERVER)
        .forEach(pidFile -> stopProcessSoftly(SERVER, pidFile.getParent()));

    Set<Integer> locatorPids = getPidFilesUnchecked(gfshExecution, LOCATOR).stream()
        .map(pidFile -> ProcessUtils.readPid(pidFile.toFile()))
        .collect(toSet());

    getPidFilesUnchecked(gfshExecution, LOCATOR)
        .forEach(pidFile -> stopProcessSoftly(LOCATOR, pidFile.getParent()));

    Set<Integer> runningServerPids = serverPids.stream()
        .filter(this::awaitProcess)
        .collect(toSet());

    Set<Integer> runningLocatorPids = locatorPids.stream()
        .filter(this::awaitProcess)
        .collect(toSet());

    // try to kill all running pids
    Set<Integer> allRunningPids = new HashSet<>();
    allRunningPids.addAll(runningServerPids);
    allRunningPids.addAll(runningLocatorPids);

    allRunningPids.forEach(this::killProcess);
  }

  /**
   * Wits for the process to go away. Returns true if it fails to depart within the timeout.
   */
  private boolean awaitProcess(int pid) {
    try {
      await()
          .alias("Awaiting termination of Process " + pid + ".")
          .until(() -> !isProcessAlive(pid));
    } catch (ConditionTimeoutException e) {
      errors.add(e);
    }
    return isProcessAlive(pid);
  }

  private boolean killProcess(int pid) {
    try {
      return ProcessUtils.killProcess(pid);
    } catch (Exception e) {
      errors.add(e);
    }
    return false;
  }

  private Set<Path> getPidFilesUnchecked(GfshExecution gfshExecution, ProcessType processType)
      throws UncheckedIOException {
    try {
      return gfshExecution.getPidFiles(processType);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void stopProcessSoftly(ProcessType processType, Path directory) {
    try {
      execute(getStopProcessCommand(processType, directory));
    } catch (Exception e) {
      errors.add(e);
    }
  }

  String getStopProcessCommand(ProcessType processType, Path directory) {
    StringBuilder sb = new StringBuilder("stop ");
    if (processType == SERVER) {
      sb.append("server ");
    } else {
      sb.append("locator ");
    }
    sb.append("--dir=");
    sb.append(directory.toFile());
    return sb.toString();
  }

  /**
   * this will stop the server that's been started in this gfsh execution
   */
  public void stopServer(GfshExecution execution, String serverName) {
    String command = // execution.getStopServerCommand(serverName);
        getStopProcessCommand(SERVER, execution.getSubDir(serverName));
    try {
      execute(GfshScript.of(command).withName("Stop-server-" + serverName));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * this will stop the locator that's been started in this gfsh execution
   */
  public void stopLocator(GfshExecution execution, String locatorName) {
    String command = // execution.getStopLocatorCommand(locatorName);
        getStopProcessCommand(LOCATOR, execution.getSubDir(locatorName));
    try {
      execute(GfshScript.of(command).withName("Stop-locator-" + locatorName));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
