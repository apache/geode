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

import static org.apache.commons.lang.SystemUtils.PATH_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

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
public class GfshRule extends ExternalResource {
  private TemporaryFolder temporaryFolder = new TemporaryFolder();
  private List<GfshExecution> gfshExecutions;
  private Path gfsh;
  private String version;

  public GfshRule() {}

  public GfshRule(String version) {
    this.version = version;
  }

  @Override
  protected void before() throws IOException {
    gfsh = findGfsh();
    assertThat(gfsh).exists();

    gfshExecutions = new ArrayList<>();
    temporaryFolder.create();
  }

  /**
   * Attempts to stop any started servers/locators via pid file and tears down any remaining gfsh
   * JVMs.
   */
  @Override
  protected void after() {
    // this would not include the "stopMemberQuietly" executions
    gfshExecutions.stream().collect(Collectors.toList()).forEach(this::stopMembers);

    // this will include the "stopMemberQuietly" executions
    try {
      gfshExecutions.stream().forEach(gfshExecution -> gfshExecution.killProcess());
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

  public GfshExecution execute(GfshScript gfshScript) {
    GfshExecution gfshExecution;
    System.out.println("Executing " + gfshScript);
    try {
      File workingDir = new File(temporaryFolder.getRoot(), gfshScript.getName());
      workingDir.mkdirs();
      Process process =
          toProcessBuilder(gfshScript, gfsh, workingDir, gfshScript.getDebugPort()).start();
      gfshExecution = new GfshExecution(process, workingDir);
      gfshExecutions.add(gfshExecution);
      gfshExecution.awaitTermination(gfshScript);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return gfshExecution;
  }

  protected ProcessBuilder toProcessBuilder(GfshScript gfshScript, Path gfshPath, File workingDir,
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
      String specified = String.join(PATH_SEPARATOR, extendedClasspath);
      String newValue =
          String.format("%s%s", existingJavaArgs == null ? "" : existingJavaArgs + PATH_SEPARATOR,
              specified);
      environmentMap.put(classpathKey, newValue);
    }
    if (gfshDebugPort > 0) {
      environmentMap.put("JAVA_ARGS",
          "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=" + gfshDebugPort);
    }

    return processBuilder;
  }

  private void stopMembers(GfshExecution gfshExecution) {
    String[] stopMemberScripts = gfshExecution.getStopMemberCommands();
    if (stopMemberScripts.length == 0) {
      return;
    }
    execute(GfshScript.of(stopMemberScripts).withName("Stop-Members"));
  }
}
