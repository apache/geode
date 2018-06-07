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
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

/**
 * The {@code GfshRule} allows a test to execute Gfsh commands via the actual (fully-assembled) gfsh
 * binaries. Each call to {@link GfshRule#execute(GfshScript)} will invoke the given gfsh script in
 * a forked JVM. The {@link GfshRule#after()} method will attempt to clean up all forked JVMs.
 */
public class GfshRule extends ExternalResource {

  private static final String DOUBLE_QUOTE = "\"";
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

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
    gfshExecutions.stream().collect(Collectors.toList()).forEach(this::stopMembersQuietly);

    final List<String> shutdownExceptions = new ArrayList<>();
    try {
      gfshExecutions.stream().map(GfshExecution::getProcess).map(Process::destroyForcibly)
          .forEach((Process process) -> {
            try {
              // Process.destroyForcibly() may not terminate immediately
              process.waitFor(1, TimeUnit.MINUTES);
            } catch (InterruptedException ie) {
              shutdownExceptions
                  .add(process.toString() + " failed to shutdown: " + ie.getMessage());
            }
          });
      if (!shutdownExceptions.isEmpty()) {
        throw new RuntimeException("gfshExecutions processes failed to shutdown" + LINE_SEPARATOR
            + String.join(LINE_SEPARATOR, shutdownExceptions));
      }
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
    try {
      File workingDir = new File(temporaryFolder.getRoot(), gfshScript.getName());
      workingDir.mkdirs();
      Process process = toProcessBuilder(gfshScript, gfsh, workingDir).start();
      gfshExecution = new GfshExecution(process, workingDir);
      gfshExecutions.add(gfshExecution);
      gfshScript.awaitIfNecessary(gfshExecution);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return gfshExecution;
  }

  protected ProcessBuilder toProcessBuilder(GfshScript gfshScript, Path gfshPath, File workingDir) {
    List<String> commandsToExecute = new ArrayList<>();
    commandsToExecute.add(gfshPath.toAbsolutePath().toString());

    for (String command : gfshScript.getCommands()) {
      commandsToExecute.add("-e " + command);
    }

    ProcessBuilder processBuilder = new ProcessBuilder(commandsToExecute);
    processBuilder.directory(workingDir);

    List<String> extendedClasspath = gfshScript.getExtendedClasspath();
    if (!extendedClasspath.isEmpty()) {
      Map<String, String> environmentMap = processBuilder.environment();
      String classpathKey = "CLASSPATH";
      String existingJavaArgs = environmentMap.get(classpathKey);
      String specified = String.join(PATH_SEPARATOR, extendedClasspath);
      String newValue =
          String.format("%s%s", existingJavaArgs == null ? "" : existingJavaArgs + ":", specified);
      environmentMap.put(classpathKey, newValue);
    }

    return processBuilder;
  }

  private void stopMembersQuietly(GfshExecution gfshExecution) {
    gfshExecution.getServerDirs().forEach(this::stopServerInDir);
    gfshExecution.getLocatorDirs().forEach(this::stopLocatorInDir);
  }

  private void stopServerInDir(File dir) {
    String stopServerCommand = "stop server --dir=" + quoteArgument(dir.toString());

    GfshScript stopServerScript =
        new GfshScript(stopServerCommand).withName("teardown-stop-server").awaitQuietly();
    execute(stopServerScript);
  }

  private void stopLocatorInDir(File dir) {
    String stopLocatorCommand = "stop locator --dir=" + quoteArgument(dir.toString());

    GfshScript stopServerScript =
        new GfshScript(stopLocatorCommand).withName("teardown-stop-locator").awaitQuietly();
    execute(stopServerScript);
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
