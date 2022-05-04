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
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.exists;
import static java.util.Collections.emptySet;
import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toSet;
import static org.apache.geode.internal.lang.SystemUtils.isWindows;
import static org.apache.geode.internal.process.ProcessType.LOCATOR;
import static org.apache.geode.internal.process.ProcessType.SERVER;
import static org.apache.geode.internal.process.ProcessUtils.identifyPidAsUnchecked;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.apache.geode.internal.process.NativeProcessUtils;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.process.ProcessUtilsProvider;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.test.version.VmConfiguration;

public class GfshContext implements GfshExecutor {

  private final List<GfshExecution> gfshExecutions = synchronizedList(new ArrayList<>());
  private final List<String> jvmOptions;

  private final Consumer<Throwable> thrown;
  private final IntConsumer processKiller;

  private final Path javaHome;
  private final Path gfshPath;
  private final Path dir;

  private GfshContext(Builder builder) {
    thrown = builder.thrown;
    javaHome = builder.javaHome;
    gfshPath = builder.gfshPath;
    dir = builder.dir;
    processKiller = builder.processKiller;
    jvmOptions = new ArrayList<>(builder.jvmOptions);
  }

  void killProcesses() {
    // kill all server processes
    getPidFiles(SERVER).stream()
        .map(ProcessUtils::readPid)
        .forEach(this::killProcess);

    // kill all locator processes
    getPidFiles(LOCATOR).stream()
        .map(ProcessUtils::readPid)
        .forEach(this::killProcess);

    // kill all gfsh processes
    gfshExecutions
        .forEach(this::killProcess);
  }

  @Override
  public GfshExecution execute(String... commands) {
    return execute(GfshScript.of(commands));
  }

  @Override
  public GfshExecution execute(GfshScript gfshScript) {
    try {
      Path scriptPath = dir.resolve(gfshScript.getName());
      File workingDir = createDirectories(scriptPath).toFile();
      return execute(workingDir, gfshScript);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public GfshExecution execute(File workingDir, String... commands) {
    return execute(workingDir.toPath(), GfshScript.of(commands));
  }

  @Override
  public GfshExecution execute(File workingDir, GfshScript gfshScript) {
    return execute(workingDir.toPath(), gfshScript);
  }

  @Override
  public GfshExecution execute(Path workingDir, String... commands) {
    return execute(workingDir, GfshScript.of(commands));
  }

  @Override
  public GfshExecution execute(Path workingDir, GfshScript gfshScript) {
    try {
      return doExecute(workingDir, gfshScript);
    } catch (ExecutionException | InterruptedException | IOException | TimeoutException e) {
      throw new AssertionError(e);
    }
  }

  private GfshExecution doExecute(Path workingDir, GfshScript gfshScript)
      throws ExecutionException, InterruptedException, IOException, TimeoutException {
    System.out.println("Executing " + gfshScript);

    Path absoluteWorkingDir = workingDir.toAbsolutePath().normalize();
    Path relativeOutputPath = gfshScript.nextOutputPath();
    Path absoluteOutputPath = absoluteWorkingDir.resolve(relativeOutputPath).normalize();
    int debugPort = gfshScript.getDebugPort();

    ProcessBuilder processBuilder =
        createProcessBuilder(gfshScript, gfshPath, absoluteWorkingDir, absoluteOutputPath,
            debugPort);

    // start the script
    Process process = processBuilder.start();

    // hand a running script to new GfshExecution
    GfshExecution gfshExecution =
        new GfshExecution(this, process, absoluteWorkingDir, absoluteOutputPath);

    // track the gfshExecution in an instance collection
    gfshExecutions.add(gfshExecution);

    // await the completion of the process in GfshExecution
    gfshExecution.awaitTermination(gfshScript);

    return gfshExecution;
  }

  private ProcessBuilder createProcessBuilder(GfshScript gfshScript, Path gfshPath, Path workingDir,
      Path outputPath, int gfshDebugPort) {
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

    ProcessBuilder processBuilder = new ProcessBuilder(commandsToExecute)
        .directory(workingDir.toFile())
        .redirectErrorStream(true)
        .redirectOutput(outputPath.toFile());

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

    Collection<String> javaArgs = new ArrayList<>(jvmOptions);
    if (gfshDebugPort > 0) {
      javaArgs
          .add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=" + gfshDebugPort);
    }
    environmentMap.put("JAVA_ARGS", String.join(" ", javaArgs));

    if (null != javaHome) {
      environmentMap.put("JAVA_HOME", javaHome.toString());
    }

    return processBuilder;
  }

  private void killProcess(GfshExecution execution) {
    try {
      execution.killProcess();
    } catch (Exception e) {
      thrown.accept(e);
    }
  }

  private void killProcess(int pid) {
    assertThat(pid).isNotEqualTo(identifyPidAsUnchecked());
    try {
      processKiller.accept(pid);
      await().untilAsserted(() -> assertThat(isProcessAlive(pid)).isFalse());
    } catch (Exception e) {
      thrown.accept(e);
    }
  }

  private Set<Path> getPidFiles(ProcessType processType)
      throws UncheckedIOException {
    try {
      if (!exists(dir)) {
        return emptySet();
      }
      return Files.walk(dir)
          .filter(Files::isDirectory)
          .map(path -> path.resolve(processType.getPidFileName()))
          .filter(Files::exists)
          .collect(toSet());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static class Builder implements GfshExecutor.Builder {

    private static final ProcessUtilsProvider processUtils = NativeProcessUtils.create();

    private final List<String> jvmOptions = new ArrayList<>();
    private final Consumer<GfshContext> contextCreated;
    private final Consumer<Throwable> thrown;
    private final IntConsumer processKiller = processUtils::killProcess;
    private Path javaHome;
    private Path gfshPath = findGfsh(null);
    private Path dir;

    Builder(Consumer<GfshContext> contextCreated, Consumer<Throwable> thrown) {
      this.contextCreated = contextCreated;
      this.thrown = thrown;
    }

    @Override
    public Builder withJavaHome(Path javaHome) {
      this.javaHome = javaHome;
      return this;
    }

    @Override
    public Builder withGeodeVersion(String geodeVersion) {
      gfshPath = findGfsh(geodeVersion);
      return this;
    }

    @Override
    public GfshExecutor.Builder withVmConfiguration(VmConfiguration vmConfiguration) {
      javaHome = vmConfiguration.javaVersion().home();
      return withGeodeVersion(vmConfiguration.geodeVersion().toString());
    }

    @Override
    public Builder withGfshJvmOptions(String... option) {
      jvmOptions.addAll(Arrays.asList(option));
      return this;
    }

    @Override
    public GfshContext build(Path dir) {
      this.dir = dir;
      GfshContext context = new GfshContext(this);
      contextCreated.accept(context);
      return context;
    }

    private static Path findGfsh(String version) {
      Path geodeHome;
      if (version == null || VersionManager.isCurrentVersion(version)) {
        geodeHome = new RequiresGeodeHome().getGeodeHome().toPath();
      } else {
        geodeHome = Paths.get(VersionManager.getInstance().getInstall(version));
      }

      if (isWindows()) {
        return geodeHome.resolve("bin").resolve("gfsh.bat");
      }
      return geodeHome.resolve("bin").resolve("gfsh");
    }
  }
}
