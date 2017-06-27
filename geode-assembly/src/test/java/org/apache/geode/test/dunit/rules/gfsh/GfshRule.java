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
package org.apache.geode.test.dunit.rules.gfsh;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.cli.commands.StatusLocatorRealGfshTest;
import org.apache.geode.test.dunit.rules.RequiresGeodeHome;

/**
 * The {@code GfshRule} allows a test to execute Gfsh commands via the actual (fully-assembled) gfsh
 * binaries. For a usage example, see {@link StatusLocatorRealGfshTest}. Each call to
 * {@link GfshRule#execute(GfshScript)} will invoke the given gfsh script in a forked JVM. The
 * {@link GfshRule#after()} method will attempt to clean up all forked JVMs.
 */
public class GfshRule extends ExternalResource {
  private TemporaryFolder temporaryFolder = new TemporaryFolder();
  private List<Process> processes = new ArrayList<>();
  private Path gfsh;

  public Process execute(String... commands) {
    return execute(GfshScript.of(commands));
  }

  public Process execute(GfshScript gfshScript) {
    Process process;
    try {
      process = gfshScript.toProcessBuilder(gfsh, temporaryFolder.getRoot()).start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    processes.add(process);
    gfshScript.awaitIfNecessary(process);

    return process;
  }

  @Override
  protected void before() throws IOException {
    gfsh = new RequiresGeodeHome().getGeodeHome().toPath().resolve("bin/gfsh");
    assertThat(gfsh).exists();

    temporaryFolder.create();
  }

  /**
   * Attempts to stop any started servers/locators via pid file and tears down any remaining gfsh
   * JVMs.
   */
  @Override
  protected void after() {
    stopMembersQuietly();
    processes.forEach(Process::destroyForcibly);
    processes.forEach((Process process) -> {
      try {
        // Process.destroyForcibly() may not terminate immediately
        process.waitFor(1, TimeUnit.MINUTES);
      } catch (InterruptedException ignore) {
        // We ignore this exception so that we still attempt the rest of the cleanup.
      }
    });
    temporaryFolder.delete();
  }

  private void stopMembersQuietly() {
    File[] directories = temporaryFolder.getRoot().listFiles(File::isDirectory);

    Predicate<File> isServerDir = (File directory) -> Arrays.stream(directory.list())
        .anyMatch(filename -> filename.endsWith("server.pid"));

    Predicate<File> isLocatorDir = (File directory) -> Arrays.stream(directory.list())
        .anyMatch(filename -> filename.endsWith("locator.pid"));

    Arrays.stream(directories).filter(isServerDir).forEach(this::stopServerInDir);
    Arrays.stream(directories).filter(isLocatorDir).forEach(this::stopLocatorInDir);
  }

  private void stopServerInDir(File dir) {
    GfshScript stopServerScript = new GfshScript("stop server --dir=" + dir.getAbsolutePath())
        .awaitQuietlyAtMost(1, TimeUnit.MINUTES);

    execute(stopServerScript);
  }

  private void stopLocatorInDir(File dir) {
    GfshScript stopServerScript = new GfshScript("stop locator --dir=" + dir.getAbsolutePath())
        .awaitQuietlyAtMost(1, TimeUnit.MINUTES);

    execute(stopServerScript);
  }
}
