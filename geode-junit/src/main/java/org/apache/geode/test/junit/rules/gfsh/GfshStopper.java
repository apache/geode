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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.OptionalInt;

import org.apache.geode.internal.process.NativeProcessUtils;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.process.ProcessUtilsProvider;

/**
 * Provides methods for stopping and awaiting the termination of server and locator processes.
 */
public class GfshStopper {

  private final GfshExecutor executor;
  private final Path rootFolder;
  private final ProcessType processType;

  GfshStopper(GfshExecutor executor, Path rootFolder, ProcessType processType) {
    this.executor = executor;
    this.rootFolder = rootFolder.toAbsolutePath();
    this.processType = processType;
  }

  public void stop(Path directory) {
    Path absoluteDirectory = rootFolder.resolve(directory).normalize();
    String command = stopCommand(absoluteDirectory);
    String scriptName =
        String.format("Stop-%s-%s", processType.name().toLowerCase(),
            absoluteDirectory.getFileName());
    try {
      executor.execute(GfshScript.of(command).withName(scriptName));
      awaitStop(absoluteDirectory);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void stop(String directoryName) {
    stop(Paths.get(directoryName));
  }

  public void awaitStop(Path directory) {
    Path serverPidFile =
        rootFolder.resolve(directory).resolve(processType.getPidFileName()).normalize();
    readPid(serverPidFile).ifPresent(this::awaitStop);
  }

  public void awaitStop(String directoryName) {
    awaitStop(Paths.get(directoryName));
  }

  private void awaitStop(int pid) {
    ProcessUtilsProvider processUtils = NativeProcessUtils.create();
    await().untilAsserted(() -> {
      assertThat(processUtils.isProcessAlive(pid))
          .as("Process for %s with pid %d is alive", processType, pid)
          .isFalse();
    });
  }

  private String stopCommand(Path directory) {
    return String.join(" ", "stop", processType.name().toLowerCase(), "--dir=" + directory);
  }

  private OptionalInt readPid(Path pidFile) {
    try {
      return OptionalInt.of(ProcessUtils.readPid(pidFile));
    } catch (Exception e) {
      // process may have terminated if there is no pid file
      return OptionalInt.empty();
    }
  }
}
