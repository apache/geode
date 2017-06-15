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
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class GfshScript {
  private final String[] commands;
  private Integer timeout;
  private TimeUnit timeoutTimeUnit;
  private boolean awaitQuietly = false;
  private Integer expectedExitValue;

  public GfshScript(String... commands) {
    this.commands = commands;
  }

  public static GfshScript of(String... commands) {
    return new GfshScript(commands);
  }


  public GfshScript expectExitCode(int expectedExitCode) {
    this.expectedExitValue = expectedExitCode;

    return this;
  }

  /**
   * Will cause the thread that executes {@link GfshScript#awaitIfNecessary} to wait, if necessary,
   * until the subprocess executing this Gfsh script has terminated, or the specified waiting time
   * elapses.
   * 
   * @throws RuntimeException if the current thread is interrupted while waiting.
   * @throws AssertionError if the specified waiting time elapses before the process exits.
   */
  public GfshScript awaitAtMost(int timeout, TimeUnit timeUnit) {
    this.timeout = timeout;
    this.timeoutTimeUnit = timeUnit;

    return this;
  }

  /**
   * Will cause the thread that executes {@link GfshScript#awaitIfNecessary} to wait, if necessary,
   * until the subprocess executing this Gfsh script has terminated, or the specified waiting time
   * elapses.
   */
  public GfshScript awaitQuietlyAtMost(int timeout, TimeUnit timeUnit) {
    this.awaitQuietly = true;

    return awaitAtMost(timeout, timeUnit);
  }


  protected ProcessBuilder toProcessBuilder(Path gfshPath, File workingDir) {
    String[] gfshCommands = new String[commands.length + 1];
    gfshCommands[0] = gfshPath.toAbsolutePath().toString();

    for (int i = 0; i < commands.length; i++) {
      gfshCommands[i + 1] = "-e " + commands[i];
    }

    return new ProcessBuilder(gfshCommands).inheritIO().directory(workingDir);
  }

  protected void awaitIfNecessary(Process process) {
    if (shouldAwaitQuietly()) {
      awaitQuietly(process);
    } else if (shouldAwaitLoudly()) {
      awaitLoudly(process);
    }

    if (expectedExitValue != null) {
      assertThat(process.exitValue()).isEqualTo(expectedExitValue);
    }
  }

  private void awaitQuietly(Process process) {
    try {
      process.waitFor(timeout, timeoutTimeUnit);
    } catch (InterruptedException ignore) {
      // ignore since we are waiting *quietly*
    }
  }

  private void awaitLoudly(Process process) {
    boolean exited;
    try {
      exited = process.waitFor(timeout, timeoutTimeUnit);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertThat(exited).isTrue();
  }

  private boolean shouldAwait() {
    return timeoutTimeUnit != null;
  }

  private boolean shouldAwaitQuietly() {
    return shouldAwait() && awaitQuietly;
  }

  private boolean shouldAwaitLoudly() {
    return shouldAwait() && !awaitQuietly;
  }
}
