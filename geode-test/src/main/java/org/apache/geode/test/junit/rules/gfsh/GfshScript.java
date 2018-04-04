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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GfshScript {
  private final String[] commands;
  private String name;
  private TimeUnit timeoutTimeUnit = TimeUnit.MINUTES;
  private int timeout = 4;
  private boolean awaitQuietly = false;
  private int expectedExitValue = 0;
  private List<String> extendedClasspath = new ArrayList<>();
  private Random random = new Random();

  public GfshScript(String... commands) {
    this.commands = commands;
    this.name = defaultName();
  }

  /**
   * By default, this GfshScript will await at most 2 minutes and will expect success.
   */
  public static GfshScript of(String... commands) {
    return new GfshScript(commands);
  }

  public GfshScript withName(String name) {
    this.name = name;

    return this;
  }

  public GfshScript expectExitCode(int expectedExitCode) {
    this.expectedExitValue = expectedExitCode;

    return this;
  }

  public GfshScript expectFailure() {
    return expectExitCode(1);
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

  public List<String> getExtendedClasspath() {
    return extendedClasspath;
  }

  public GfshScript addToClasspath(String classpath) {
    extendedClasspath.add(classpath);

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

  public GfshScript awaitQuietly() {
    this.awaitQuietly = true;

    return this;
  }

  public GfshExecution execute(GfshRule gfshRule) {
    return gfshRule.execute(this);
  }

  protected void awaitIfNecessary(GfshExecution gfshExecution) {
    if (shouldAwaitQuietly()) {
      awaitQuietly(gfshExecution);
    } else if (shouldAwaitLoudly()) {
      awaitLoudly(gfshExecution);
    }

    try {
      assertThat(gfshExecution.getProcess().exitValue()).isEqualTo(expectedExitValue);
    } catch (AssertionError e) {
      gfshExecution.printLogFiles();
      throw e;
    }

  }

  private void awaitQuietly(GfshExecution gfshExecution) {
    try {
      gfshExecution.getProcess().waitFor(timeout, timeoutTimeUnit);
    } catch (InterruptedException ignore) {
      // ignore since we are waiting *quietly*
    }
  }

  private void awaitLoudly(GfshExecution gfshExecution) {
    boolean exited;
    try {
      exited = gfshExecution.getProcess().waitFor(timeout, timeoutTimeUnit);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    try {
      assertThat(exited).isTrue();
    } catch (AssertionError e) {
      gfshExecution.printLogFiles();
      throw e;
    }
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

  public String[] getCommands() {
    return commands;
  }

  public String getName() {
    return name;
  }

  private String defaultName() {
    return Long.toHexString(random.nextLong());
  }



}
