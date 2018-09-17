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
import java.util.stream.Collectors;

/**
 * All the commands represented in this script is executed within one gfsh session.
 *
 * all the commands in this script are executed using this bash command:
 * gfsh -e command1 -e command2 -e command3 ....
 *
 * You can chain commands together to create a gfshScript
 * GfshScript.of("command1").and("command2").and("command3", "command4")
 *
 * If your command started another process and you want to that process to be debuggable, you can do
 * GfshScript.of("start locator", 30000).and("start server", 30001)
 * this will allow locator to be debuggable at 30000 and the server to be debuggable at 30001
 *
 * By default, each scripts await at most 4 minutes for all the commands to finish
 * and will expect success. if you want to change this, you can use:
 * gfshScript.awaitAtMost(1, TimeUnit.MINUTES).expectFailure()
 *
 * if you want this gfsh session to be debuggable, you can use:
 * gfshScript.withDebugPort(30000)
 * This will allow gfsh to be debuggable at port 30000.
 *
 */
public class GfshScript {
  private List<DebuggableCommand> commands = new ArrayList<>();
  private String name;
  private TimeUnit timeoutTimeUnit = TimeUnit.MINUTES;
  private int timeout = 4;
  private int expectedExitValue = 0;
  private List<String> extendedClasspath = new ArrayList<>();
  private Random random = new Random();
  private int debugPort = -1;

  public GfshScript() {
    this.name = defaultName();
  }

  public static GfshScript of(String... commands) {
    GfshScript script = new GfshScript();
    script.and(commands);
    return script;
  }

  public static GfshScript of(String command, int debugPort) {
    GfshScript script = new GfshScript();
    script.and(command, debugPort);
    return script;
  }

  public GfshScript and(String... commands) {
    for (String command : commands) {
      this.commands.add(new DebuggableCommand(command));
    }
    return this;
  }

  public GfshScript and(String command, int debugPort) {
    this.commands.add(new DebuggableCommand(command, debugPort));
    return this;
  }

  public GfshScript withName(String name) {
    assertThat(name.contains(" ")).as("argument passed to withName cannot have spaces").isFalse();
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
   * Will cause the thread that executes to wait, if necessary,
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

  public GfshScript withDebugPort(int debugPort) {
    this.debugPort = debugPort;
    return this;
  }

  public GfshExecution execute(GfshRule gfshRule) {
    return gfshRule.execute(this);
  }

  public List<DebuggableCommand> getCommands() {
    return commands;
  }

  public String getName() {
    return name;
  }

  public TimeUnit getTimeoutTimeUnit() {
    return timeoutTimeUnit;
  }

  public int getTimeout() {
    return timeout;
  }

  public int getExpectedExitValue() {
    return expectedExitValue;
  }

  public int getDebugPort() {
    return debugPort;
  }

  private String defaultName() {
    return Long.toHexString(random.nextLong());
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(name).append(": gfsh ");
    builder.append(commands.stream().map(c -> "-e " + c.command).collect(Collectors.joining(" ")));
    return builder.toString();
  }
}
