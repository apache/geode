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

import static java.lang.Long.toHexString;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * All the commands represented in this script are executed within one gfsh session.
 *
 * <p>
 * All the commands in this script are executed using this bash command:<br>
 *
 * <pre>
 * gfsh -e command1 -e command2 -e command3 ....
 * </pre>
 *
 * <p>
 * You can chain commands together to create a GfshScript<br>
 *
 * <pre>
 * GfshScript.of("command1").and("command2").and("command3", "command4")
 * </pre>
 *
 * <p>
 * If your command started another process and you want that process to be debuggable, you can do:
 *
 * <pre>
 * GfshScript.of("start locator", 30000).and("start server", 30001)
 * </pre>
 *
 * This will allow locator to be debuggable at 30000 and the server to be debuggable at 30001.
 *
 * <p>
 * By default, each script awaits at most 5 minutes for all the commands to finish and will expect
 * success. If you want to change this, you can use:
 *
 * <pre>
 * gfshScript.awaitAtMost(1, TimeUnit.MINUTES).expectFailure()
 * </pre>
 *
 * <p>
 * If you want this gfsh session to be debuggable, you can use:
 *
 * <pre>
 * gfshScript.withDebugPort(30000)
 * </pre>
 *
 * This will allow gfsh to be debuggable at port 30000.
 */
public class GfshScript {

  private final List<DebuggableCommand> commands = new ArrayList<>();
  private final List<String> extendedClasspath = new ArrayList<>();
  private final Random random = new Random();

  private String name;
  private TimeUnit timeoutTimeUnit = TimeUnit.MINUTES;
  private long timeout = GeodeAwaitility.getTimeout().toMinutes();
  private int expectedExitValue;
  private int debugPort = -1;

  public GfshScript() {
    name = defaultName();
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
    commands.add(new DebuggableCommand(command, debugPort));
    return this;
  }

  public GfshScript withName(String name) {
    assertThat(name).doesNotContain(" ");
    this.name = name;
    return this;
  }

  public GfshScript expectExitCode(int expectedExitCode) {
    expectedExitValue = expectedExitCode;
    return this;
  }

  public GfshScript expectFailure() {
    return expectExitCode(1);
  }

  /**
   * Will cause the thread that executes to wait, if necessary, until the subprocess executing this
   * Gfsh script has terminated, or the specified waiting time elapses.
   *
   * @throws RuntimeException if the current thread is interrupted while waiting.
   * @throws AssertionError if the specified waiting time elapses before the process exits.
   */
  public GfshScript awaitAtMost(int timeout, TimeUnit timeUnit) {
    this.timeout = timeout;
    timeoutTimeUnit = timeUnit;

    return this;
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

  /**
   * this will allow you to specify a gfsh workingDir when executing the script
   */
  public GfshExecution execute(GfshRule gfshRule, File workingDir) {
    return gfshRule.execute(this, workingDir);
  }

  @Override
  public String toString() {
    return name + ": gfsh "
        + commands.stream()
            .map(debuggableCommand -> "-e " + debuggableCommand.command)
            .collect(joining(" "));
  }

  List<String> getExtendedClasspath() {
    return unmodifiableList(extendedClasspath);
  }

  List<DebuggableCommand> getCommands() {
    return unmodifiableList(commands);
  }

  String getName() {
    return name;
  }

  TimeUnit getTimeoutTimeUnit() {
    return timeoutTimeUnit;
  }

  long getTimeout() {
    return timeout;
  }

  int getExpectedExitValue() {
    return expectedExitValue;
  }

  int getDebugPort() {
    return debugPort;
  }

  private String defaultName() {
    return toHexString(random.nextLong());
  }
}
