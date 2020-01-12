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
package org.apache.geode.distributed;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.distributed.LocatorLauncher.Command;

@SuppressWarnings("unused")
public class LocatorCommand {

  private String javaPath;
  private List<String> jvmArguments = new ArrayList<>();
  private String classPath;
  private Command command;
  private String name;
  private boolean force;
  private int port;

  public LocatorCommand() {
    // do nothing
  }

  public LocatorCommand(final UsesLocatorCommand user) {
    javaPath = user.getJavaPath();
    jvmArguments = user.getJvmArguments();
    classPath = user.getClassPath();
    name = user.getName();
    command = Command.START;
  }

  public LocatorCommand withJavaPath(final String javaPath) {
    this.javaPath = javaPath;
    return this;
  }

  public LocatorCommand withJvmArguments(final List<String> jvmArguments) {
    this.jvmArguments = jvmArguments;
    return this;
  }

  public LocatorCommand addJvmArgument(final String arg) {
    jvmArguments.add(arg);
    return this;
  }

  public LocatorCommand withClassPath(final String classPath) {
    this.classPath = classPath;
    return this;
  }

  public LocatorCommand withCommand(final Command command) {
    this.command = command;
    return this;
  }

  public LocatorCommand withName(final String name) {
    this.name = name;
    return this;
  }

  public LocatorCommand force() {
    return force(true);
  }

  public LocatorCommand force(final boolean value) {
    force = value;
    return this;
  }

  public LocatorCommand withPort(final int port) {
    this.port = port;
    return this;
  }

  public List<String> create() {
    List<String> cmd = new ArrayList<>();
    cmd.add(javaPath);
    cmd.addAll(jvmArguments);
    cmd.add("-cp");
    cmd.add(classPath);
    cmd.add(LocatorLauncher.class.getName());
    cmd.add(command.getName());
    cmd.add(name);
    if (force) {
      cmd.add("--force");
    }
    cmd.add("--redirect-output");
    if (port > 0) {
      cmd.add("--port=" + port);
    }
    return cmd;
  }
}
