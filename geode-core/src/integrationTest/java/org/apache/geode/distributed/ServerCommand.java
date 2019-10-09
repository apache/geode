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

import org.apache.geode.distributed.ServerLauncher.Command;

@SuppressWarnings("unused")
public class ServerCommand {

  private String javaPath;
  private List<String> jvmArguments = new ArrayList<>();
  private String classPath;
  private Command command;
  private String name;
  private boolean disableDefaultServer;
  private boolean force;
  private int serverPort;

  public ServerCommand() {
    // do nothing
  }

  public ServerCommand(final UsesServerCommand user) {
    javaPath = user.getJavaPath();
    jvmArguments = user.getJvmArguments();
    classPath = user.getClassPath();
    name = user.getName();
    disableDefaultServer = user.getDisableDefaultServer();
    command = Command.START;
  }

  public ServerCommand withJavaPath(final String javaPath) {
    this.javaPath = javaPath;
    return this;
  }

  public ServerCommand withJvmArguments(final List<String> jvmArguments) {
    this.jvmArguments = jvmArguments;
    return this;
  }

  public ServerCommand addJvmArgument(final String arg) {
    jvmArguments.add(arg);
    return this;
  }

  public ServerCommand withClassPath(final String classPath) {
    this.classPath = classPath;
    return this;
  }

  public ServerCommand withCommand(final Command command) {
    this.command = command;
    return this;
  }

  public ServerCommand withName(final String name) {
    this.name = name;
    return this;
  }

  public ServerCommand disableDefaultServer() {
    return disableDefaultServer(true);
  }

  public ServerCommand disableDefaultServer(final boolean value) {
    disableDefaultServer = value;
    return this;
  }

  public ServerCommand force() {
    return force(true);
  }

  public ServerCommand force(final boolean value) {
    force = value;
    return this;
  }

  public ServerCommand withServerPort(final int serverPort) {
    this.serverPort = serverPort;
    return disableDefaultServer(false);
  }

  public List<String> create() {
    List<String> cmd = new ArrayList<>();
    cmd.add(javaPath);
    cmd.addAll(jvmArguments);
    cmd.add("-cp");
    cmd.add(classPath);
    cmd.add(ServerLauncher.class.getName());
    cmd.add(command.getName());
    cmd.add(name);
    if (disableDefaultServer) {
      cmd.add("--disable-default-server");
    }
    if (force) {
      cmd.add("--force");
    }
    cmd.add("--redirect-output");
    if (serverPort > 0) {
      cmd.add("--server-port=" + serverPort);
    }
    return cmd;
  }
}
