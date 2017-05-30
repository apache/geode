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
package org.apache.geode.internal;

public class ShellExitCode {

  // The choice of values here is to be consistent with currently expected behavior
  // while allowing for extensibility of exit codes.
  public static final ShellExitCode NORMAL_EXIT = new ShellExitCode(0);
  public static final ShellExitCode FATAL_EXIT = new ShellExitCode(1);
  public static final ShellExitCode COULD_NOT_EXECUTE_COMMAND_EXIT = new ShellExitCode(1);
  public static final ShellExitCode INVALID_COMMAND_EXIT = new ShellExitCode(1);
  public static final ShellExitCode COMMAND_NOT_ALLOWED_EXIT = new ShellExitCode(1);
  public static final ShellExitCode COMMAND_NOT_SUCCESSFUL = new ShellExitCode(1);
  public static final ShellExitCode INSTALL_FAILURE = new ShellExitCode(2);

  // Field
  private final int exitCode;

  private ShellExitCode(final int exitCode) {
    this.exitCode = exitCode;
  }

  public int getExitCode() {
    return exitCode;
  }

}
