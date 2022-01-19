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

import java.util.Arrays;

public enum ExitCode {

  // JVM_TERMINATED_EXIT(99) exists for coverage of Spring's ExitShellRequest values in fromSpring.
  DEPENDENCY_GRAPH_FAILURE(-1), NORMAL(0), FATAL(1), INSTALL_FAILURE(2), JVM_TERMINATED_EXIT(99);

  private final int shellReturnValue;

  ExitCode(final int shellReturnValue) {
    this.shellReturnValue = shellReturnValue;
  }

  public int getValue() {
    return shellReturnValue;
  }

  public void doSystemExit() {
    System.exit(shellReturnValue);
  }

  public static ExitCode fromValue(int i) {
    return Arrays.stream(ExitCode.values()).filter(c -> c.getValue() == i).findFirst().orElseThrow(
        () -> new IllegalArgumentException("No ExitCode exists with shell exit value: " + i));
  }
}
