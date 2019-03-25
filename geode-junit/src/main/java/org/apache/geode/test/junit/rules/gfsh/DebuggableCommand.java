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

import org.apache.geode.management.api.ClusterManagementService;

public class DebuggableCommand {
  final int debugPort;
  final String command;

  public DebuggableCommand(String command) {
    this(command, -1);
  }

  public DebuggableCommand(String command, int debugPort) {

    if (command.startsWith("start locator")) {
      command += " --J=-D" + ClusterManagementService.FEATURE_FLAG + "=true ";
    }
    this.command = command;
    this.debugPort = debugPort;
  }
}
