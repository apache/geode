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
package org.apache.geode.test.junit.assertions;

import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;

public class GfshShellConnectionRuleExecution {
  private GfshShellConnectionRule gfsh;
  private CommandResult commandResult;

  public GfshShellConnectionRuleExecution(GfshShellConnectionRule gfsh,
      CommandResult commandResult) {

    this.gfsh = gfsh;
    this.commandResult = commandResult;
  }

  public GfshShellConnectionRule getGfsh() {
    return gfsh;
  }

  public CommandResult getCommandResult() {
    return commandResult;
  }

  public String getGfshOutput() {
    return gfsh.getGfshOutput();
  }
}
