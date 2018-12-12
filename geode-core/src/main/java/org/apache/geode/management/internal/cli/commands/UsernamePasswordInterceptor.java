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
package org.apache.geode.management.internal.cli.commands;

import org.apache.commons.lang.StringUtils;

import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;

public class UsernamePasswordInterceptor extends AbstractCliAroundInterceptor {
  private final Gfsh gfsh;

  public UsernamePasswordInterceptor() {
    this(Gfsh.getCurrentInstance());
  }

  // Constructor for unit test
  UsernamePasswordInterceptor(Gfsh gfsh) {
    this.gfsh = gfsh;
  }

  @Override
  public ResultModel preExecution(GfshParseResult parseResult) {
    if (gfsh == null || !gfsh.isConnectedAndReady()) {
      return new ResultModel();
    }

    String userInput = parseResult.getUserInput();

    String username = parseResult.getParamValueAsString("username");
    String password = parseResult.getParamValueAsString("password");

    if (!StringUtils.isBlank(password) && StringUtils.isBlank(username)) {
      username = gfsh.readText(CliStrings.INTERCEPTOR_USERNAME);
      userInput = userInput + " --username=" + username;
      parseResult.setUserInput(userInput);
    } else if (!StringUtils.isBlank(username) && StringUtils.isBlank(password)) {
      password = gfsh.readPassword(CliStrings.INTERCEPTOR_PASSWORD);
      userInput = userInput + " --password=" + password;
      parseResult.setUserInput(userInput);
    }

    return new ResultModel();
  }
}
