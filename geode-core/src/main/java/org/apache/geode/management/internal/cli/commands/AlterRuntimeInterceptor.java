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

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import org.apache.geode.internal.logging.log4j.LogLevel;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;

public class AlterRuntimeInterceptor extends AbstractCliAroundInterceptor {
  @Override
  public Result preExecution(GfshParseResult parseResult) {
    Map<String, String> arguments = parseResult.getParamValueStrings();
    // validate log level
    String logLevel = arguments.get("log-level");
    if (StringUtils.isNotBlank(logLevel) && (LogLevel.getLevel(logLevel) == null)) {
      return ResultBuilder.createUserErrorResult("Invalid log level: " + logLevel);
    }
    return ResultBuilder.createInfoResult("");
  }
}
