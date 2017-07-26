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

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResultException;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;

public class GatewayCommandsUtils {
  public static void accumulateStartResult(TabularResultData resultData, String member,
      String Status, String message) {
    if (member != null) {
      resultData.accumulate("Member", member);
    }
    resultData.accumulate("Result", Status);
    resultData.accumulate("Message", message);
  }

  static Result handleCommandResultException(CommandResultException crex) {
    Result result;
    if (crex.getResult() != null) {
      result = crex.getResult();
    } else {
      LogWrapper.getInstance().warning(CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(crex));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR + crex.getMessage());
    }
    return result;
  }
}
