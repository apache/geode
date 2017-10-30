/*
 *
 * * Licensed to the Apache Software Foundation (ASF) under one or more contributor license *
 * agreements. See the NOTICE file distributed with this work for additional information regarding *
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * * "License"); you may not use this file except in compliance with the License. You may obtain a *
 * copy of the License at * * http://www.apache.org/licenses/LICENSE-2.0 * * Unless required by
 * applicable law or agreed to in writing, software distributed under the License * is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express * or implied.
 * See the License for the specific language governing permissions and limitations under * the
 * License. *
 *
 */

package org.apache.geode.management.internal.cli.commands;

import java.util.List;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;

public class DataCommandUtil {
  public static Result getFunctionResult(ResultCollector<?, ?> rc, String commandName) {
    Result result;
    List<Object> results = (List<Object>) rc.getResult();
    if (results != null) {
      Object resultObj = results.get(0);
      if (resultObj instanceof String) {
        result = ResultBuilder.createInfoResult((String) resultObj);
      } else if (resultObj instanceof Exception) {
        result = ResultBuilder.createGemFireErrorResult(((Exception) resultObj).getMessage());
      } else {
        result = ResultBuilder.createGemFireErrorResult(
            CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, commandName));
      }
    } else {
      result = ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, commandName));
    }
    return result;
  }
}
