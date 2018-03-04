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

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.ListJndiBindingFunction;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;

public class ListJndiBindingCommand implements GfshCommand {
  private static final Logger logger = LogService.getLogger();

  private static final String LIST_JNDIBINDING = "list jndi-binding";
  private static final String LIST_JNDIBINDING__HELP = "List all jndi bindings.";
  private static final Function LIST_BINDING_FUNCTION = new ListJndiBindingFunction();

  @CliCommand(value = LIST_JNDIBINDING, help = LIST_JNDIBINDING__HELP)
  public Result listJndiBinding() {
    Result result = null;
    TabularResultData tabularData = ResultBuilder.createTabularResultData();

    Set<DistributedMember> members = findMembers(null, null);
    if (members.size() == 0) {
      return ResultBuilder.createUserErrorResult("No members found");
    }

    List<CliFunctionResult> rc = executeAndGetFunctionResult(LIST_BINDING_FUNCTION, null, members);
    for (CliFunctionResult oneResult : rc) {
      Serializable[] serializables = oneResult.getSerializables();
      for (int i = 0; i < serializables.length; i += 2) {
        tabularData.accumulate("Member", oneResult.getMemberIdOrName());
        tabularData.accumulate("Name", serializables[i]);
        tabularData.accumulate("Class", serializables[i + 1]);
      }
    }

    result = ResultBuilder.buildResult(tabularData);

    return result;
  }
}
