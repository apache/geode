/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal.cli;


import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.cache.operations.OperationContext.Resource;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.commands.AbstractCommandsSupport;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

public class LuceneIndexCommands extends AbstractCommandsSupport {
  @CliCommand(value = LuceneCliStrings.LIST_INDEX, help = LuceneCliStrings.LIST_INDEX__HELP)
  @CliMetaData(shellOnly = false, relatedTopic={CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA })
  @ResourceOperation(resource = Resource.CLUSTER, operation = OperationCode.READ)
  public Result listIndex() {
    try {
      final TabularResultData indexData = ResultBuilder.createTabularResultData();
      return ResultBuilder.buildResult(indexData);
    }
    catch (FunctionInvocationTargetException ignore) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN,
        CliStrings.LIST_INDEX));
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      SystemFailure.checkFailure();
      getCache().getLogger().error(t);
      return ResultBuilder.createGemFireErrorResult(String.format(CliStrings.LIST_INDEX__ERROR_MESSAGE,
        toString(t, isDebugging())));
    }
  }

}
