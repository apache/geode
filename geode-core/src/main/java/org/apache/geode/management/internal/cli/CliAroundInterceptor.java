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
package org.apache.geode.management.internal.cli;

import java.nio.file.Path;

import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.GfshExecutionStrategy;

/**
 * Interceptor interface which {@link GfshExecutionStrategy} can use to intercept before & after
 * actual command execution.
 *
 *
 * @since GemFire 7.0
 */
public interface CliAroundInterceptor {

  /**
   * called by the OperationInvoker before the command is executed
   */
  default Object preExecution(GfshParseResult parseResult) {
    return ResultBuilder.createInfoResult("");
  }

  /**
   * called by the OperationInvoker after the command is executed
   *
   * @param tempFile if the command's isFileDownloadOverHttp is true, the is the File downloaded
   *        after the http response is processed.
   */
  default CommandResult postExecution(GfshParseResult parseResult, CommandResult commandResult,
      Path tempFile) throws Exception {
    return commandResult;
  }

  default ResultModel postExecution(GfshParseResult parseResult, ResultModel resultModel,
      Path tempFile) throws Exception {
    return resultModel;
  }

}
