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
package org.apache.geode.connectors.jdbc.internal.cli;

import java.io.Serializable;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

/**
 * Handles exceptions by returning an error result to GFSH
 */
class ExceptionHandler implements Serializable {
  private static final Logger logger = LogService.getLogger();

  void handleException(final FunctionContext<?> context, final Exception exception) {
    String message = getExceptionMessage(exception);
    String member = getMember(context.getCache());
    context.getResultSender().lastResult(handleException(member, message, exception));
  }

  private CliFunctionResult handleException(final String memberNameOrId, final String exceptionMsg,
      final Exception exception) {
    if (exception != null && logger.isDebugEnabled()) {
      logger.debug(exception.getMessage(), exception);
    }
    // if (exceptionMsg != null) {
    return new CliFunctionResult(memberNameOrId, false, exceptionMsg);
    // }
    //
    // return new CliFunctionResult(memberNameOrId);
  }

  private String getMember(final Cache cache) {
    return CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());
  }

  private String getExceptionMessage(final Exception exception) {
    String message = exception.getMessage();
    if (message == null) {
      message = CliUtil.stackTraceAsString(exception);
    }
    return message;
  }
}
