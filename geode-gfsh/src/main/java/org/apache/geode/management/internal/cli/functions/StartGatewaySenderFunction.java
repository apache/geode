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
package org.apache.geode.management.internal.cli.functions;

import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

public class StartGatewaySenderFunction implements InternalFunction<Object> {
  private static final long serialVersionUID = 1L;

  private static final String ID = StartGatewaySenderFunction.class.getName();

  private static final Logger logger = LogService.getLogger();

  @Override
  public void execute(FunctionContext<Object> context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = context.getCache();
    String memberNameOrId = context.getMemberName();

    String senderId = (String) ((List) context.getArguments()).get(0);
    Boolean cleanQueues = (Boolean) ((List) context.getArguments()).get(1);

    try {
      startGatewaySender(cache, resultSender, memberNameOrId, senderId, cleanQueues);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, e, null));
    }
  }

  private void startGatewaySender(Cache cache,
      ResultSender<Object> resultSender,
      String memberNameOrId,
      String senderId,
      Boolean cleanQueues) {
    GatewaySender gateway = cache.getGatewaySender(senderId);
    if (gateway == null) {
      resultSender.lastResult(new CliFunctionResult(memberNameOrId,
          CliFunctionResult.StatusState.ERROR, CliStrings.format(
              CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
              senderId, memberNameOrId)));
      return;
    }

    if (gateway.isRunning()) {
      resultSender.lastResult(new CliFunctionResult(memberNameOrId,
          CliFunctionResult.StatusState.ERROR, CliStrings.format(
              CliStrings.GATEWAY_SENDER_0_IS_ALREADY_STARTED_ON_MEMBER_1,
              senderId, memberNameOrId)));
      return;
    }

    if (cleanQueues) {
      gateway.startWithCleanQueue();
    } else {
      gateway.start();
    }
    resultSender.lastResult(new CliFunctionResult(memberNameOrId,
        CliFunctionResult.StatusState.OK, CliStrings.format(
            CliStrings.GATEWAY_SENDER_0_IS_STARTED_ON_MEMBER_1,
            senderId, memberNameOrId)));

  }

  @Override
  public String getId() {
    return ID;
  }
}
