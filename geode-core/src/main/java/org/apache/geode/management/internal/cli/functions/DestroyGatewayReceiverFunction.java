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


import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState;

public class DestroyGatewayReceiverFunction implements InternalFunction {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 1490927519860899562L;
  private static final String ID = DestroyGatewayReceiverFunction.class.getName();
  public static DestroyGatewayReceiverFunction INSTANCE = new DestroyGatewayReceiverFunction();

  @Override
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = context.getCache();
    String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    if (gatewayReceivers == null || gatewayReceivers.isEmpty()) {
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, StatusState.IGNORED,
          "Gateway receiver not found."));
      return;
    }
    for (GatewayReceiver receiver : cache.getGatewayReceivers()) {
      try {
        if (receiver.isRunning()) {
          receiver.stop();
        }
        receiver.destroy();
        resultSender.sendResult(new CliFunctionResult(memberNameOrId, StatusState.OK,
            String.format("GatewayReceiver destroyed on \"%s\"", memberNameOrId)));
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        resultSender.sendResult(new CliFunctionResult(memberNameOrId, e, ""));
      }
      resultSender.lastResult(-1);
    }
  }

  @Override
  public String getId() {
    return ID;
  }

}
