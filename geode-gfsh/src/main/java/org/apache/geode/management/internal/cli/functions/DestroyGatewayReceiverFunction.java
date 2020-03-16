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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.functions.CliFunctionResult.StatusState;

public class DestroyGatewayReceiverFunction extends CliFunction<Void> {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 1490927519860899562L;
  private static final String ID = DestroyGatewayReceiverFunction.class.getName();
  @Immutable
  public static final DestroyGatewayReceiverFunction INSTANCE =
      new DestroyGatewayReceiverFunction();

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Void> context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = context.getCache();
    String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    if (gatewayReceivers != null && !gatewayReceivers.isEmpty()) {
      for (GatewayReceiver receiver : gatewayReceivers) {
        try {
          if (receiver.isRunning()) {
            receiver.stop();
          }
          receiver.destroy();
          return new CliFunctionResult(memberNameOrId, StatusState.OK,
              String.format("GatewayReceiver destroyed on \"%s\"", memberNameOrId));
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
          return new CliFunctionResult(memberNameOrId, e, "");
        }
      }
    }
    return new CliFunctionResult(memberNameOrId, StatusState.IGNORABLE,
        "Gateway receiver not found.");
  }

  @Override
  public String getId() {
    return ID;
  }

}
