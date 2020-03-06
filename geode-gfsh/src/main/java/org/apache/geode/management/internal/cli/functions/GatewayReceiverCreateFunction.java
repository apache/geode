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


import static org.apache.geode.cache.wan.GatewayReceiverFactory.A_GATEWAY_RECEIVER_ALREADY_EXISTS_ON_THIS_MEMBER;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.configuration.realizers.GatewayReceiverRealizer;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 * The function to a create GatewayReceiver using given configuration parameters.
 */
public class GatewayReceiverCreateFunction implements InternalFunction<Object[]> {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 8746830191680509335L;

  private static final String ID = GatewayReceiverCreateFunction.class.getName();

  @Immutable
  public static final GatewayReceiverCreateFunction INSTANCE = new GatewayReceiverCreateFunction();

  @Override
  public void execute(FunctionContext<Object[]> context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = context.getCache();
    String memberNameOrId = context.getMemberName();

    Object[] gatewayReceiverCreateArgs =
        context.getArguments();
    GatewayReceiverConfig gatewayReceiverConfig =
        (GatewayReceiverConfig) gatewayReceiverCreateArgs[0];
    Boolean ifNotExist = (Boolean) gatewayReceiverCreateArgs[1];

    // Exit early if a receiver already exists.
    // Consider this a failure unless --if-not-exists was provided.
    if (gatewayReceiverExists(cache)) {
      CliFunctionResult result;
      if (ifNotExist) {
        result = new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.OK,
            "Skipping: " + A_GATEWAY_RECEIVER_ALREADY_EXISTS_ON_THIS_MEMBER);
      } else {
        Exception illegalState =
            new IllegalStateException(A_GATEWAY_RECEIVER_ALREADY_EXISTS_ON_THIS_MEMBER);
        result = new CliFunctionResult(memberNameOrId, illegalState, illegalState.getMessage());
      }
      resultSender.lastResult(result);
      return;
    }


    try {
      GatewayReceiver createdGatewayReceiver = createGatewayReceiver(cache, gatewayReceiverConfig);

      resultSender.lastResult(new CliFunctionResult(memberNameOrId,
          CliFunctionResult.StatusState.OK,
          CliStrings.format(
              CliStrings.CREATE_GATEWAYRECEIVER__MSG__GATEWAYRECEIVER_CREATED_ON_0_ONPORT_1,
              memberNameOrId, Integer.toString(createdGatewayReceiver.getPort()))));
    } catch (IllegalStateException e) {
      // no need to log the stack trace
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, e, e.getMessage()));
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, e, e.getMessage()));
    }

  }

  GatewayReceiver createGatewayReceiver(Cache cache,
      GatewayReceiverConfig gatewayReceiverConfig) {
    GatewayReceiverRealizer receiverRealizer = new GatewayReceiverRealizer();
    receiverRealizer.create(gatewayReceiverConfig, (InternalCache) cache);

    return cache.getGatewayReceivers().iterator().next();
  }

  boolean gatewayReceiverExists(Cache cache) {
    return cache.getGatewayReceivers() != null && !cache.getGatewayReceivers().isEmpty();
  }

  @Override
  public String getId() {
    return ID;
  }

}
