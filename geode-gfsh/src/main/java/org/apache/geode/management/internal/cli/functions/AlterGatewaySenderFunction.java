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
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.security.CallbackInstantiator;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

public class AlterGatewaySenderFunction implements InternalFunction<GatewaySenderFunctionArgs> {
  private static final long serialVersionUID = 1L;

  private static final String ID = AlterGatewaySenderFunction.class.getName();

  private static final Logger logger = LogService.getLogger();

  @Override
  public void execute(FunctionContext<GatewaySenderFunctionArgs> context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = context.getCache();
    String memberNameOrId = context.getMemberName();

    GatewaySenderFunctionArgs gatewaySenderAlterArgs =
        context.getArguments();

    try {
      GatewaySender alterGatewaySender = alterGatewaySender(cache, gatewaySenderAlterArgs);
      resultSender.lastResult(new CliFunctionResult(memberNameOrId,
          CliFunctionResult.StatusState.OK, CliStrings.format(
              CliStrings.GATEWAY_SENDER_0_IS_UPDATED_ON_MEMBER_1,
              alterGatewaySender.getId(), memberNameOrId)));
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, e, null));
    }
  }

  private GatewaySender alterGatewaySender(Cache cache,
      GatewaySenderFunctionArgs gatewaySenderCreateArgs) {
    String gwId = gatewaySenderCreateArgs.getId();
    GatewaySender gateway = cache.getGatewaySender(gwId);
    if (gateway == null) {
      String message = String.format("Cannot find existing gateway sender with id '%s'.", gwId);
      throw new EntityNotFoundException(message);
    }

    GatewaySenderAttributes attributes = new GatewaySenderAttributes();

    Integer alertThreshold = gatewaySenderCreateArgs.getAlertThreshold();
    if (alertThreshold != null) {
      attributes.modifyAlertThreshold = true;
      attributes.alertThreshold = alertThreshold;
    }

    Integer batchSize = gatewaySenderCreateArgs.getBatchSize();
    if (batchSize != null) {
      attributes.modifyBatchSize = true;
      attributes.batchSize = batchSize;
    }

    Integer batchTimeInterval = gatewaySenderCreateArgs.getBatchTimeInterval();
    if (batchTimeInterval != null) {
      attributes.modifyBatchTimeInterval = true;
      attributes.batchTimeInterval = batchTimeInterval;
    }

    Boolean groupTransactionEvents = gatewaySenderCreateArgs.mustGroupTransactionEvents();
    if (groupTransactionEvents != null) {
      attributes.modifyGroupTransactionEvents = true;
      attributes.groupTransactionEvents = groupTransactionEvents;
    }

    List<String> gatewayEventFilters = gatewaySenderCreateArgs.getGatewayEventFilter();
    if (gatewayEventFilters != null) {
      attributes.modifyGatewayEventFilter = true;
      for (String filter : gatewayEventFilters) {
        attributes.addGatewayEventFilter(CallbackInstantiator.getObjectOfTypeFromClassName(filter,
            GatewayEventFilter.class));
      }
    }

    List<String> gatewayTransportFilters = gatewaySenderCreateArgs.getGatewayTransportFilter();
    if (gatewayTransportFilters != null) {
      attributes.modifyGatewayTransportFilter = true;
      for (String filter : gatewayTransportFilters) {
        attributes
            .addGatewayTransportFilter(CallbackInstantiator.getObjectOfTypeFromClassName(filter,
                GatewayTransportFilter.class));
      }
    }

    gateway.update(attributes);
    return gateway;
  }


  @Override
  public String getId() {
    return ID;
  }

}
