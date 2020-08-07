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
package org.apache.geode.management.internal.configuration.functions;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class GatewaySenderManageFunction implements InternalFunction {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = -6022504778575202026L;

  @Immutable
  public static final GatewaySenderManageFunction INSTANCE = new GatewaySenderManageFunction();
  private static final String ID = GatewaySenderManageFunction.class.getName();

  @Override
  public boolean hasResult() {
    return true;
  }

  /**
   * This function is used to report to each gateway sender instance for a given sender
   * if all intances are stopped. In order to do so, it invokes the setAllInstancesStopped()
   * method on every gateway sender instance for the gateway sender passed as a parameter.
   * The function requires an Object[] as context.getArguments() in which
   * - the first parameter must be a Boolean stating of all gateway sender instances are stopped
   * - the second parameter must be a String containing the name of the sender.
   */
  @Override
  public void execute(FunctionContext context) {
    String memberName = context.getMemberName();
    try {
      Object[] arguments = (Object[]) context.getArguments();
      String senderId = (String) arguments[0];
      Boolean stop = (Boolean) arguments[1];
      Cache cache = context.getCache();
      GatewaySender sender = cache.getGatewaySender(senderId);
      sender.setAllInstancesStopped(stop);
      context.getResultSender().lastResult(new CliFunctionResult(memberName,
          String.format("Set mustQueueTempDroppedEvents to '%b', for Sender '%s'", !stop,
              senderId)));
    } catch (IllegalStateException ex) {
      context.getResultSender()
          .lastResult(new CliFunctionResult(memberName, false, ex.getMessage()));
    } catch (Exception ex) {
      LogService.getLogger().error(ex.getMessage(), ex);
      context.getResultSender().lastResult(new CliFunctionResult(memberName, ex, ex.getMessage()));
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
