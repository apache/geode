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
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
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
   * This function is used to report all the gateway sender instances for a given sender
   * if all intances are stopped. In order to do so, it invokes the setMustQueueDroppedEvents()
   * method
   * on every gateway sender instance for the gateway sender passed as a parameter.
   * The function requires an Object[] as context.getArguments() in which
   * - the first parameter must be a Boolean stating of all gateway sender instances are stopped
   * - the second parameter must be a String containing the name of the sender.
   */
  @Override
  public void execute(FunctionContext context) {
    String memberName = context.getMemberName();
    try {
      Object[] arguments;

      Boolean stop = getStopFromArguments(context.getArguments());
      if (stop == null) {
        context.getResultSender().lastResult(new CliFunctionResult("", false,
            "Wrong arguments passed"));
        return;
      }
      String senderId = getSenderIdFromArguments(context.getArguments());
      if (senderId == null) {
        context.getResultSender().lastResult(new CliFunctionResult("", false,
            "Wrong arguments passed"));
        return;
      }

      Cache cache = context.getCache();
      GatewaySender sender = cache.getGatewaySender(senderId);

      if (sender == null) {
        context.getResultSender().lastResult(new CliFunctionResult(memberName, false,
            String.format("Sender '%s' does not exist", senderId)));
        return;
      }

      sender.setMustQueueDroppedEvents(!stop);
      XmlEntity xmlEntity = new XmlEntity(CacheXml.GATEWAY_SENDER, "name", senderId);
      context.getResultSender().lastResult(new CliFunctionResult(memberName, xmlEntity,
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

  private Object[] getArrayArguments(Object arguments) {
    if (arguments == null || !(arguments instanceof Object[])) {
      return null;
    }
    Object[] arrayArguments = (Object[]) arguments;

    if (arrayArguments.length < 2) {
      return null;
    }
    return arrayArguments;
  }

  private Boolean getStopFromArguments(Object arguments) {
    Object[] arrayArguments = getArrayArguments(arguments);

    if (arrayArguments == null) {
      return null;
    }

    if (!(arrayArguments[0] instanceof Boolean)) {
      return null;
    }
    return (Boolean) arrayArguments[0];
  }

  private String getSenderIdFromArguments(Object arguments) {
    Object[] arrayArguments = getArrayArguments(arguments);

    if (arrayArguments == null) {
      return null;
    }

    if (!(arrayArguments[1] instanceof String)) {
      return null;
    }
    return (String) arrayArguments[1];
  }
}
