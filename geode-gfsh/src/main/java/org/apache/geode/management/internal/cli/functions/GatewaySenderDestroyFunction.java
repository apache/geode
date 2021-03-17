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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class GatewaySenderDestroyFunction
    implements InternalFunction<GatewaySenderDestroyFunctionArgs> {
  private static final long serialVersionUID = 1L;
  private static final String ID = GatewaySenderDestroyFunction.class.getName();
  @Immutable
  public static final GatewaySenderDestroyFunction INSTANCE = new GatewaySenderDestroyFunction();

  @Override
  @SuppressWarnings("deprecation")
  public void execute(FunctionContext<GatewaySenderDestroyFunctionArgs> context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = context.getCache();
    String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    GatewaySenderDestroyFunctionArgs gatewaySenderDestroyFunctionArgs =
        context.getArguments();

    String senderId = gatewaySenderDestroyFunctionArgs.getId();
    boolean ifExists = gatewaySenderDestroyFunctionArgs.isIfExists();
    GatewaySender gatewaySender = cache.getGatewaySender(senderId);
    if (gatewaySender == null) {
      String message = "Gateway sender " + senderId + " not found.";
      if (ifExists) {
        resultSender
            .lastResult(new CliFunctionResult(memberNameOrId, true, "Skipping: " + message));
      } else {
        resultSender.lastResult(new CliFunctionResult(memberNameOrId, false, message));
      }
      return;
    }

    try {
      gatewaySender.stop();
      gatewaySender.destroy();
      XmlEntity xmlEntity = new XmlEntity(CacheXml.GATEWAY_SENDER, "id", senderId);
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity,
          String.format("GatewaySender \"%s\" destroyed on \"%s\"", senderId, memberNameOrId)));
    } catch (Exception e) {
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, e, ""));
    }
  }

  @Override
  public String getId() {
    return ID;
  }

}
