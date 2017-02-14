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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.logging.log4j.Logger;

public class GatewaySenderDestroyFunction extends FunctionAdapter implements InternalEntity {
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LogService.getLogger();
  private static final String ID = GatewaySenderDestroyFunction.class.getName();
  public static GatewaySenderDestroyFunction INSTANCE = new GatewaySenderDestroyFunction();

  @Override
  public void execute(FunctionContext context) {
    ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = CacheFactory.getAnyInstance();
    String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    GatewaySenderDestroyFunctionArgs gatewaySenderDestroyFunctionArgs =
        (GatewaySenderDestroyFunctionArgs) context.getArguments();

    try {
      GatewaySender gatewaySender =
          cache.getGatewaySender(gatewaySenderDestroyFunctionArgs.getId());
      if (gatewaySender != null) {
        gatewaySender.stop();
        gatewaySender.destroy();
      } else {
        throw new GatewaySenderException(
            "GateWaySender with Id  " + gatewaySenderDestroyFunctionArgs.getId() + " not found");
      }
      resultSender.lastResult(new CliFunctionResult(memberNameOrId, true,
          CliStrings.format(CliStrings.DESTROY_GATEWAYSENDER__MSG__GATEWAYSENDER_0_DESTROYED_ON_1,
              new Object[] {gatewaySenderDestroyFunctionArgs.getId(), memberNameOrId})));

    } catch (GatewaySenderException gse) {
      resultSender.lastResult(handleException(memberNameOrId, gse.getMessage(), gse));
    } catch (Exception e) {
      String exceptionMsg = e.getMessage();
      if (exceptionMsg == null) {
        exceptionMsg = CliUtil.stackTraceAsString(e);
      }
      resultSender.lastResult(handleException(memberNameOrId, exceptionMsg, e));
    }
  }

  private CliFunctionResult handleException(final String memberNameOrId, final String exceptionMsg,
      final Exception e) {
    if (e != null && logger.isDebugEnabled()) {
      logger.debug(e.getMessage(), e);
    }
    if (exceptionMsg != null) {
      return new CliFunctionResult(memberNameOrId, false, exceptionMsg);
    }

    return new CliFunctionResult(memberNameOrId);
  }

  @Override
  public String getId() {
    return ID;
  }

}
