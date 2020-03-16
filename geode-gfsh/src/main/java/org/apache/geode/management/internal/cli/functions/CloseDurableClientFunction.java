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
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

/***
 * Function to close a durable client
 *
 */
public class CloseDurableClientFunction implements InternalFunction<String> {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext<String> context) {
    String durableClientId = context.getArguments();
    final Cache cache = context.getCache();
    final String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    context.getResultSender().lastResult(createFunctionResult(memberNameOrId, durableClientId));
  }

  private CliFunctionResult createFunctionResult(String memberNameOrId, String durableClientId) {
    try {
      CacheClientNotifier cacheClientNotifier = CacheClientNotifier.getInstance();

      if (cacheClientNotifier == null) {
        return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
            CliStrings.NO_CLIENT_FOUND);
      }

      CacheClientProxy ccp = cacheClientNotifier.getClientProxy(durableClientId);
      if (ccp == null) {
        return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
            CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, durableClientId));
      }

      boolean isClosed = cacheClientNotifier.closeDurableClientProxy(durableClientId);
      if (isClosed) {
        return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.OK,
            CliStrings.format(CliStrings.CLOSE_DURABLE_CLIENTS__SUCCESS, durableClientId));
      } else {
        return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
            CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, durableClientId));
      }
    } catch (Exception e) {
      return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
          e.getMessage());
    }
  }

}
