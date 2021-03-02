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
 * Function to close a durable cq
 *
 */
public class CloseDurableCqFunction implements InternalFunction<String[]> {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext<String[]> context) {

    final Cache cache = context.getCache();
    final String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());
    String[] args = context.getArguments();
    String durableClientId = args[0];
    String cqName = args[1];

    context.getResultSender()
        .lastResult(createFunctionResult(memberNameOrId, durableClientId, cqName));
  }

  private CliFunctionResult createFunctionResult(String memberNameOrId, String durableClientId,
      String cqName) {
    CacheClientNotifier cacheClientNotifier = CacheClientNotifier.getInstance();
    try {
      if (cacheClientNotifier == null) {
        return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
            CliStrings.NO_CLIENT_FOUND);
      }

      CacheClientProxy cacheClientProxy = cacheClientNotifier.getClientProxy(durableClientId);
      if (cacheClientProxy == null) {
        return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
            CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, durableClientId));
      }

      if (cacheClientNotifier.closeClientCq(durableClientId, cqName)) {
        return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.OK,
            CliStrings.format(CliStrings.CLOSE_DURABLE_CQS__SUCCESS, cqName, durableClientId));
      } else {
        return new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
            CliStrings.format(CliStrings.CLOSE_DURABLE_CQS__UNABLE__TO__CLOSE__CQ, cqName,
                durableClientId));
      }
    } catch (Exception e) {
      return new CliFunctionResult(memberNameOrId, e);
    }
  }

}
