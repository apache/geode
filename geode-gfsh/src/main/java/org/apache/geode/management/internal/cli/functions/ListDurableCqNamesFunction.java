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

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 * The ListDurableCqs class is a GemFire function used to collect all the durable client names on
 * the server
 * </p>
 *
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.execute.Function
 * @see org.apache.geode.cache.execute.FunctionContext
 * @see org.apache.geode.internal.InternalEntity
 * @see org.apache.geode.management.internal.cli.domain.IndexDetails
 * @since GemFire 7.0.1
 */
@SuppressWarnings("unused")
public class ListDurableCqNamesFunction implements InternalFunction<String> {
  private static final long serialVersionUID = 1L;

  @Override
  public String getId() {
    return ListDurableCqNamesFunction.class.getName();
  }

  @Override
  public void execute(final FunctionContext<String> context) {
    final Cache cache = context.getCache();
    final DistributedMember member = cache.getDistributedSystem().getDistributedMember();
    String memberNameOrId = CliUtil.getMemberNameOrId(member);

    String durableClientId = context.getArguments();

    context.getResultSender().lastResult(createFunctionResult(memberNameOrId, durableClientId));
  }

  private List<CliFunctionResult> createFunctionResult(String memberNameOrId,
      String durableClientId) {
    List<CliFunctionResult> results = new ArrayList<>();

    try {
      CacheClientNotifier ccn = CacheClientNotifier.getInstance();
      if (ccn == null) {
        results.add(new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.IGNORABLE,
            CliStrings.NO_CLIENT_FOUND));
        return results;
      }

      CacheClientProxy ccp = ccn.getClientProxy(durableClientId);
      if (ccp == null) {
        results.add(new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.IGNORABLE,
            CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, durableClientId)));
        return results;
      }

      CqService cqService = ccp.getCache().getCqService();
      if (cqService == null || !cqService.isRunning()) {
        results.add(new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.IGNORABLE,
            CliStrings.LIST_DURABLE_CQS__NO__CQS__REGISTERED));
        return results;
      }

      List<String> durableCqNames = cqService.getAllDurableClientCqs(ccp.getProxyID());
      if (durableCqNames == null || durableCqNames.isEmpty()) {
        results.add(new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.IGNORABLE,
            CliStrings
                .format(CliStrings.LIST_DURABLE_CQS__NO__CQS__FOR__CLIENT, durableClientId)));
        return results;
      }

      for (String cqName : durableCqNames) {
        results.add(new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.OK,
            cqName));
      }
      return results;
    } catch (Exception e) {
      results.add(new CliFunctionResult(memberNameOrId, CliFunctionResult.StatusState.ERROR,
          e.getMessage()));
      return results;
    }
  }
}
