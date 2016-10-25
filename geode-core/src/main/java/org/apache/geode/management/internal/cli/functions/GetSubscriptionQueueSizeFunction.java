/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.functions;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.internal.CqQueryVsdStats;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.SubscriptionQueueSizeResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/***
 * Function to get subscription-queue-size
 *
 */
public class GetSubscriptionQueueSizeFunction extends FunctionAdapter implements
InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    final Cache cache = CliUtil.getCacheIfExists();
    final String memberNameOrId = CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());
    String args[] = (String []) context.getArguments();
    String durableClientId = null, cqName = null;
    SubscriptionQueueSizeResult result = new SubscriptionQueueSizeResult(memberNameOrId);

    durableClientId = args[0];
    cqName = args[1];

    try {
      CacheClientNotifier cacheClientNotifier = CacheClientNotifier.getInstance();

      if (cacheClientNotifier != null) {
        CacheClientProxy cacheClientProxy = cacheClientNotifier.getClientProxy(durableClientId);
        //Check if the client is present or not
        if (cacheClientProxy != null) {
          if (cqName != null && !cqName.isEmpty()) {
            CqService cqService = cacheClientProxy.getCache().getCqService();
            if (cqService != null) {
              CqQuery cqQuery = cqService.getClientCqFromServer(cacheClientProxy.getProxyID(), cqName);
              if (cqQuery != null) {
                CqQueryVsdStats cqVsdStats = ((InternalCqQuery)cqQuery).getVsdStats();

                if (cqVsdStats != null) {
                  long queueSize = cqVsdStats.getNumHAQueuedEvents();
                  result.setSubscriptionQueueSize(queueSize);
                } else {
                  result.setErrorMessage(CliStrings.format(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE_CQ_STATS_NOT_FOUND, durableClientId, cqName));
                }
              } else {
                result.setErrorMessage(CliStrings.format(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE_CQ_NOT_FOUND, durableClientId, cqName));
              }
            } else {
              result.setErrorMessage(CliStrings.COUNT_DURABLE_CQ_EVENTS__NO__CQS__REGISTERED);
            }
          } else {
            result.setSubscriptionQueueSize(cacheClientNotifier.getDurableClientHAQueueSize(durableClientId));
          }
        } else {
          result.setErrorMessage(CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, durableClientId));
        }
      } else {
        result.setErrorMessage(CliStrings.NO_CLIENT_FOUND);
      }
    } catch (Exception e) {
      result.setExceptionMessage(e.getMessage());
    } finally {
      context.getResultSender().lastResult(result);
    }
  }

  @Override
  public String getId() {
    return GetSubscriptionQueueSizeFunction.class.getName();
  }

}
