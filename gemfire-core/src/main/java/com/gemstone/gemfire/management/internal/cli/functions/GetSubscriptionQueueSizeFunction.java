/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.internal.CqQueryVsdStats;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.cq.InternalCqQuery;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.domain.SubscriptionQueueSizeResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/***
 * Function to get subscription-queue-size
 * @author bansods
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
