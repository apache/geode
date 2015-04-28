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
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.domain.MemberResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/***
 * Function to close a durable client
 * @author bansods
 *
 */
public class CloseDurableClientFunction extends FunctionAdapter implements
InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    String durableClientId = (String)context.getArguments();
    final Cache cache = CliUtil.getCacheIfExists();
    final String memberNameOrId = CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());
    MemberResult memberResult = new MemberResult(memberNameOrId);

    try {
      CacheClientNotifier cacheClientNotifier = CacheClientNotifier.getInstance();

      if (cacheClientNotifier != null) {
        CacheClientProxy ccp = cacheClientNotifier.getClientProxy(durableClientId);
        if (ccp != null) {
          boolean isClosed = cacheClientNotifier.closeDurableClientProxy(durableClientId);
          if (isClosed) {
            memberResult.setSuccessMessage(CliStrings.format(CliStrings.CLOSE_DURABLE_CLIENTS__SUCCESS, durableClientId));
          } else {
            memberResult.setErrorMessage(CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, durableClientId));
          }
        } else {
          memberResult.setErrorMessage(CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, durableClientId));
        }
      } else {
        memberResult.setErrorMessage(CliStrings.NO_CLIENT_FOUND);
      }
    } catch (Exception e) {
      memberResult.setExceptionMessage(e.getMessage());
    } finally {
      context.getResultSender().lastResult(memberResult);
    }
  }

  @Override
  public String getId() {
    return CloseDurableClientFunction.class.getName();
  }

}
