/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.domain.DurableCqNamesResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/**
 * The ListDurableCqs class is a GemFire function used to collect all the durable client names
 * on the server
 * </p>
 * @author jhuynh
 * @author bansods
 * @see com.gemstone.gemfire.cache.Cache
 * @see com.gemstone.gemfire.cache.execute.Function
 * @see com.gemstone.gemfire.cache.execute.FunctionAdapter
 * @see com.gemstone.gemfire.cache.execute.FunctionContext
 * @see com.gemstone.gemfire.internal.InternalEntity
 * @see com.gemstone.gemfire.management.internal.cli.domain.IndexDetails
 * @since 7.0.1
 */
@SuppressWarnings("unused")
public class ListDurableCqNamesFunction extends FunctionAdapter implements InternalEntity {
  private static final long serialVersionUID = 1L;

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public String getId() {
    return ListDurableCqNamesFunction.class.getName();
  }

  public void execute(final FunctionContext context) {
    final Cache cache = getCache();
    final DistributedMember member = cache.getDistributedSystem().getDistributedMember();
    String memberNameOrId = CliUtil.getMemberNameOrId(member);
    DurableCqNamesResult result = new DurableCqNamesResult(memberNameOrId);

    String durableClientId = (String)context.getArguments();

    try {
      CacheClientNotifier ccn = CacheClientNotifier.getInstance();
      if (ccn != null) {
        CacheClientProxy ccp = ccn.getClientProxy(durableClientId);
        if (ccp != null) {
          CqService cqService = ccp.getCache().getCqService();
          if (cqService != null && cqService.isRunning()) {
            List<String> durableCqNames = cqService.getAllDurableClientCqs(ccp.getProxyID());
            if (durableCqNames != null && !durableCqNames.isEmpty()) {
              result.setCqNamesList(new ArrayList<String>(durableCqNames));
            } else {
              result.setErrorMessage(CliStrings.format(CliStrings.LIST_DURABLE_CQS__NO__CQS__FOR__CLIENT, durableClientId));
            }
          } else {
            result.setErrorMessage(CliStrings.LIST_DURABLE_CQS__NO__CQS__REGISTERED);
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
}
