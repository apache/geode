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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.DurableCqNamesResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * The ListDurableCqs class is a GemFire function used to collect all the durable client names
 * on the server
 * </p>
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.execute.Function
 * @see org.apache.geode.cache.execute.FunctionAdapter
 * @see org.apache.geode.cache.execute.FunctionContext
 * @see org.apache.geode.internal.InternalEntity
 * @see org.apache.geode.management.internal.cli.domain.IndexDetails
 * @since GemFire 7.0.1
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
