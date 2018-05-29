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
package org.apache.geode.cache.query.internal.cq;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CqServiceImplJUnitTest {

  @Test
  public void closedCacheClientProxyInExecuteCqShouldThrowCQException() {
    InternalCache cache = mock(InternalCache.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    doNothing().when(cancelCriterion).checkCancelInProgress(null);
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    when(cache.getDistributedSystem()).thenReturn(distributedSystem);


    ClientProxyMembershipID clientProxyMembershipID = mock(ClientProxyMembershipID.class);
    CacheClientNotifier cacheClientNotifier = mock(CacheClientNotifier.class);
    when(cacheClientNotifier.getClientProxy(clientProxyMembershipID, true)).thenReturn(null);

    CqServiceImpl cqService = new CqServiceImpl(cache);
    try {
      cqService.getCacheClientProxy(clientProxyMembershipID, cacheClientNotifier);
      fail();
    } catch (Exception ex) {
      if (!(ex instanceof CqException && ex.getMessage()
          .contains(LocalizedStrings.cq_CACHE_CLIENT_PROXY_IS_NULL.toLocalizedString()))) {
        fail();
      }
    }

  }
}
