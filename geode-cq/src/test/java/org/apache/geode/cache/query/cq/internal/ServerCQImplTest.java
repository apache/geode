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
package org.apache.geode.cache.query.cq.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.InternalLogWriter;

public class ServerCQImplTest {
  private ServerCQImpl serverCq;
  private CqServiceImpl mockCqService;

  @Before
  @SuppressWarnings("deprecation")
  public void setUp() {
    mockCqService = mock(CqServiceImpl.class);
    when(mockCqService.getCache()).thenReturn(mock(Cache.class));
    when(mockCqService.getCache().getSecurityLoggerI18n())
        .thenReturn(mock(InternalLogWriter.class));
    serverCq = spy(
        new ServerCQImpl(mockCqService, "cqName", "SELECT * FROM /region", false, "test"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void invalidateCqResultKeysShouldClearCacheAndDisableInitializedFlag()
      throws QueryException {
    ClientProxyMembershipID mockClientProxyMembershipID = mock(ClientProxyMembershipID.class);
    doNothing().when(serverCq).validateCq();
    doReturn(mock(Query.class)).when(serverCq).constructServerSideQuery();
    LocalRegion mockLocalRegion = mock(LocalRegion.class);
    when(mockLocalRegion.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    when(mockCqService.getCache().getRegion(any())).thenReturn(mockLocalRegion);
    doNothing().when(serverCq).updateCqCreateStats();
    serverCq.registerCq(mockClientProxyMembershipID, null, CqStateImpl.INIT);

    // Initialize cache
    serverCq.addToCqResultKeys("key1");
    serverCq.setCqResultsCacheInitialized();
    assertThat(serverCq.isCqResultsCacheInitialized()).isTrue();
    assertThat(serverCq.isPartOfCqResult("key1")).isTrue();

    // Invalidate and assert results
    serverCq.invalidateCqResultKeys();
    assertThat(serverCq.isCqResultsCacheInitialized()).isFalse();
    assertThat(serverCq.isPartOfCqResult("key1")).isFalse();
  }
}
