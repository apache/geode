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
package org.apache.geode.internal.cache;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;

public class ExpireDisconnectedClientTransactionsMessageTest {
  private final ClusterDistributionManager dm = mock(ClusterDistributionManager.class);
  private final InternalCache cache = mock(InternalCache.class);
  private final TXManagerImpl txManager = mock(TXManagerImpl.class);
  private final InternalDistributedMember sender = mock(InternalDistributedMember.class);
  private final ExpireDisconnectedClientTransactionsMessage message =
      spy(new ExpireDisconnectedClientTransactionsMessage());
  private Version version = mock(Version.class);

  @Before
  public void setup() {
    when(dm.getCache()).thenReturn(cache);
    when(cache.getTXMgr()).thenReturn(txManager);
    doReturn(sender).when(message).getSender();
    when(sender.getVersionObject()).thenReturn(version);
  }

  @Test
  public void processMessageFromServerOfGeode170AndLaterVersionWillExpireDisconnectedClientTransactions() {
    when(version.compareTo(Version.GEODE_170)).thenReturn(1);

    message.process(dm);

    verify(txManager, times(1)).expireDisconnectedClientTransactions(any(), eq(false));
  }

  @Test
  public void processMessageFromServerOfPriorGeode170VersionWillRemoveExpiredClientTransactions() {
    when(version.compareTo(Version.GEODE_170)).thenReturn(-1);

    message.process(dm);

    verify(txManager, times(1)).removeExpiredClientTransactions(any());
  }
}
