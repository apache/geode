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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;

public class FetchKeysMessageTest {
  @Mock(answer = RETURNS_DEEP_STUBS)
  private InternalCache cache;
  @Mock
  private DistributionManager distributionManager;
  @Mock(answer = RETURNS_DEEP_STUBS)
  private InternalDistributedSystem distributedSystem;
  @Mock
  private InternalDistributedMember recipient;
  @Mock(answer = RETURNS_DEEP_STUBS)
  private PartitionedRegion region;
  @Mock
  private TXStateProxy txStateProxy;
  @Captor
  private ArgumentCaptor<FetchKeysMessage> sentMessage;
  private TXManagerImpl originalTxManager;
  private TXManagerImpl txManager;

  @Before
  public void setup() {
    initMocks(this);

    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(region.getCache()).thenReturn(cache);
    when(region.getDistributionManager()).thenReturn(distributionManager);
    when(txStateProxy.isInProgress()).thenReturn(true);

    originalTxManager = TXManagerImpl.getCurrentInstanceForTest();
    // The constructor sets the new tx manager as currentInstance
    txManager = spy(new TXManagerImpl(mock(CachePerfStats.class), cache, disabledClock()));
    txManager.setTXState(txStateProxy);
    txManager.setDistributed(false);

    when(cache.getTxManager()).thenReturn(txManager);
  }

  @After
  public void restoreTxManager() {
    TXManagerImpl.setCurrentInstanceForTest(originalTxManager);
  }

  @Test
  public void sendsWithTransactionPaused_ifTransactionIsHostedLocally() throws Exception {
    // Transaction is locally hosted
    when(txStateProxy.isRealDealLocal()).thenReturn(true);
    when(txStateProxy.isDistTx()).thenReturn(false);

    FetchKeysMessage.send(recipient, region, 1, false);

    InOrder inOrder = inOrder(txManager, distributionManager);
    inOrder.verify(txManager, times(1)).pauseTransaction();
    inOrder.verify(distributionManager, times(1)).putOutgoing(sentMessage.capture());
    inOrder.verify(txManager, times(1)).unpauseTransaction(same(txStateProxy));

    assertThat(sentMessage.getValue().getTXUniqId()).isEqualTo(TXManagerImpl.NOTX);
  }

  @Test
  public void sendsWithoutPausingTransaction_ifTransactionIsNotHostedLocally() throws Exception {
    // Transaction is not locally hosted
    when(txStateProxy.isRealDealLocal()).thenReturn(false);

    int uniqueId = 99;
    TXId txID = new TXId(recipient, uniqueId);
    when(txStateProxy.getTxId()).thenReturn(txID);

    FetchKeysMessage.send(recipient, region, 1, false);

    verify(distributionManager, times(1)).putOutgoing(sentMessage.capture());
    assertThat(sentMessage.getValue().getTXUniqId()).isEqualTo(uniqueId);
    verify(txManager, never()).pauseTransaction();
    verify(txManager, never()).unpauseTransaction(any());
  }
}
