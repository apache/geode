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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PartitionMessageTest {

  private GemFireCacheImpl cache;
  private PartitionMessage msg;
  private ClusterDistributionManager dm;
  private PartitionedRegion pr;
  private TXManagerImpl txMgr;
  private long startTime = 1;
  private TXStateProxy tx;
  private DistributionAdvisor advisor;

  @Before
  public void setUp() throws Exception {
    cache = Fakes.cache();
    dm = mock(ClusterDistributionManager.class);
    msg = mock(PartitionMessage.class);
    pr = mock(PartitionedRegion.class);
    txMgr = mock(TXManagerImpl.class);
    tx = mock(TXStateProxyImpl.class);
    advisor = mock(DistributionAdvisor.class);

    when(msg.checkCacheClosing(dm)).thenReturn(false);
    when(msg.checkDSClosing(dm)).thenReturn(false);
    when(msg.getPartitionedRegion()).thenReturn(pr);
    when(msg.getStartPartitionMessageProcessingTime(pr)).thenReturn(startTime);
    when(msg.getTXManagerImpl(cache)).thenReturn(txMgr);
    when(dm.getCache()).thenReturn(cache);
    when(pr.getDistributionAdvisor()).thenReturn(advisor);
    when(advisor.isInitialized()).thenReturn(true);

    doAnswer(CALLS_REAL_METHODS).when(msg).process(dm);
  }

  @Test
  public void shouldBeMockable() throws Exception {
    PartitionMessage mockPartitionMessage = mock(PartitionMessage.class);
    InternalDistributedMember mockInternalDistributedMember = mock(InternalDistributedMember.class);

    when(mockPartitionMessage.getMemberToMasqueradeAs()).thenReturn(mockInternalDistributedMember);

    assertThat(mockPartitionMessage.getMemberToMasqueradeAs())
        .isSameAs(mockInternalDistributedMember);
  }

  @Test
  public void messageWithNoTXPerformsOnRegion() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(null);
    msg.process(dm);

    verify(msg, times(1)).operateOnPartitionedRegion(dm, pr, startTime);
  }

  @Test
  public void messageForNotFinishedTXPerformsOnRegion() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(tx.isInProgress()).thenReturn(true);
    msg.process(dm);

    verify(msg, times(1)).operateOnPartitionedRegion(dm, pr, startTime);
  }

  @Test
  public void messageForFinishedTXDoesNotPerformOnRegion() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(tx.isInProgress()).thenReturn(false);
    msg.process(dm);

    verify(msg, times(0)).operateOnPartitionedRegion(dm, pr, startTime);
  }

  @Test
  public void noNewTxProcessingAfterTXManagerImplClosed() throws Exception {
    txMgr = new TXManagerImpl(null, cache);
    when(msg.getPartitionedRegion()).thenReturn(pr);
    when(msg.getStartPartitionMessageProcessingTime(pr)).thenReturn(startTime);
    when(msg.getTXManagerImpl(cache)).thenReturn(txMgr);
    when(msg.canParticipateInTransaction()).thenReturn(true);
    when(msg.canStartRemoteTransaction()).thenReturn(true);

    msg.process(dm);

    txMgr.close();

    msg.process(dm);

    verify(msg, times(1)).operateOnPartitionedRegion(dm, pr, startTime);
  }
}
