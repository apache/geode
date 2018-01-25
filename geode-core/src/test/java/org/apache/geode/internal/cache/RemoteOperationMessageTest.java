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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class RemoteOperationMessageTest {

  private RemoteOperationMessage msg; // the class under test

  private InternalDistributedMember recipient;
  private InternalDistributedMember sender;
  private final String regionPath = "regionPath";
  private ReplyProcessor21 processor;

  private GemFireCacheImpl cache;
  private InternalDistributedSystem system;
  private ClusterDistributionManager dm;
  private LocalRegion r;
  private TXManagerImpl txMgr;
  private long startTime = 0;
  private TXStateProxy tx;

  @Before
  public void setUp() throws Exception {
    cache = Fakes.cache();
    system = cache.getSystem();
    dm = (ClusterDistributionManager) system.getDistributionManager();
    r = mock(LocalRegion.class);
    txMgr = mock(TXManagerImpl.class);
    tx = mock(TXStateProxyImpl.class);
    when(cache.getRegionByPathForProcessing(regionPath)).thenReturn(r);
    when(cache.getTxManager()).thenReturn(txMgr);

    sender = mock(InternalDistributedMember.class);

    recipient = mock(InternalDistributedMember.class);
    processor = mock(ReplyProcessor21.class);
    // make it a spy to aid verification
    msg = spy(new TestableRemoteOperationMessage(recipient, regionPath, processor));
  }

  @Test
  public void messageWithNoTXPerformsOnRegion() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(null);
    msg.setSender(sender);

    msg.process(dm);

    verify(msg, times(1)).operateOnRegion(dm, r, startTime);
  }

  @Test
  public void messageForNotFinishedTXPerformsOnRegion() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(tx.isInProgress()).thenReturn(true);
    msg.setSender(sender);

    msg.process(dm);

    verify(msg, times(1)).operateOnRegion(dm, r, startTime);
  }

  @Test
  public void messageForFinishedTXDoesNotPerformOnRegion() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(tx.isInProgress()).thenReturn(false);
    msg.setSender(sender);

    msg.process(dm);

    verify(msg, times(0)).operateOnRegion(dm, r, startTime);
  }

  @Test
  public void noNewTxProcessingAfterTXManagerImplClosed() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(txMgr.isClosed()).thenReturn(true);
    msg.setSender(sender);

    msg.process(dm);

    verify(msg, times(0)).operateOnRegion(dm, r, startTime);
  }

  private static class TestableRemoteOperationMessage extends RemoteOperationMessage {

    public TestableRemoteOperationMessage(InternalDistributedMember recipient, String regionPath,
        ReplyProcessor21 processor) {
      super(recipient, regionPath, processor);
    }

    public TestableRemoteOperationMessage(Set recipients, String regionPath,
        ReplyProcessor21 processor) {
      super(recipients, regionPath, processor);
    }

    @Override
    public int getDSFID() {
      return 0;
    }

    @Override
    protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
        throws RemoteOperationException {
      return false;
    }

  }
}
