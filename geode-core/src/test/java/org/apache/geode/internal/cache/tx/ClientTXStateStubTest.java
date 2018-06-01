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
package org.apache.geode.internal.cache.tx;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.internal.InternalPool;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXLockRequest;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXRegionLockRequestImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ClientTXStateStubTest {

  private InternalCache cache;
  private DistributionManager dm;
  private TXStateProxy stateProxy;
  private DistributedMember target;
  private LocalRegion region;
  private ServerRegionProxy serverRegionProxy;
  private CancelCriterion cancelCriterion;
  private InternalPool internalPool;

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    dm = mock(DistributionManager.class);
    stateProxy = mock(TXStateProxy.class);
    target = mock(DistributedMember.class);
    region = mock(LocalRegion.class);
    serverRegionProxy = mock(ServerRegionProxy.class);
    internalPool = mock(InternalPool.class);
    cancelCriterion = mock(CancelCriterion.class);

    when(region.getServerProxy()).thenReturn(serverRegionProxy);
    when(serverRegionProxy.getPool()).thenReturn(internalPool);
    when(stateProxy.getTxId()).thenReturn(mock(TXId.class));
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    doThrow(new CacheClosedException()).when(cancelCriterion).checkCancelInProgress(any());
  }

  @Test
  public void commitThrowsCancelExceptionIfCacheIsClosed() {
    ClientTXStateStub stub = spy(new ClientTXStateStub(cache, dm, stateProxy, target, region));

    when(stub.createTXLockRequest()).thenReturn(mock(TXLockRequest.class));
    when(stub.createTXRegionLockRequestImpl(any(), any()))
        .thenReturn(mock(TXRegionLockRequestImpl.class));

    assertThatThrownBy(() -> stub.commit()).isInstanceOf(CancelException.class);
  }

  @Test
  public void commitReleasesServerAffinityAfterCommit() {
    TXCommitMessage txCommitMessage = mock(TXCommitMessage.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);
    when(cache.getTxManager()).thenReturn(txManager);
    when(serverRegionProxy.commit(anyInt())).thenReturn(txCommitMessage);

    doNothing().when(cancelCriterion).checkCancelInProgress(null);
    doNothing().when(txManager).setTXState(null);
    ClientTXStateStub stub = spy(new ClientTXStateStub(cache, dm, stateProxy, target, region));

    InOrder order = inOrder(serverRegionProxy, internalPool, cancelCriterion);
    stub.commit();

    order.verify(serverRegionProxy).commit(anyInt());
    order.verify(internalPool).releaseServerAffinity();
    order.verify(cancelCriterion).checkCancelInProgress(null);
  }

  @Test
  public void commitReleasesServerAffinity_whenCommitThrowsAnException() {
    TXManagerImpl txManager = mock(TXManagerImpl.class);
    when(cache.getTxManager()).thenReturn(txManager);
    when(serverRegionProxy.commit(anyInt())).thenThrow(new InternalGemFireError());

    doNothing().when(cancelCriterion).checkCancelInProgress(null);
    doNothing().when(txManager).setTXState(null);
    ClientTXStateStub stub = spy(new ClientTXStateStub(cache, dm, stateProxy, target, region));

    InOrder order = inOrder(serverRegionProxy, internalPool);
    assertThatThrownBy(() -> stub.commit()).isInstanceOf(InternalGemFireError.class);

    order.verify(serverRegionProxy).commit(anyInt());
    order.verify(internalPool).releaseServerAffinity();
  }
}
