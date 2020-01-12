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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class JtaAfterCompletionMessageTest {
  @Test
  public void testAfterCompletionNotInvokedIfJTACompleted() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    TXManagerImpl txMgr = mock(TXManagerImpl.class);
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);
    TXId txId = mock(TXId.class);

    when(distributionManager.getCache()).thenReturn(cache);
    when(cache.getTXMgr()).thenReturn(txMgr);
    when(txMgr.getRecentlyCompletedMessage(txId)).thenReturn(mock(TXCommitMessage.class));
    when(txMgr.getTXState()).thenReturn(mock(TXStateProxyImpl.class));

    JtaAfterCompletionMessage message = new JtaAfterCompletionMessage();
    JtaAfterCompletionMessage spyMessage = spy(message);
    when(spyMessage.getSender()).thenReturn(mock(InternalDistributedMember.class));

    spyMessage.operateOnTx(txId, distributionManager);
    verify(txMgr, never()).getTXState();
  }

}
