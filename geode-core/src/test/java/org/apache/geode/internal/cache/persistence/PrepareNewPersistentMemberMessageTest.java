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
package org.apache.geode.internal.cache.persistence;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InternalCache;

public class PrepareNewPersistentMemberMessageTest {
  private final ClusterDistributionManager manager = mock(ClusterDistributionManager.class);
  private final ReplyMessage replyMessage = mock(ReplyMessage.class);
  private final InternalCache cache = mock(InternalCache.class);
  private final String regionPath = "regionPath";
  private final DistributedRegion region = mock(DistributedRegion.class);
  private final PersistenceAdvisor advisor = mock(PersistenceAdvisor.class);
  private final InternalDistributedMember sender = mock(InternalDistributedMember.class);

  @Test
  public void doesNotSendReplyIfFailedWithCancelledException() {
    PrepareNewPersistentMemberMessage message = spy(new PrepareNewPersistentMemberMessage());
    when(manager.getExistingCache()).thenThrow(new CacheClosedException());
    doReturn(replyMessage).when(message).createReplyMessage();

    message.process(manager);

    verify(manager, never()).putOutgoing(replyMessage);
  }

  @Test
  public void sendReplyIfNoException() {
    PrepareNewPersistentMemberMessage message = spy(new PrepareNewPersistentMemberMessage());
    doReturn(sender).when(message).getSender();
    doReturn(replyMessage).when(message).createReplyMessage();
    doReturn(regionPath).when(message).getRegionPath();
    when(manager.getExistingCache()).thenReturn(cache);
    when(cache.getRegion(regionPath)).thenReturn(region);
    when(region.getPersistenceAdvisor()).thenReturn(advisor);

    message.process(manager);

    verify(manager).putOutgoing(replyMessage);
  }
}
