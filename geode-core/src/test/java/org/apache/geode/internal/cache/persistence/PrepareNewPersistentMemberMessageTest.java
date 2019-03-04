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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.internal.cache.InternalCache;

public class PrepareNewPersistentMemberMessageTest {
  private final ClusterDistributionManager manager = mock(ClusterDistributionManager.class);
  private final ReplyMessage replyMessage = mock(ReplyMessage.class);
  private final InternalCache cache = mock(InternalCache.class);
  private final String regionPath = "regionPath";

  @Test
  public void processMessageSendReplyWithExceptionIfFailedWithCancelledException() {
    PrepareNewPersistentMemberMessage message = spy(new PrepareNewPersistentMemberMessage());
    when(manager.getExistingCache()).thenThrow(new CacheClosedException());
    doReturn(replyMessage).when(message).createReplyMessage();

    message.process(manager);

    verify(replyMessage).setException(any());
  }

  @Test
  public void processMessageSendReplyWithExceptionIfFailedWithRegionDestroyedException() {
    PrepareNewPersistentMemberMessage message =
        spy(new PrepareNewPersistentMemberMessage(regionPath, null, null, 1));
    when(manager.getExistingCache()).thenReturn(cache);
    when(cache.getRegion(regionPath)).thenThrow(new RegionDestroyedException("", ""));
    doReturn(replyMessage).when(message).createReplyMessage();

    message.process(manager);

    verify(replyMessage).setException(any());
  }

}
