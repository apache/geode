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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedCacheOperation.CacheOperationMessage;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.tier.MessageType;

public class DistributedCacheOperationTest {

  @Test
  public void shouldBeMockable() throws Exception {
    DistributedCacheOperation mockDistributedCacheOperation = mock(DistributedCacheOperation.class);
    @SuppressWarnings("unused") // forces CacheOperationMessage to be mockable
    CacheOperationMessage mockCacheOperationMessage = mock(CacheOperationMessage.class);
    Map<InternalDistributedMember, PersistentMemberID> persistentIds = new HashMap<>();
    when(mockDistributedCacheOperation.supportsDirectAck()).thenReturn(false);

    mockDistributedCacheOperation.waitForAckIfNeeded(persistentIds);

    verify(mockDistributedCacheOperation, times(1)).waitForAckIfNeeded(
        persistentIds);

    assertThat(mockDistributedCacheOperation.supportsDirectAck()).isFalse();
  }

  /**
   * The startOperation and endOperation methods of DistributedCacheOperation record the
   * beginning and end of distribution of an operation. If startOperation is invoked it
   * is essential that endOperation be invoked or the state-flush operation will hang.<br>
   * This test ensures that if distribution of the operation throws an exception then
   * endOperation is correctly invoked before allowing the exception to escape the startOperation
   * method.
   */
  @Test
  public void endOperationIsInvokedOnDistributionError() {
    DistributedRegion region = mock(DistributedRegion.class);
    CacheDistributionAdvisor advisor = mock(CacheDistributionAdvisor.class);
    when(region.getDistributionAdvisor()).thenReturn(advisor);
    TestOperation operation = new TestOperation(null);
    operation.region = region;
    try {
      operation.startOperation();
    } catch (RuntimeException e) {
      assertEquals("boom", e.getMessage());
    }
    assertTrue(operation.endOperationInvoked);
  }

  static class TestOperation extends DistributedCacheOperation {
    boolean endOperationInvoked;
    DistributedRegion region;

    public TestOperation(CacheEvent<?, ?> event) {
      super(event);
    }

    @Override
    public DistributedRegion getRegion() {
      return region;
    }

    @Override
    public boolean containsRegionContentChange() {
      return true;
    }

    @Override
    public void endOperation(long viewVersion) {
      endOperationInvoked = true;
      super.endOperation(viewVersion);
    }

    @Override
    protected CacheOperationMessage createMessage() {
      return null;
    }

    @Override
    protected void _distribute() {
      throw new RuntimeException("boom");
    }
  }

  @Test
  public void testDoRemoveDestroyTokensFromCqResultKeys() {
    Object key = new Object();
    HashMap<Long, Integer> hashMap = new HashMap<>();
    hashMap.put(1L, MessageType.LOCAL_DESTROY);
    EntryEventImpl baseEvent = mock(EntryEventImpl.class);
    ServerCQ serverCQ = mock(ServerCQ.class);
    FilterRoutingInfo.FilterInfo filterInfo = mock(FilterRoutingInfo.FilterInfo.class);
    DistributedCacheOperation distributedCacheOperation =
        new DestroyOperation(baseEvent);
    when(baseEvent.getKey()).thenReturn(key);
    when(filterInfo.getCQs()).thenReturn(hashMap);
    when(serverCQ.getFilterID()).thenReturn(1L);
    doNothing().when(serverCQ).removeFromCqResultKeys(isA(Object.class), isA(Boolean.class));

    distributedCacheOperation.doRemoveDestroyTokensFromCqResultKeys(filterInfo, serverCQ);

    verify(serverCQ, times(1)).removeFromCqResultKeys(key, true);
  }
}
