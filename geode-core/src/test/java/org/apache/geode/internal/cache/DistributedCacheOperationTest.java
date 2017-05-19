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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedCacheOperation.CacheOperationMessage;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

@Category(UnitTest.class)
public class DistributedCacheOperationTest {

  @Test
  public void shouldBeMockable() throws Exception {
    DistributedCacheOperation mockDistributedCacheOperation = mock(DistributedCacheOperation.class);
    CacheOperationMessage mockCacheOperationMessage = mock(CacheOperationMessage.class);
    Map<InternalDistributedMember, PersistentMemberID> persistentIds = new HashMap<>();
    when(mockDistributedCacheOperation.supportsDirectAck()).thenReturn(false);

    mockDistributedCacheOperation.waitForAckIfNeeded(mockCacheOperationMessage, persistentIds);

    verify(mockDistributedCacheOperation, times(1)).waitForAckIfNeeded(mockCacheOperationMessage,
        persistentIds);

    assertThat(mockDistributedCacheOperation.supportsDirectAck()).isFalse();
  }
}
