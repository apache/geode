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
package org.apache.geode.internal.cache.wan.serial;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.wan.serial.BatchDestroyOperation.DestroyMessage;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class DestroyMessageTest {

  @Test
  public void shouldBeMockable() throws Exception {
    DestroyMessage mockDestroyMessageX = mock(DestroyMessage.class);
    InternalCacheEvent mockInternalCacheEvent = mock(InternalCacheEvent.class);
    DistributedRegion mockDistributedRegion = mock(DistributedRegion.class);

    when(mockDestroyMessageX.createEvent(eq(mockDistributedRegion)))
        .thenReturn(mockInternalCacheEvent);

    assertThat(mockDestroyMessageX.createEvent(mockDistributedRegion))
        .isSameAs(mockInternalCacheEvent);
  }
}
