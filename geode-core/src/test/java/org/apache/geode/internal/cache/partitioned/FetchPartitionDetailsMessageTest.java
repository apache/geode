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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class FetchPartitionDetailsMessageTest {

  @Test
  public void shouldBeMockable() throws Exception {
    FetchPartitionDetailsMessage mockFetchPartitionDetailsMessage =
        mock(FetchPartitionDetailsMessage.class);
    DistributionManager mockDistributionManager = mock(DistributionManager.class);
    PartitionedRegion mockPartitionedRegion = mock(PartitionedRegion.class);
    long startTime = System.currentTimeMillis();
    Object key = new Object();

    when(mockFetchPartitionDetailsMessage.operateOnPartitionedRegion(eq(mockDistributionManager),
        eq(mockPartitionedRegion), eq(startTime))).thenReturn(true);

    assertThat(mockFetchPartitionDetailsMessage.operateOnPartitionedRegion(mockDistributionManager,
        mockPartitionedRegion, startTime)).isTrue();
  }
}
