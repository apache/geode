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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.InternalDataView;
import org.apache.geode.internal.cache.PartitionedRegion;

public class RemoveAllPRMessageTest {

  @Test
  public void shouldBeMockable() throws Exception {
    RemoveAllPRMessage mockRemoveAllPRMessage = mock(RemoveAllPRMessage.class);
    StringBuilder stringBuilder = new StringBuilder();

    mockRemoveAllPRMessage.appendFields(stringBuilder);

    verify(mockRemoveAllPRMessage, times(1)).appendFields(stringBuilder);
  }


  @Test
  public void doPostRemoveAllCallsCheckReadinessBeforeAndAfter() throws Exception {
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    DistributedRemoveAllOperation distributedRemoveAllOperation =
        mock(DistributedRemoveAllOperation.class);
    BucketRegion bucketRegion = mock(BucketRegion.class);
    InternalDataView internalDataView = mock(InternalDataView.class);
    when(bucketRegion.getDataView()).thenReturn(internalDataView);
    RemoveAllPRMessage removeAllPRMessage = new RemoveAllPRMessage();

    removeAllPRMessage.doPostRemoveAll(partitionedRegion, distributedRemoveAllOperation,
        bucketRegion, true);

    InOrder inOrder = inOrder(partitionedRegion, internalDataView);
    inOrder.verify(partitionedRegion).checkReadiness();
    inOrder.verify(internalDataView).postRemoveAll(any(), any(), any());
    inOrder.verify(partitionedRegion).checkReadiness();
  }
}
