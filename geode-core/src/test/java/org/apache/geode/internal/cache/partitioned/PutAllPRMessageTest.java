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
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.InternalDataView;
import org.apache.geode.internal.cache.PartitionedRegion;

public class PutAllPRMessageTest {

  @Test
  public void doPostPutAllCallsCheckReadinessBeforeAndAfter() throws Exception {
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    DistributedPutAllOperation distributedPutAllOperation = mock(DistributedPutAllOperation.class);
    BucketRegion bucketRegion = mock(BucketRegion.class);
    InternalDataView internalDataView = mock(InternalDataView.class);
    when(bucketRegion.getDataView()).thenReturn(internalDataView);
    PutAllPRMessage putAllPRMessage = new PutAllPRMessage();

    putAllPRMessage.doPostPutAll(partitionedRegion, distributedPutAllOperation, bucketRegion, true);

    InOrder inOrder = inOrder(partitionedRegion, internalDataView);
    inOrder.verify(partitionedRegion).checkReadiness();
    inOrder.verify(internalDataView).postPutAll(any(), any(), any());
    inOrder.verify(partitionedRegion).checkReadiness();
  }
}
