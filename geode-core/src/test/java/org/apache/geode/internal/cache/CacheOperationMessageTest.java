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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.DistributedCacheOperation.CacheOperationMessage;

public class CacheOperationMessageTest {

  @Test
  public void shouldBeMockable() throws Exception {
    CacheOperationMessage mockCacheOperationMessage = mock(CacheOperationMessage.class);
    ClusterDistributionManager mockDistributionManager = mock(ClusterDistributionManager.class);

    when(mockCacheOperationMessage.supportsDirectAck()).thenReturn(true);
    when(mockCacheOperationMessage._mayAddToMultipleSerialGateways(eq(mockDistributionManager)))
        .thenReturn(true);

    mockCacheOperationMessage.process(mockDistributionManager);

    verify(mockCacheOperationMessage, times(1)).process(mockDistributionManager);

    assertThat(mockCacheOperationMessage.supportsDirectAck()).isTrue();
    assertThat(mockCacheOperationMessage._mayAddToMultipleSerialGateways(mockDistributionManager))
        .isTrue();
  }

  @Test
  public void inhibitAllNotificationsShouldOnlybBeSetInextBits() throws Exception {
    // create an UpdateMessage for a bucket region
    UpdateOperation.UpdateMessage updateMessage = mock(UpdateOperation.UpdateMessage.class);
    byte[] memId = {1, 2, 3};
    EventID eventId = new EventID(memId, 11, 12, 13);
    BucketRegion br = mock(BucketRegion.class);
    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(br.getPartitionedRegion()).thenReturn(pr);
    when(pr.isUsedForPartitionedRegionBucket()).thenReturn(true);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    DistributionConfig dc = mock(DistributionConfig.class);
    when(br.getSystem()).thenReturn(system);
    when(system.getConfig()).thenReturn(dc);
    when(dc.getDeltaPropagation()).thenReturn(false);
    KeyInfo keyInfo = new KeyInfo("key1", null, null);
    when(br.getKeyInfo(eq("key1"), any(), any())).thenReturn(keyInfo);
    EntryEventImpl event = EntryEventImpl.create(br, Operation.UPDATE, "key1", "value1",
        null, false, null, true, eventId);
    UpdateOperation operation = new UpdateOperation(event, 0);
    CacheOperationMessage message = operation.createMessage();
    event.setInhibitAllNotifications(true);
    operation.initMessage(message, null);
    assertTrue(message instanceof UpdateOperation.UpdateMessage);
    assertTrue(message.inhibitAllNotifications);
    assertFalse(message.hasDelta()); // inhibitAllNotifications should not be interpreted as
                                     // hasDelta
    HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(message, hdos);
    byte[] outputArray = hdos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(outputArray);
    UpdateOperation.UpdateMessage message2 = DataSerializer.readObject(new DataInputStream(bais));
    assertTrue(message2 instanceof UpdateOperation.UpdateMessage);
    assertTrue(message2.inhibitAllNotifications);
    assertFalse(message2.hasDelta()); // inhibitAllNotifications should not be interpreted as
                                      // hasDelta
  }
}
