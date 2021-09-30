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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.internal.cache.wan.GatewaySenderEventImpl.TransactionMetadataDisposition.EXCLUDE;
import static org.apache.geode.internal.cache.wan.GatewaySenderEventImpl.TransactionMetadataDisposition.INCLUDE;
import static org.apache.geode.internal.cache.wan.GatewaySenderEventImpl.TransactionMetadataDisposition.INCLUDE_LAST_EVENT;
import static org.apache.geode.internal.serialization.KnownVersion.GEODE_1_13_0;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderHelper;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.test.fake.Fakes;

public class GatewaySenderEventImplTest {

  private GemFireCacheImpl cache;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUpGemFire() {
    createCache();
  }

  private void createCache() {
    // Mock cache
    cache = Fakes.cache();
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(cache.getDistributedSystem()).thenReturn(ids);
  }

  @Test
  public void versionedFromData() throws IOException, ClassNotFoundException {
    GatewaySenderEventImpl gatewaySenderEvent = spy(GatewaySenderEventImpl.class);
    DataInput dataInput = mock(DataInput.class);
    DeserializationContext deserializationContext = mock(DeserializationContext.class);
    ObjectDeserializer objectDeserializer = mock(ObjectDeserializer.class);
    EventID eventID = mock(EventID.class);
    GatewaySenderEventCallbackArgument gatewaySenderEventCallbackArgument =
        mock(GatewaySenderEventCallbackArgument.class);
    TransactionId transactionId = mock(TransactionId.class);
    when(deserializationContext.getDeserializer()).thenReturn(objectDeserializer);
    when(objectDeserializer.readObject(dataInput)).thenReturn(eventID,
        gatewaySenderEventCallbackArgument);
    when(dataInput.readByte()).thenReturn(DSCODE.STRING.toByte());
    when(dataInput.readBoolean()).thenReturn(true);
    when(dataInput.readShort()).thenReturn(KnownVersion.GEODE_1_13_0.ordinal());

    gatewaySenderEvent.fromData(dataInput, deserializationContext);
    assertThat(gatewaySenderEvent.getTransactionId()).isNull();

    when(dataInput.readShort()).thenReturn(KnownVersion.GEODE_1_14_0.ordinal());
    when(objectDeserializer.readObject(dataInput)).thenReturn(eventID, new Object(),
        gatewaySenderEventCallbackArgument, transactionId);
    gatewaySenderEvent.fromData(dataInput, deserializationContext);
    assertThat(gatewaySenderEvent.getTransactionId()).isNotNull();
  }

  @Test
  public void testSerializingDataFromVersion_1_14_0_OrNewerToVersion_1_13_0() throws IOException {
    InternalDataSerializer internalDataSerializer = spy(InternalDataSerializer.class);
    GatewaySenderEventImpl gatewaySenderEvent = spy(GatewaySenderEventImpl.class);
    OutputStream outputStream = mock(OutputStream.class);
    VersionedDataOutputStream versionedDataOutputStream =
        new VersionedDataOutputStream(outputStream, GEODE_1_13_0);

    internalDataSerializer.invokeToData(gatewaySenderEvent, versionedDataOutputStream);
    verify(gatewaySenderEvent, times(0)).toData(any(), any());
    verify(gatewaySenderEvent, times(1)).toDataPre_GEODE_1_14_0_0(any(), any());
    verify(gatewaySenderEvent, times(1)).toDataPre_GEODE_1_9_0_0(any(), any());
  }

  @Test
  public void testDeserializingDataFromVersion_1_13_0_ToVersion_1_14_0_OrNewer()
      throws IOException, ClassNotFoundException {
    InternalDataSerializer internalDataSerializer = spy(InternalDataSerializer.class);
    GatewaySenderEventImpl gatewaySenderEvent = spy(GatewaySenderEventImpl.class);
    InputStream inputStream = mock(InputStream.class);
    when(inputStream.read()).thenReturn(69); // NULL_STRING
    when(inputStream.read(isA(byte[].class), isA(int.class), isA(int.class))).thenReturn(1);
    VersionedDataInputStream versionedDataInputStream =
        new VersionedDataInputStream(inputStream, GEODE_1_13_0);

    internalDataSerializer.invokeFromData(gatewaySenderEvent, versionedDataInputStream);
    verify(gatewaySenderEvent, times(0)).fromData(any(), any());
    verify(gatewaySenderEvent, times(1)).fromDataPre_GEODE_1_14_0_0(any(), any());
    verify(gatewaySenderEvent, times(1)).fromDataPre_GEODE_1_9_0_0(any(), any());
  }

  @Test
  public void testEquality() throws Exception {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getFullPath()).thenReturn(testName.getMethodName() + "_region");
    when(region.getCache()).thenReturn(cache);
    Object event = ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
        "key1", "value1", 0, 0, 0, 0);

    // Basic equality tests
    assertThat(event).isNotEqualTo(null);
    assertThat(event).isEqualTo(event);

    // Verify an event is equal to a duplicate
    Object eventDuplicate =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value1", 0, 0, 0, 0);
    assertThat(event).isEqualTo(eventDuplicate);

    // Verify an event is not equal if any of its fields are different
    Object eventDifferentShadowKey =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value1", 0, 0, 0, 1);
    assertThat(event).isNotEqualTo(eventDifferentShadowKey);

    Object eventDifferentEventId =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value1", 0, 1, 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentEventId);

    Object eventDifferentBucketId =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value1", 0, 0, 1, 0);
    assertThat(event).isNotEqualTo(eventDifferentBucketId);

    Object eventDifferentOperation =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.UPDATE,
            "key1", "value1", 0, 0, 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentOperation);

    Object eventDifferentKey =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key2", "value1", 0, 0, 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentKey);

    Object eventDifferentValue =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value2", 0, 0, 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentValue);

    LocalRegion region2 = mock(LocalRegion.class);
    when(region2.getFullPath()).thenReturn(testName.getMethodName() + "_region2");
    when(region2.getCache()).thenReturn(cache);
    Object eventDifferentRegion =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region2, Operation.CREATE,
            "key1", "value1", 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentRegion);
  }

  @Test
  public void constructsWithTransactionMetadataWhenInclude() throws IOException {
    final EntryEventImpl cacheEvent = mockEntryEventImpl(mock(TransactionId.class));

    final GatewaySenderEventImpl gatewaySenderEvent =
        new GatewaySenderEventImpl(EnumListenerEvent.AFTER_CREATE, cacheEvent, null, INCLUDE);

    assertThat(gatewaySenderEvent.getTransactionId()).isNotNull();
    assertThat(gatewaySenderEvent.isLastEventInTransaction()).isFalse();
  }

  @Test
  public void constructsWithTransactionMetadataWhenIncludedLastEvent() throws IOException {
    final EntryEventImpl cacheEvent = mockEntryEventImpl(mock(TransactionId.class));

    final GatewaySenderEventImpl gatewaySenderEvent =
        new GatewaySenderEventImpl(EnumListenerEvent.AFTER_CREATE, cacheEvent, null,
            INCLUDE_LAST_EVENT);

    assertThat(gatewaySenderEvent.getTransactionId()).isNotNull();
    assertThat(gatewaySenderEvent.isLastEventInTransaction()).isTrue();
  }

  @Test
  public void constructsWithoutTransactionMetadataWhenExcluded() throws IOException {
    final EntryEventImpl cacheEvent = mockEntryEventImpl(mock(TransactionId.class));

    final GatewaySenderEventImpl gatewaySenderEvent =
        new GatewaySenderEventImpl(EnumListenerEvent.AFTER_CREATE, cacheEvent, null, EXCLUDE);

    assertThat(gatewaySenderEvent.getTransactionId()).isNull();
    assertThat(gatewaySenderEvent.isLastEventInTransaction()).isFalse();
  }

  @Test
  public void constructsWithoutTransactionMetadataWhenIncludedButNotTransactionEvent()
      throws IOException {
    final EntryEventImpl cacheEvent = mockEntryEventImpl(null);

    final GatewaySenderEventImpl gatewaySenderEvent =
        new GatewaySenderEventImpl(EnumListenerEvent.AFTER_CREATE, cacheEvent, null, INCLUDE);

    assertThat(gatewaySenderEvent.getTransactionId()).isNull();
    assertThat(gatewaySenderEvent.isLastEventInTransaction()).isFalse();
  }

  @Test
  public void constructsWithoutTransactionMetadataWhenIncludedLastEventButNotTransactionEvent()
      throws IOException {
    final EntryEventImpl cacheEvent = mockEntryEventImpl(null);

    final GatewaySenderEventImpl gatewaySenderEvent =
        new GatewaySenderEventImpl(EnumListenerEvent.AFTER_CREATE, cacheEvent, null,
            INCLUDE_LAST_EVENT);

    assertThat(gatewaySenderEvent.getTransactionId()).isNull();
    assertThat(gatewaySenderEvent.isLastEventInTransaction()).isFalse();
  }

  @Test
  public void constructsWithoutTransactionMetadataWhenExcludedButNotTransactionEvent()
      throws IOException {
    final EntryEventImpl cacheEvent = mockEntryEventImpl(null);

    final GatewaySenderEventImpl gatewaySenderEvent =
        new GatewaySenderEventImpl(EnumListenerEvent.AFTER_CREATE, cacheEvent, null, EXCLUDE);

    assertThat(gatewaySenderEvent.getTransactionId()).isNull();
    assertThat(gatewaySenderEvent.isLastEventInTransaction()).isFalse();
  }

  private EntryEventImpl mockEntryEventImpl(final TransactionId transactionId) {
    final EntryEventImpl cacheEvent = mock(EntryEventImpl.class);
    when(cacheEvent.getEventId()).thenReturn(mock(EventID.class));
    when(cacheEvent.getOperation()).thenReturn(Operation.CREATE);
    when(cacheEvent.getTransactionId()).thenReturn(transactionId);
    final LocalRegion region = mock(LocalRegion.class);
    when(cacheEvent.getRegion()).thenReturn(region);
    return cacheEvent;
  }

}
