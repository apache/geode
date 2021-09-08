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
import static org.apache.geode.internal.serialization.KnownVersion.GEODE_1_14_0;
import static org.apache.geode.internal.serialization.KnownVersion.GEODE_1_8_0;
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

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderHelper;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
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

    when(dataInput.readShort()).thenReturn(GEODE_1_14_0.ordinal());
    when(objectDeserializer.readObject(dataInput)).thenReturn(eventID, new Object(),
        gatewaySenderEventCallbackArgument, transactionId);
    gatewaySenderEvent.fromData(dataInput, deserializationContext);
    assertThat(gatewaySenderEvent.getTransactionId()).isNotNull();
  }

  @Test
  @Parameters(method = "getVersionsAndExpectedInvocations")
  public void testSerializingDataFromCurrentVersionToOldVersion(VersionAndExpectedInvocations vaei)
      throws IOException {
    InternalDataSerializer internalDataSerializer = spy(InternalDataSerializer.class);
    GatewaySenderEventImpl gatewaySenderEvent = spy(GatewaySenderEventImpl.class);
    OutputStream outputStream = mock(OutputStream.class);
    VersionedDataOutputStream versionedDataOutputStream =
        new VersionedDataOutputStream(outputStream, vaei.getVersion());

    internalDataSerializer.invokeToData(gatewaySenderEvent, versionedDataOutputStream);
    verify(gatewaySenderEvent, times(0)).toData(any(), any());
    verify(gatewaySenderEvent, times(vaei.getPre115Invocations())).toDataPre_GEODE_1_15_0_0(any(),
        any());
    verify(gatewaySenderEvent, times(vaei.getPre114Invocations())).toDataPre_GEODE_1_14_0_0(any(),
        any());
    verify(gatewaySenderEvent, times(vaei.getPre19Invocations())).toDataPre_GEODE_1_9_0_0(any(),
        any());
  }

  @Test
  @Parameters(method = "getVersionsAndExpectedInvocations")
  public void testDeserializingDataFromOldVersionToCurrentVersion(
      VersionAndExpectedInvocations vaei)
      throws IOException, ClassNotFoundException {
    InternalDataSerializer internalDataSerializer = spy(InternalDataSerializer.class);
    GatewaySenderEventImpl gatewaySenderEvent = spy(GatewaySenderEventImpl.class);
    InputStream inputStream = mock(InputStream.class);
    when(inputStream.read()).thenReturn(69); // NULL_STRING
    when(inputStream.read(isA(byte[].class), isA(int.class), isA(int.class))).thenReturn(1);
    VersionedDataInputStream versionedDataInputStream =
        new VersionedDataInputStream(inputStream, vaei.getVersion());

    internalDataSerializer.invokeFromData(gatewaySenderEvent, versionedDataInputStream);
    verify(gatewaySenderEvent, times(0)).fromData(any(), any());
    verify(gatewaySenderEvent, times(vaei.getPre115Invocations())).fromDataPre_GEODE_1_15_0_0(any(),
        any());
    verify(gatewaySenderEvent, times(vaei.getPre114Invocations())).fromDataPre_GEODE_1_14_0_0(any(),
        any());
    verify(gatewaySenderEvent, times(vaei.getPre19Invocations())).fromDataPre_GEODE_1_9_0_0(any(),
        any());
  }

  private VersionAndExpectedInvocations[] getVersionsAndExpectedInvocations() {
    return new VersionAndExpectedInvocations[] {
        new VersionAndExpectedInvocations(GEODE_1_8_0, 1, 0, 0),
        new VersionAndExpectedInvocations(GEODE_1_13_0, 1, 1, 0),
        new VersionAndExpectedInvocations(GEODE_1_14_0, 1, 1, 1)
    };
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
  public void testSerialization() throws Exception {
    // Set up test
    LocalRegion region = mock(LocalRegion.class);
    when(region.getFullPath()).thenReturn(testName.getMethodName() + "_region");
    when(region.getCache()).thenReturn(cache);
    TXId txId = new TXId(cache.getMyId(), 0);
    when(region.getTXId()).thenReturn(txId);

    // Create GatewaySenderEventImpl
    GatewaySenderEventImpl originalEvent =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.PUTALL_CREATE,
            "key1", "value1", 1, 3, 3, 113);

    // Serialize GatewaySenderEventImpl
    byte[] eventBytes = BlobHelper.serializeToBlob(originalEvent);

    // Deserialize GatewaySenderEventImpl
    GatewaySenderEventImpl deserializedEvent =
        (GatewaySenderEventImpl) BlobHelper.deserializeBlob(eventBytes);

    // Verify fields are equal
    assertThat(originalEvent.getEventId()).isEqualTo(deserializedEvent.getEventId());
    assertThat(originalEvent.getAction()).isEqualTo(deserializedEvent.getAction());
    assertThat(originalEvent.getOperation()).isEqualTo(deserializedEvent.getOperation());
    assertThat(originalEvent.getRegionPath()).isEqualTo(deserializedEvent.getRegionPath());
    assertThat(originalEvent.getKey()).isEqualTo(deserializedEvent.getKey());
    assertThat(originalEvent.getDeserializedValue())
        .isEqualTo(deserializedEvent.getDeserializedValue());
    assertThat(originalEvent.getValueIsObject()).isEqualTo(deserializedEvent.getValueIsObject());
    assertThat(originalEvent.getNumberOfParts()).isEqualTo(deserializedEvent.getNumberOfParts());
    assertThat(originalEvent.getCallbackArgument())
        .isEqualTo(deserializedEvent.getCallbackArgument());
    assertThat(originalEvent.getPossibleDuplicate())
        .isEqualTo(deserializedEvent.getPossibleDuplicate());
    assertThat(originalEvent.getCreationTime()).isEqualTo(deserializedEvent.getCreationTime());
    assertThat(originalEvent.getShadowKey()).isEqualTo(deserializedEvent.getShadowKey());
    assertThat(originalEvent.getVersionTimeStamp())
        .isEqualTo(deserializedEvent.getVersionTimeStamp());
    assertThat(originalEvent.isAcked).isEqualTo(deserializedEvent.isAcked);
    assertThat(originalEvent.isDispatched).isEqualTo(deserializedEvent.isDispatched);
    assertThat(originalEvent.getBucketId()).isEqualTo(deserializedEvent.getBucketId());
    assertThat(originalEvent.isConcurrencyConflict())
        .isEqualTo(deserializedEvent.isConcurrencyConflict());
    assertThat(originalEvent.getTransactionId())
        .isEqualTo(deserializedEvent.getTransactionId());
    assertThat(originalEvent.isLastEventInTransaction())
        .isEqualTo(deserializedEvent.isLastEventInTransaction());
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

  @Parameters({"true, true", "true, false", "false, false"})
  public void testCreation_WithAfterUpdateWithGenerateCallbacks(boolean isGenerateCallbacks,
      boolean isCallbackArgumentNull)
      throws IOException {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getFullPath()).thenReturn(testName.getMethodName() + "_region");

    Operation operation = mock(Operation.class);
    when(operation.isLocalLoad()).thenReturn(true);

    EntryEventImpl cacheEvent = mock(EntryEventImpl.class);
    when(cacheEvent.getRegion()).thenReturn(region);
    when(cacheEvent.getEventId()).thenReturn(mock(EventID.class));
    when(cacheEvent.getOperation()).thenReturn(operation);
    when(cacheEvent.isGenerateCallbacks()).thenReturn(isGenerateCallbacks);
    when(cacheEvent.getRawCallbackArgument())
        .thenReturn(isCallbackArgumentNull ? null : mock(GatewaySenderEventCallbackArgument.class));

    GatewaySenderEventImpl event = new GatewaySenderEventImpl(
        EnumListenerEvent.AFTER_UPDATE_WITH_GENERATE_CALLBACKS, cacheEvent,
        null, false, INCLUDE_LAST_EVENT);

    final int numberOfParts = isCallbackArgumentNull ? 8 : 9;
    assertThat(event.getNumberOfParts()).isEqualTo(numberOfParts);

    final int action = isGenerateCallbacks ? 1 : 4;
    assertThat(event.getAction()).isEqualTo(action);
  }

  public static class VersionAndExpectedInvocations {

    private final KnownVersion version;

    private final int pre19Invocations;

    private final int pre114Invocations;

    private final int pre115Invocations;

    public VersionAndExpectedInvocations(KnownVersion version, int pre19Invocations,
        int pre114Invocations, int pre115Invocations) {
      this.version = version;
      this.pre19Invocations = pre19Invocations;
      this.pre114Invocations = pre114Invocations;
      this.pre115Invocations = pre115Invocations;
    }

    public KnownVersion getVersion() {
      return this.version;
    }

    public int getPre19Invocations() {
      return this.pre19Invocations;
    }

    public int getPre114Invocations() {
      return this.pre114Invocations;
    }

    public int getPre115Invocations() {
      return this.pre115Invocations;
    }
  }
}
