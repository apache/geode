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
package org.apache.geode.internal.cache.entries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.util.GatewayConflictResolver;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.TimestampedEntryEventImpl;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;
import org.apache.geode.internal.offheap.OffHeapMemoryStats;
import org.apache.geode.internal.offheap.OutOfOffHeapMemoryListener;
import org.apache.geode.internal.offheap.SlabImpl;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;

public class AbstractRegionEntryTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void whenMakeTombstoneHasSetValueThatThrowsExceptionDoesNotChangeValueToTombstone()
      throws RegionClearedException {
    LocalRegion lr = mock(LocalRegion.class);
    RegionVersionVector<?> rvv = mock(RegionVersionVector.class);
    when(lr.getVersionVector()).thenReturn(rvv);
    VersionTag<?> vt = mock(VersionTag.class);
    Object value = "value";
    AbstractRegionEntry re = new TestableRegionEntry(lr, value);
    assertEquals(value, re.getValueField());
    Assertions.assertThatThrownBy(() -> re.makeTombstone(lr, vt))
        .isInstanceOf(RuntimeException.class).hasMessage("throw exception on setValue(TOMBSTONE)");
    Assert.assertEquals(Token.REMOVED_PHASE2, re.getValueField());
  }


  @Test
  public void whenRegionEntryIsTombstoneThenIndexShouldNotBeUpdated() throws QueryException {
    InternalRegion region = mock(InternalRegion.class);
    IndexManager indexManager = mock(IndexManager.class);
    when(region.getIndexManager()).thenReturn(indexManager);
    AbstractRegionEntry abstractRegionEntry = mock(AbstractRegionEntry.class);
    when(abstractRegionEntry.isTombstone()).thenReturn(true);
    abstractRegionEntry.updateIndexOnDestroyOperation(region);
    verify(indexManager, never()).updateIndexes(any(AbstractRegionEntry.class),
        eq(IndexManager.REMOVE_ENTRY), eq(IndexProtocol.OTHER_OP));

  }


  @Test
  public void whenPrepareValueForCacheCalledWithOffHeapEntryHasNewCachedSerializedValue()
      throws RegionClearedException, IOException, ClassNotFoundException {
    LocalRegion lr = mock(LocalRegion.class);
    RegionEntryContext regionEntryContext = mock(RegionEntryContext.class);
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);
    SlabImpl slab = new SlabImpl(1024); // 1k
    MemoryAllocatorImpl ma =
        MemoryAllocatorImpl.createForUnitTest(ooohml, stats, new SlabImpl[] {slab});
    try {
      when(regionEntryContext.getOffHeap()).thenReturn(true);
      String value = "value";
      AbstractRegionEntry re = new TestableRegionEntry(lr, value);
      assertEquals(value, re.getValueField());
      EntryEventImpl entryEvent = new EntryEventImpl();
      StoredObject valueForCache =
          (StoredObject) re.prepareValueForCache(regionEntryContext, value, entryEvent, true);
      final byte[] cachedSerializedNewValue = entryEvent.getCachedSerializedNewValue();
      assertNotNull(cachedSerializedNewValue);
      valueForCache.checkDataEquals(cachedSerializedNewValue);
      DataInputStream dataInputStream =
          new DataInputStream(new ByteArrayInputStream(cachedSerializedNewValue));
      Object o = DataSerializer.readObject(dataInputStream);
      assertEquals(o, value);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void gatewayEventsFromSameDSShouldThrowCMEIfMisordered() {
    // create 2 gateway events with the same dsid, but different timestamp
    // apply them in misorder, it should throw CME before calling resolver
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    LocalRegion lr = mock(LocalRegion.class);
    String value = "value";
    AbstractRegionEntry re = new TestableRegionEntry(lr, value);
    InternalDistributedMember member1 = mock(InternalDistributedMember.class);

    EntryEventImpl entryEvent1 = new EntryEventImpl();
    entryEvent1.setRegion(lr);
    when(lr.getCache()).thenReturn(cache);
    GatewayConflictResolver resolver = mock(GatewayConflictResolver.class);
    when(cache.getGatewayConflictResolver()).thenReturn(resolver);

    VersionTag tag1 = VersionTag.create(member1);
    tag1.setVersionTimeStamp(1);
    tag1.setDistributedSystemId(3);
    tag1.setIsGatewayTag(true);

    VersionTag tag2 = VersionTag.create(member1);
    tag2.setVersionTimeStamp(2);
    tag2.setDistributedSystemId(3);
    tag2.setIsGatewayTag(true);

    ((TestableRegionEntry) re).setVersions(tag2);
    assertEquals(tag2.getVersionTimeStamp(),
        re.getVersionStamp().asVersionTag().getVersionTimeStamp());
    assertEquals(3, ((TestableRegionEntry) re).getDistributedSystemId());

    // apply tag1 with smaller timestamp should throw CME
    entryEvent1.setVersionTag(tag1);
    expectedException.expect(ConcurrentCacheModificationException.class);
    expectedException.expectMessage("conflicting WAN event detected");
    re.processVersionTag(entryEvent1);
    verify(resolver, never()).onEvent(any(), any());
  }

  @Test
  public void gatewayEventsFromSameDSInCorrectOrderOfTimestampShouldPass() {
    // create 2 gateway events with the same dsid, but different timestamp
    // apply them in correct order, it should pass
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    LocalRegion lr = mock(LocalRegion.class);
    String value = "value";
    AbstractRegionEntry re = new TestableRegionEntry(lr, value);
    InternalDistributedMember member1 = mock(InternalDistributedMember.class);

    EntryEventImpl entryEvent1 = new EntryEventImpl();
    entryEvent1.setRegion(lr);
    when(lr.getCache()).thenReturn(cache);
    when(cache.getGatewayConflictResolver()).thenReturn(null);

    VersionTag tag1 = VersionTag.create(member1);
    tag1.setVersionTimeStamp(1);
    tag1.setDistributedSystemId(3);
    tag1.setIsGatewayTag(true);

    VersionTag tag2 = VersionTag.create(member1);
    tag2.setVersionTimeStamp(2);
    tag2.setDistributedSystemId(3);
    tag2.setIsGatewayTag(true);

    ((TestableRegionEntry) re).setVersions(tag1);
    assertEquals(tag1.getVersionTimeStamp(),
        re.getVersionStamp().asVersionTag().getVersionTimeStamp());
    assertEquals(3, ((TestableRegionEntry) re).getDistributedSystemId());

    // apply tag2 should be accepted
    entryEvent1.setVersionTag(tag2);
    re.processVersionTag(entryEvent1);
  }

  @Test
  public void stampWithoutDSIDShouldAcceptAnyTag() {
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    LocalRegion lr = mock(LocalRegion.class);
    String value = "value";
    AbstractRegionEntry re = new TestableRegionEntry(lr, value);
    InternalDistributedMember member1 = mock(InternalDistributedMember.class);

    EntryEventImpl entryEvent1 = new EntryEventImpl();
    entryEvent1.setRegion(lr);
    when(lr.getCache()).thenReturn(cache);
    when(cache.getGatewayConflictResolver()).thenReturn(null);

    VersionTag tag1 = VersionTag.create(member1);
    tag1.setVersionTimeStamp(1);
    tag1.setDistributedSystemId(-1);
    tag1.setIsGatewayTag(true);

    VersionTag tag2 = VersionTag.create(member1);
    tag2.setVersionTimeStamp(2);
    tag2.setDistributedSystemId(2);
    tag2.setIsGatewayTag(true);

    ((TestableRegionEntry) re).setVersions(tag1);
    assertEquals(tag1.getVersionTimeStamp(),
        re.getVersionStamp().asVersionTag().getVersionTimeStamp());
    assertEquals(-1, ((TestableRegionEntry) re).getDistributedSystemId());

    // apply tag2 should be accepted
    entryEvent1.setVersionTag(tag2);
    re.processVersionTag(entryEvent1);
  }

  @Test
  public void applyingGatewayEventsFromDifferentDSShouldAcceptBiggerTimestamp() {
    // create 2 gateway events:
    // tag1 with smaller distributed system ids (DSIDs) and bigger timestamp
    // tag2 with bigger DSID and smaller timestamp
    // set tag2 into stamp. Apply event with tag1 should pass
    // i.e. We compare timestamp first, then DSID
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    LocalRegion lr = mock(LocalRegion.class);
    String value = "value";
    AbstractRegionEntry re = new TestableRegionEntry(lr, value);
    InternalDistributedMember member1 = mock(InternalDistributedMember.class);

    EntryEventImpl entryEvent1 = new EntryEventImpl();
    entryEvent1.setRegion(lr);
    when(lr.getCache()).thenReturn(cache);
    when(cache.getGatewayConflictResolver()).thenReturn(null);

    VersionTag tag1 = VersionTag.create(member1);
    tag1.setVersionTimeStamp(2);
    tag1.setDistributedSystemId(1);
    tag1.setIsGatewayTag(true);

    VersionTag tag2 = VersionTag.create(member1);
    tag2.setVersionTimeStamp(1);
    tag2.setDistributedSystemId(2);
    tag2.setIsGatewayTag(true);

    ((TestableRegionEntry) re).setVersions(tag2);
    assertEquals(2, ((TestableRegionEntry) re).getDistributedSystemId());

    // apply tag1 with bigger timestamp should pass
    entryEvent1.setVersionTag(tag1);
    re.processVersionTag(entryEvent1);
  }

  @Test
  public void applyingGatewayEventsFromSmallerDSWithSameTimestampShouldThrowCMEIfNoResolver() {
    // create 2 gateway events with different distributed system ids (DSIDs), with same timestamp
    // set the one with bigger DSID into stamp.
    // Apply the one with smaller DSID show throw CME
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    LocalRegion lr = mock(LocalRegion.class);
    String value = "value";
    AbstractRegionEntry re = new TestableRegionEntry(lr, value);
    InternalDistributedMember member1 = mock(InternalDistributedMember.class);

    EntryEventImpl entryEvent1 = new EntryEventImpl();
    entryEvent1.setRegion(lr);
    when(lr.getCache()).thenReturn(cache);
    when(cache.getGatewayConflictResolver()).thenReturn(null);

    VersionTag tag1 = VersionTag.create(member1);
    tag1.setVersionTimeStamp(1);
    tag1.setDistributedSystemId(1);
    tag1.setIsGatewayTag(true);

    VersionTag tag2 = VersionTag.create(member1);
    tag2.setVersionTimeStamp(1);
    tag2.setDistributedSystemId(2);
    tag2.setIsGatewayTag(true);

    ((TestableRegionEntry) re).setVersions(tag2);
    assertEquals(2, ((TestableRegionEntry) re).getDistributedSystemId());

    // apply tag1 with smaller timestamp should throw CME
    entryEvent1.setVersionTag(tag1);
    expectedException.expect(ConcurrentCacheModificationException.class);
    expectedException.expectMessage("conflicting WAN event detected");
    re.processVersionTag(entryEvent1);
  }

  @Test
  public void resolverShouldHandleConflictEventsFromDifferentDS() {
    // create 2 gateway events with different distributed system ids (DSIDs), with same timestamp
    // set the one with bigger DSID into stamp.
    // Usually, apply the one with smaller DSID should throw CME, but since there's resolver
    // resolver will accept the event
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    LocalRegion lr = mock(LocalRegion.class);
    String value = "value";
    AbstractRegionEntry re = new TestableRegionEntry(lr, value);
    InternalDistributedMember member1 = mock(InternalDistributedMember.class);

    EntryEventImpl entryEvent1 = new EntryEventImpl();
    entryEvent1 = Mockito.spy(entryEvent1);
    entryEvent1.setRegion(lr);
    when(lr.getCache()).thenReturn(cache);
    GatewayConflictResolver resolver = mock(GatewayConflictResolver.class);
    when(cache.getGatewayConflictResolver()).thenReturn(resolver);
    doNothing().when(resolver).onEvent(any(), any());
    TimestampedEntryEventImpl timestampedEvent = mock(TimestampedEntryEventImpl.class);
    // when(entryEvent1.getTimestampedEvent(anyInt(), anyInt(), anyLong(), anyLong()));
    doReturn(timestampedEvent).when(entryEvent1).getTimestampedEvent(anyInt(), anyInt(), anyLong(),
        anyLong());
    when(timestampedEvent.hasOldValue()).thenReturn(true);

    VersionTag tag1 = VersionTag.create(member1);
    tag1.setVersionTimeStamp(1);
    tag1.setDistributedSystemId(1);
    tag1.setIsGatewayTag(true);

    VersionTag tag2 = VersionTag.create(member1);
    tag2.setVersionTimeStamp(1);
    tag2.setDistributedSystemId(2);
    tag2.setIsGatewayTag(true);

    ((TestableRegionEntry) re).setVersions(tag2);
    assertEquals(2, ((TestableRegionEntry) re).getDistributedSystemId());

    entryEvent1.setVersionTag(tag1);
    re.processVersionTag(entryEvent1);
    verify(resolver, Mockito.times(1)).onEvent(any(), any());
  }

  public static class TestableRegionEntry extends AbstractRegionEntry
      implements OffHeapRegionEntry, VersionStamp {

    private Object value;
    private VersionTag tag;
    private long timeStamp = 0;
    private int dsId;

    protected TestableRegionEntry(RegionEntryContext context, Object value) {
      super(context, value);
    }

    @Override
    public void setVersionTimeStamp(long timeStamp) {
      this.timeStamp = timeStamp;
    }

    @Override
    public void setVersions(VersionTag tag) {
      this.tag = tag;
      this.timeStamp = tag.getVersionTimeStamp();
      this.dsId = tag.getDistributedSystemId();
    }

    @Override
    public void setMemberID(VersionSource memberID) {

    }

    @Override
    public VersionTag asVersionTag() {
      return tag;
    }

    @Override
    public void processVersionTag(InternalRegion region, VersionTag tag, boolean isTombstoneFromGII,
        boolean hasDelta, VersionSource versionSource,
        InternalDistributedMember sender, boolean checkConflicts) {

    }

    @Override
    public VersionStamp getVersionStamp() {
      return this;
    }

    @Override
    protected Object getValueField() {
      return this.value;
    }

    @Override
    protected void setValueField(Object v) {
      this.value = v;
    }

    @Override
    public void setValue(RegionEntryContext context, @Unretained Object value)
        throws RegionClearedException {
      super.setValue(context, value);
      if (value == Token.TOMBSTONE) {
        throw new RuntimeException("throw exception on setValue(TOMBSTONE)");
      }
    }

    @Override
    public int getEntryHash() {
      return 0;
    }

    @Override
    public HashEntry<Object, Object> getNextEntry() {
      return null;
    }

    @Override
    public void setNextEntry(HashEntry<Object, Object> n) {}

    @Override
    public Object getKey() {
      return null;
    }

    @Override
    protected long getLastModifiedField() {
      return 0;
    }

    @Override
    protected boolean compareAndSetLastModifiedField(long expectedValue, long newValue) {
      return false;
    }

    @Override
    protected void setEntryHash(int v) {}

    @Override
    public void release() {

    }

    @Override
    public long getAddress() {
      return 0;
    }

    @Override
    public boolean setAddress(long expectedAddr, long newAddr) {
      return false;
    }

    @Override
    public int getEntryVersion() {
      return 0;
    }

    @Override
    public long getRegionVersion() {
      return 0;
    }

    @Override
    public long getVersionTimeStamp() {
      return this.timeStamp;
    }

    @Override
    public VersionSource getMemberID() {
      return null;
    }

    @Override
    public int getDistributedSystemId() {
      return this.dsId;
    }

    @Override
    public short getRegionVersionHighBytes() {
      return 0;
    }

    @Override
    public int getRegionVersionLowBytes() {
      return 0;
    }
  }
}
