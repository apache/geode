/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.SerializedCacheValue;
import org.apache.geode.internal.cache.EntryEventImpl.NewValueImporter;
import org.apache.geode.internal.cache.EntryEventImpl.OldValueImporter;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class EntryEventImplTest {

  private String key;

  @Before
  public void setUp() throws Exception {
    key = "key1";
  }

  @Test
  public void verifyToStringOutputHasRegionName() {
    // mock a region object
    LocalRegion region = mock(LocalRegion.class);
    String expectedRegionName = "ExpectedFullRegionPathName";
    String value = "value1";
    KeyInfo keyInfo = new KeyInfo(key, value, null);
    doReturn(expectedRegionName).when(region).getFullPath();
    doReturn(keyInfo).when(region).getKeyInfo(any(), any(), any());

    // create entry event for the region
    EntryEventImpl e = createEntryEvent(region, value);
    
    // The name of the region should be in the toString text
    String toStringValue = e.toString();
    assertTrue("String " + expectedRegionName + " was not in toString text: " + toStringValue, toStringValue.indexOf(expectedRegionName) > 0);

    // verify that toString called getFullPath method of region object
    verify(region, times(1)).getFullPath();
  }
  
  @Test
  public void verifyExportNewValueWithUnserializedStoredObject() {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);
    StoredObject newValue = mock(StoredObject.class);
    byte[] newValueBytes = new byte[]{1,2,3};
    when(newValue.getValueAsHeapByteArray()).thenReturn(newValueBytes);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, newValue);
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewBytes(newValueBytes, false);
  }

  @Test
  public void verifyExportNewValueWithByteArray() {
    LocalRegion region = mock(LocalRegion.class);
    byte[] newValue = new byte[]{1,2,3};
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, newValue);
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewBytes(newValue, false);
  }

  @Test
  public void verifyExportNewValueWithStringIgnoresNewValueBytes() {
    LocalRegion region = mock(LocalRegion.class);
    String newValue = "newValue";
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, newValue);
    byte[] newValueBytes = new byte[] {1,2};
    e.newValueBytes = newValueBytes;
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewObject(newValue, true);
  }
  
  @Test
  public void verifyExportNewValueWithByteArrayCachedDeserializable() {
    LocalRegion region = mock(LocalRegion.class);
    CachedDeserializable newValue = mock(CachedDeserializable.class);
    byte[] newValueBytes = new byte[] {1,2,3};
    when(newValue.getValue()).thenReturn(newValueBytes);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, newValue);
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewBytes(newValueBytes, true);
  }
  
  @Test
  public void verifyExportNewValueWithStringCachedDeserializable() {
    LocalRegion region = mock(LocalRegion.class);
    CachedDeserializable newValue = mock(CachedDeserializable.class);
    Object newValueObj = "newValueObj";
    when(newValue.getValue()).thenReturn(newValueObj);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, newValue);
    byte[] newValueBytes = new byte[] {1,2};
    e.newValueBytes = newValueBytes;
    e.setCachedSerializedNewValue(newValueBytes);
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewObject(newValueObj, true);
  }

  @Test
  public void verifyExportNewValueWithStringCachedDeserializablePrefersNewValueBytes() {
    LocalRegion region = mock(LocalRegion.class);
    CachedDeserializable newValue = mock(CachedDeserializable.class);
    Object newValueObj = "newValueObj";
    when(newValue.getValue()).thenReturn(newValueObj);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, newValue);
    byte[] newValueBytes = new byte[] {1,2};
    e.newValueBytes = newValueBytes;
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithStringCachedDeserializablePrefersCachedSerializedNewValue() {
    LocalRegion region = mock(LocalRegion.class);
    CachedDeserializable newValue = mock(CachedDeserializable.class);
    Object newValueObj = "newValueObj";
    when(newValue.getValue()).thenReturn(newValueObj);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, newValue);
    byte[] newValueBytes = new byte[] {1,2};
    e.setCachedSerializedNewValue(newValueBytes);
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithSerializedStoredObjectAndImporterPrefersSerialized() {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);
    StoredObject newValue = mock(StoredObject.class);
    when(newValue.isSerialized()).thenReturn(true);
    byte[] newValueBytes = new byte[]{1,2,3};
    when(newValue.getValueAsHeapByteArray()).thenReturn(newValueBytes);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    when(nvImporter.prefersNewSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, newValue);
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewBytes(newValueBytes, true);
  }

  @Test
  public void verifyExportNewValueWithSerializedStoredObject() {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);
    StoredObject newValue = mock(StoredObject.class);
    when(newValue.isSerialized()).thenReturn(true);
    Object newValueDeserialized = "newValueDeserialized";
    when(newValue.getValueAsDeserializedHeapObject()).thenReturn(newValueDeserialized);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, newValue);
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewObject(newValueDeserialized, true);
  }

  @Test
  public void verifyExportNewValueWithSerializedStoredObjectAndUnretainedNewReferenceOk() {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);
    StoredObject newValue = mock(StoredObject.class);
    when(newValue.isSerialized()).thenReturn(true);
    Object newValueDeserialized = "newValueDeserialized";
    when(newValue.getValueAsDeserializedHeapObject()).thenReturn(newValueDeserialized);
    NewValueImporter nvImporter = mock(NewValueImporter.class);
    when(nvImporter.isUnretainedNewReferenceOk()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, newValue);
    
    e.exportNewValue(nvImporter);
    
    verify(nvImporter).importNewObject(newValue, true);
  }

  @Test
  public void verifyExportOldValueWithUnserializedStoredObject() {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);
    StoredObject oldValue = mock(StoredObject.class);
    byte[] oldValueBytes = new byte[]{1,2,3};
    when(oldValue.getValueAsHeapByteArray()).thenReturn(oldValueBytes);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldBytes(oldValueBytes, false);
  }

  @Test
  public void verifyExportOldValueWithByteArray() {
    LocalRegion region = mock(LocalRegion.class);
    byte[] oldValue = new byte[]{1,2,3};
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldBytes(oldValue, false);
  }

  @Test
  public void verifyExportOldValueWithStringIgnoresOldValueBytes() {
    LocalRegion region = mock(LocalRegion.class);
    String oldValue = "oldValue";
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    when(ovImporter.prefersOldSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, null);
    byte[] oldValueBytes = new byte[]{1,2,3};
    e.setSerializedOldValue(oldValueBytes);
    e.setOldValue(oldValue);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldObject(oldValue, true);
  }

  @Test
  public void verifyExportOldValuePrefersOldValueBytes() {
    LocalRegion region = mock(LocalRegion.class);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    when(ovImporter.prefersOldSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, null);
    byte[] oldValueBytes = new byte[]{1,2,3};
    e.setSerializedOldValue(oldValueBytes);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldBytes(oldValueBytes, true);
  }

  @Test
  public void verifyExportOldValueWithCacheDeserializableByteArray() {
    LocalRegion region = mock(LocalRegion.class);
    CachedDeserializable oldValue = mock(CachedDeserializable.class);
    byte[] oldValueBytes = new byte[]{1,2,3};
    when(oldValue.getValue()).thenReturn(oldValueBytes);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldBytes(oldValueBytes, true);
  }

  @Test
  public void verifyExportOldValueWithCacheDeserializableString() {
    LocalRegion region = mock(LocalRegion.class);
    CachedDeserializable oldValue = mock(CachedDeserializable.class);
    Object oldValueObj = "oldValueObj";
    when(oldValue.getValue()).thenReturn(oldValueObj);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldObject(oldValueObj, true);
  }

  @Test
  public void verifyExportOldValueWithCacheDeserializableOk() {
    LocalRegion region = mock(LocalRegion.class);
    CachedDeserializable oldValue = mock(CachedDeserializable.class);
    Object oldValueObj = "oldValueObj";
    when(oldValue.getValue()).thenReturn(oldValueObj);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    when(ovImporter.isCachedDeserializableValueOk()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldObject(oldValue, true);
  }

  @Test
  public void verifyExportOldValueWithSerializedStoredObjectAndImporterPrefersSerialized() {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);
    StoredObject oldValue = mock(StoredObject.class);
    when(oldValue.isSerialized()).thenReturn(true);
    byte[] oldValueBytes = new byte[]{1,2,3};
    when(oldValue.getValueAsHeapByteArray()).thenReturn(oldValueBytes);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    when(ovImporter.prefersOldSerialized()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldBytes(oldValueBytes, true);
  }

  @Test
  public void verifyExportOldValueWithSerializedStoredObject() {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);
    StoredObject oldValue = mock(StoredObject.class);
    when(oldValue.isSerialized()).thenReturn(true);
    Object oldValueDeserialized = "newValueDeserialized";
    when(oldValue.getValueAsDeserializedHeapObject()).thenReturn(oldValueDeserialized);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldObject(oldValueDeserialized, true);
  }

  @Test
  public void verifyExportOldValueWithSerializedStoredObjectAndUnretainedOldReferenceOk() {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);
    StoredObject oldValue = mock(StoredObject.class);
    when(oldValue.isSerialized()).thenReturn(true);
    Object oldValueDeserialized = "oldValueDeserialized";
    when(oldValue.getValueAsDeserializedHeapObject()).thenReturn(oldValueDeserialized);
    OldValueImporter ovImporter = mock(OldValueImporter.class);
    when(ovImporter.isUnretainedOldReferenceOk()).thenReturn(true);
    EntryEventImpl e = createEntryEvent(region, null);
    e.setOldValue(oldValue);
    
    e.exportOldValue(ovImporter);
    
    verify(ovImporter).importOldObject(oldValue, true);
  }

  @Test
  public void verifyExternalReadMethodsBlockedByRelease() throws InterruptedException {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getOffHeap()).thenReturn(true);
    StoredObject newValue = mock(StoredObject.class);
    when(newValue.hasRefCount()).thenReturn(true);
    when(newValue.isSerialized()).thenReturn(true);
    when(newValue.retain()).thenReturn(true);
    when(newValue.getDeserializedValue(any(), any())).thenReturn("newValue");
    final byte[] serializedNewValue = new byte[]{(byte)'n', (byte)'e', (byte)'w'};
    when(newValue.getSerializedValue()).thenReturn(serializedNewValue);
    StoredObject oldValue = mock(StoredObject.class);
    when(oldValue.hasRefCount()).thenReturn(true);
    when(oldValue.isSerialized()).thenReturn(true);
    when(oldValue.retain()).thenReturn(true);
    when(oldValue.getDeserializedValue(any(), any())).thenReturn("oldValue");
    final byte[] serializedOldValue = new byte[]{(byte)'o', (byte)'l', (byte)'d'};
    when(oldValue.getSerializedValue()).thenReturn(serializedOldValue);
    final CountDownLatch releaseCountDown = new CountDownLatch(1);
    final TestableEntryEventImpl e = new TestableEntryEventImpl(region, key, newValue, releaseCountDown);
    e.setOldValue(oldValue);
    assertEquals("newValue", e.getNewValue());
    assertEquals("oldValue", e.getOldValue());
    final SerializedCacheValue<?> serializableNewValue = e.getSerializedNewValue();
    assertEquals(serializedNewValue, serializableNewValue.getSerializedValue());
    assertEquals("newValue", serializableNewValue.getDeserializedValue());
    final SerializedCacheValue<?> serializableOldValue = e.getSerializedOldValue();
    assertEquals(serializedOldValue, serializableOldValue.getSerializedValue());
    assertEquals("oldValue", serializableOldValue.getDeserializedValue());
    Thread doRelease = new Thread(() -> {e.release();});
    doRelease.start(); // release thread will be stuck until releaseCountDown changes
    Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(1, TimeUnit.MILLISECONDS).timeout(15, TimeUnit.SECONDS)
    .until(() -> assertEquals(true, e.isWaitingOnRelease()));
    assertEquals(true, e.offHeapOk);
    assertEquals(true, doRelease.isAlive());
    
    // Now start a getNewValue. It should block on the release.
    Thread doGetNewValue = new Thread(() -> {e.getAndCacheNewValue();});
    doGetNewValue.start();
    // Now start a getOldValue. It should block on the release.
    Thread doGetOldValue = new Thread(() -> {e.getAndCacheOldValue();});
    doGetOldValue.start();
    // Now start a getSerializedValue on serializableNewValue. It should block on the release.
    Thread doSNVgetSerializedValue = new Thread(() -> {e.getAndCacheSerializedNew(serializableNewValue);});
    doSNVgetSerializedValue.start();
    // Now start a getDeserializedValue on serializableNewValue. It should block on the release.
    Thread doSNVgetDeserializedValue = new Thread(() -> {e.getAndCachDeserializedNew(serializableNewValue);});
    doSNVgetDeserializedValue.start();
    // Now start a getSerializedValue on serializableOldValue. It should block on the release.
    Thread doSOVgetSerializedValue = new Thread(() -> {e.getAndCacheSerializedOld(serializableOldValue);});
    doSOVgetSerializedValue.start();
    // Now start a getDeserializedValue on serializableOldValue. It should block on the release.
    Thread doSOVgetDeserializedValue = new Thread(() -> {e.getAndCachDeserializedOld(serializableOldValue);});
    doSOVgetDeserializedValue.start();
    
    Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(1, TimeUnit.MILLISECONDS).timeout(15, TimeUnit.SECONDS)
    .until(() -> assertEquals(true, e.isAboutToCallGetNewValue() && e.isAboutToCallGetOldValue()
        && e.isAboutToCallSerializedNew() && e.isAboutToCallDeserializedNew()
        && e.isAboutToCallSerializedOld() && e.isAboutToCallDeserializedOld()
        ));
    // all the threads should now be hung waiting on release; so just wait for a little bit for it to improperly finish
    doGetNewValue.join(50);
    if (e.hasFinishedCallOfGetNewValue()) {
      fail("expected doGetNewValue thread to be hung. It completed with " + e.getCachedNewValue());
    }
    if (e.hasFinishedCallOfGetOldValue()) {
      fail("expected doGetOldValue thread to be hung. It completed with " + e.getCachedOldValue());
    }
    if (e.hasFinishedCallOfSerializedNew()) {
      fail("expected doSNVgetSerializedValue thread to be hung. It completed with " + e.getTestCachedSerializedNew());
    }
    if (e.hasFinishedCallOfDeserializedNew()) {
      fail("expected doSNVgetDeserializedValue thread to be hung. It completed with " + e.getCachedDeserializedNew());
    }
    if (e.hasFinishedCallOfSerializedOld()) {
      fail("expected doSOVgetSerializedValue thread to be hung. It completed with " + e.getCachedSerializedOld());
    }
    if (e.hasFinishedCallOfDeserializedOld()) {
      fail("expected doSOVgetDeserializedValue thread to be hung. It completed with " + e.getCachedDeserializedOld());
    }
   // now signal the release to go ahead
    releaseCountDown.countDown();
    doRelease.join();
    assertEquals(false, e.offHeapOk);
    // which should allow getNewValue to complete
    Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(1, TimeUnit.MILLISECONDS).timeout(15, TimeUnit.SECONDS)
    .until(() -> assertEquals(true, e.hasFinishedCallOfGetNewValue()));
    doGetNewValue.join();
    if (!(e.getCachedNewValue() instanceof IllegalStateException)) {
      // since the release happened before getNewValue we expect it to get an exception
      fail("unexpected success of getNewValue. It returned " + e.getCachedNewValue());
    }
    // which should allow getOldValue to complete
    Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(1, TimeUnit.MILLISECONDS).timeout(15, TimeUnit.SECONDS)
    .until(() -> assertEquals(true, e.hasFinishedCallOfGetOldValue()));
    doGetOldValue.join();
    if (!(e.getCachedOldValue() instanceof IllegalStateException)) {
      fail("unexpected success of getOldValue. It returned " + e.getCachedOldValue());
    }
    // which should allow doSNVgetSerializedValue to complete
    Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(1, TimeUnit.MILLISECONDS).timeout(15, TimeUnit.SECONDS)
    .until(() -> assertEquals(true, e.hasFinishedCallOfSerializedNew()));
    doSNVgetSerializedValue.join();
    if (!(e.getTestCachedSerializedNew() instanceof IllegalStateException)) {
      fail("unexpected success of new getSerializedValue. It returned " + e.getTestCachedSerializedNew());
    }
    // which should allow doSNVgetDeserializedValue to complete
    Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(1, TimeUnit.MILLISECONDS).timeout(15, TimeUnit.SECONDS)
    .until(() -> assertEquals(true, e.hasFinishedCallOfDeserializedNew()));
    doSNVgetDeserializedValue.join();
    if (!(e.getCachedDeserializedNew() instanceof IllegalStateException)) {
      fail("unexpected success of new getDeserializedValue. It returned " + e.getCachedDeserializedNew());
    }
    // which should allow doSOVgetSerializedValue to complete
    Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(1, TimeUnit.MILLISECONDS).timeout(15, TimeUnit.SECONDS)
    .until(() -> assertEquals(true, e.hasFinishedCallOfSerializedOld()));
    doSOVgetSerializedValue.join();
    if (!(e.getCachedSerializedOld() instanceof IllegalStateException)) {
      fail("unexpected success of old getSerializedValue. It returned " + e.getCachedSerializedOld());
    }
    // which should allow doSOVgetDeserializedValue to complete
    Awaitility.await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(1, TimeUnit.MILLISECONDS).timeout(15, TimeUnit.SECONDS)
    .until(() -> assertEquals(true, e.hasFinishedCallOfDeserializedOld()));
    doSOVgetDeserializedValue.join();
    if (!(e.getCachedDeserializedOld() instanceof IllegalStateException)) {
      fail("unexpected success of old getDeserializedValue. It returned " + e.getCachedDeserializedOld());
    }
  }

  private static class TestableEntryEventImpl extends EntryEventImpl {
    private final CountDownLatch releaseCountDown;
    private volatile boolean waitingOnRelease = false;
    private volatile boolean aboutToCallGetNewValue = false;
    private volatile boolean finishedCallOfGetNewValue = false;
    private volatile Object cachedNewValue = null;
    private volatile boolean aboutToCallGetOldValue = false;
    private volatile boolean finishedCallOfGetOldValue = false;
    private volatile Object cachedOldValue = null;
    private volatile boolean aboutToCallSerializedNew = false;
    private volatile Object testCachedSerializedNew = null;
    private volatile boolean finishedCallOfSerializedNew = false;
    private volatile boolean aboutToCallDeserializedNew = false;
    private volatile Object cachedDeserializedNew = null;
    private volatile boolean finishedCallOfDeserializedNew = false;
    private volatile boolean aboutToCallSerializedOld = false;
    private volatile Object cachedSerializedOld = null;
    private volatile boolean finishedCallOfSerializedOld = false;
    private volatile boolean aboutToCallDeserializedOld = false;
    private volatile Object cachedDeserializedOld = null;
    private volatile boolean finishedCallOfDeserializedOld = false;
    
    public TestableEntryEventImpl(LocalRegion region, Object key,
        Object newValue, CountDownLatch releaseCountDown) {
      super(region, Operation.CREATE, key, newValue, null, false, null, false, createEventID());
      callbacksInvoked(true);
      this.releaseCountDown = releaseCountDown;
    }
    public Object getCachedDeserializedOld() {
      return this.cachedDeserializedOld;
    }
    public boolean hasFinishedCallOfDeserializedOld() {
      return this.finishedCallOfDeserializedOld;
    }
    public Object getCachedSerializedOld() {
      return this.cachedSerializedOld;
    }
    public boolean hasFinishedCallOfSerializedOld() {
      return this.finishedCallOfSerializedOld;
    }
    public Object getCachedDeserializedNew() {
      return this.cachedDeserializedNew;
    }
    public Object getTestCachedSerializedNew() {
      return this.testCachedSerializedNew;
    }
    public boolean hasFinishedCallOfDeserializedNew() {
      return this.finishedCallOfDeserializedNew;
    }
    public boolean hasFinishedCallOfSerializedNew() {
      return this.finishedCallOfSerializedNew;
    }
    public boolean isAboutToCallDeserializedOld() {
      return this.aboutToCallDeserializedOld;
    }
    public boolean isAboutToCallSerializedOld() {
      return this.aboutToCallSerializedOld;
    }
    public boolean isAboutToCallDeserializedNew() {
      return this.aboutToCallDeserializedNew;
    }
    public boolean isAboutToCallSerializedNew() {
      return this.aboutToCallSerializedNew;
    }
    public void getAndCachDeserializedOld(SerializedCacheValue<?> serializableOldValue) {
      try {
        this.aboutToCallDeserializedOld = true;
        this.cachedDeserializedOld = serializableOldValue.getDeserializedValue();
      } catch (IllegalStateException ex) {
        this.cachedDeserializedOld = ex;
      } finally {
        this.finishedCallOfDeserializedOld = true;
      }
    }
    public void getAndCacheSerializedOld(SerializedCacheValue<?> serializableOldValue) {
      try {
        this.aboutToCallSerializedOld = true;
        this.cachedSerializedOld = serializableOldValue.getSerializedValue();
      } catch (IllegalStateException ex) {
        this.cachedSerializedOld = ex;
      } finally {
        this.finishedCallOfSerializedOld = true;
      }
    }
    public void getAndCachDeserializedNew(SerializedCacheValue<?> serializableNewValue) {
      try {
        this.aboutToCallDeserializedNew = true;
        this.cachedDeserializedNew = serializableNewValue.getDeserializedValue();
      } catch (IllegalStateException ex) {
        this.cachedDeserializedNew = ex;
      } finally {
        this.finishedCallOfDeserializedNew = true;
      }
    }
    public void getAndCacheSerializedNew(SerializedCacheValue<?> serializableNewValue) {
      try {
        this.aboutToCallSerializedNew = true;
        this.testCachedSerializedNew = serializableNewValue.getSerializedValue();
      } catch (IllegalStateException ex) {
        this.testCachedSerializedNew = ex;
      } finally {
        this.finishedCallOfSerializedNew = true;
      }
    }
    public Object getCachedNewValue() {
      return this.cachedNewValue;
    }
    public void getAndCacheNewValue() {
      try {
        this.aboutToCallGetNewValue = true;
        this.cachedNewValue = getNewValue();
      } catch (IllegalStateException ex) {
        this.cachedNewValue = ex;
      } finally {
        this.finishedCallOfGetNewValue = true;
      }
    }
    public Object getCachedOldValue() {
      return this.cachedOldValue;
    }
    public void getAndCacheOldValue() {
      try {
        this.aboutToCallGetOldValue = true;
        this.cachedOldValue = getOldValue();
      } catch (IllegalStateException ex) {
        this.cachedOldValue = ex;
      } finally {
        this.finishedCallOfGetOldValue = true;
      }
    }
    public boolean isWaitingOnRelease() {
      return this.waitingOnRelease;
    }
    public boolean isAboutToCallGetNewValue() {
      return this.aboutToCallGetNewValue;
    }
    public boolean hasFinishedCallOfGetNewValue() {
      return this.finishedCallOfGetNewValue;
    }
    public boolean isAboutToCallGetOldValue() {
      return this.aboutToCallGetOldValue;
    }
    public boolean hasFinishedCallOfGetOldValue() {
      return this.finishedCallOfGetOldValue;
    }
    @Override
    void testHookReleaseInProgress() {
      try {
        this.waitingOnRelease = true;
        this.releaseCountDown.await();
      } catch (InterruptedException e) {
        // quit waiting
      }
    }
  }
  private static EventID createEventID() {
    byte[] memId = { 1,2,3 };
    return new EventID(memId, 11, 12, 13);
  }
  
  private EntryEventImpl createEntryEvent(LocalRegion l, Object newValue) {
    // create an event
    EntryEventImpl event = EntryEventImpl.create(l, Operation.CREATE, key,
        newValue, null,  false /* origin remote */, null,
        false /* generateCallbacks */,
        createEventID());
    // avoid calling invokeCallbacks
    event.callbacksInvoked(true);

    return event;
  }
}