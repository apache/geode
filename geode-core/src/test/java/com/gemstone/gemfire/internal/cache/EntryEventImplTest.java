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
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.cache.EntryEventImpl.NewValueImporter;
import com.gemstone.gemfire.internal.cache.EntryEventImpl.OldValueImporter;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

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
  public void verifyExportOewValueWithSerializedStoredObjectAndUnretainedOldReferenceOk() {
    LocalRegion region = mock(LocalRegion.class);
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

  private EntryEventImpl createEntryEvent(LocalRegion l, Object newValue) {
    // create a dummy event id
    byte[] memId = { 1,2,3 };
    EventID eventId = new EventID(memId, 11, 12, 13);

    // create an event
    EntryEventImpl event = EntryEventImpl.create(l, Operation.CREATE, key,
        newValue, null,  false /* origin remote */, null,
        false /* generateCallbacks */,
        eventId);
    // avoid calling invokeCallbacks
    event.callbacksInvoked(true);

    return event;
  }
}