package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.EntryEventImpl.OldValueImporter;
import com.gemstone.gemfire.internal.offheap.NullOffHeapMemoryStats;
import com.gemstone.gemfire.internal.offheap.NullOutOfOffHeapMemoryListener;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.DataAsAddress;
import com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk;
import com.gemstone.gemfire.internal.util.BlobHelper;

public abstract class OldValueImporterTestBase {
  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }
  
  protected abstract OldValueImporter createImporter();
  protected abstract Object getOldValueFromImporter(OldValueImporter ovi);
  protected abstract void toData(OldValueImporter ovi, HeapDataOutputStream hdos) throws IOException;
  protected abstract void fromData(OldValueImporter ovi, byte[] bytes) throws IOException, ClassNotFoundException;

  @Test
  public void testValueSerialization() throws IOException, ClassNotFoundException {
    byte[] bytes = new byte[1024];
    HeapDataOutputStream hdos = new HeapDataOutputStream(bytes);
    OldValueImporter imsg = createImporter();

    // null byte array value
    {
      OldValueImporter omsg = createImporter();
      omsg.importOldBytes(null, false);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertEquals(null, getOldValueFromImporter(imsg));
    }
    
    // null object value
    {
      OldValueImporter omsg = createImporter();
      omsg.importOldObject(null, true);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertEquals(null, getOldValueFromImporter(imsg));
    }
    
    // simple byte array
    {
      byte[] baValue = new byte[] {1,2,3,4,5,6,7,8,9};
      OldValueImporter omsg = createImporter();
      omsg.importOldBytes(baValue, false);
      hdos = new HeapDataOutputStream(bytes);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertArrayEquals(baValue, (byte[])getOldValueFromImporter(imsg));
    }
    
    // String in serialized form
    {
      String stringValue = "1,2,3,4,5,6,7,8,9";
      byte[] stringValueBlob = EntryEventImpl.serialize(stringValue);
      OldValueImporter omsg = createImporter();
      omsg.importOldBytes(stringValueBlob, true);
      hdos = new HeapDataOutputStream(bytes);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertArrayEquals(stringValueBlob, ((VMCachedDeserializable)getOldValueFromImporter(imsg)).getSerializedValue());
    }
    
    // String in object form
    {
      String stringValue = "1,2,3,4,5,6,7,8,9";
      byte[] stringValueBlob = EntryEventImpl.serialize(stringValue);
      OldValueImporter omsg = createImporter();
      omsg.importOldObject(stringValue, true);
      hdos = new HeapDataOutputStream(bytes);
      toData(omsg, hdos);
      fromData(imsg, bytes);
      assertArrayEquals(stringValueBlob, ((VMCachedDeserializable)getOldValueFromImporter(imsg)).getSerializedValue());
    }
    
    // off-heap DataAsAddress byte array
    {
      SimpleMemoryAllocatorImpl sma =
          SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
      try {
        byte[] baValue = new byte[] {1,2};
        DataAsAddress baValueSO = (DataAsAddress) sma.allocateAndInitialize(baValue, false, false, null);
        OldValueImporter omsg = createImporter();
        omsg.importOldObject(baValueSO, false);
        hdos = new HeapDataOutputStream(bytes);
        toData(omsg, hdos);
        fromData(imsg, bytes);
        assertArrayEquals(baValue, (byte[])getOldValueFromImporter(imsg));
      } finally {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
    }
    // off-heap Chunk byte array
    {
      SimpleMemoryAllocatorImpl sma =
          SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
      try {
        byte[] baValue = new byte[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17};
        Chunk baValueSO = (Chunk) sma.allocateAndInitialize(baValue, false, false, null);
        OldValueImporter omsg = createImporter();
        omsg.importOldObject(baValueSO, false);
        hdos = new HeapDataOutputStream(bytes);
        toData(omsg, hdos);
        fromData(imsg, bytes);
        assertArrayEquals(baValue, (byte[])getOldValueFromImporter(imsg));
      } finally {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
    }
    // off-heap DataAsAddress String
    {
      SimpleMemoryAllocatorImpl sma =
          SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
      try {
        String baValue = "12";
        byte[] baValueBlob = BlobHelper.serializeToBlob(baValue);
        DataAsAddress baValueSO = (DataAsAddress) sma.allocateAndInitialize(baValueBlob, true, false, null);
        OldValueImporter omsg = createImporter();
        omsg.importOldObject(baValueSO, true);
        hdos = new HeapDataOutputStream(bytes);
        toData(omsg, hdos);
        fromData(imsg, bytes);
        assertArrayEquals(baValueBlob, ((VMCachedDeserializable)getOldValueFromImporter(imsg)).getSerializedValue());
      } finally {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
    }
    // off-heap Chunk String
    {
      SimpleMemoryAllocatorImpl sma =
          SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
      try {
        String baValue = "12345678";
        byte[] baValueBlob = BlobHelper.serializeToBlob(baValue);
        Chunk baValueSO = (Chunk) sma.allocateAndInitialize(baValueBlob, true, false, null);
        OldValueImporter omsg = createImporter();
        omsg.importOldObject(baValueSO, true);
        hdos = new HeapDataOutputStream(bytes);
        toData(omsg, hdos);
        fromData(imsg, bytes);
        assertArrayEquals(baValueBlob, ((VMCachedDeserializable)getOldValueFromImporter(imsg)).getSerializedValue());
      } finally {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
    }
  }
}
