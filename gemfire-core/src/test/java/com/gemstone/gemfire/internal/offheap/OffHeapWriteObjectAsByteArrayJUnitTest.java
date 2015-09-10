package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.offheap.NullOffHeapMemoryStats;
import com.gemstone.gemfire.internal.offheap.NullOutOfOffHeapMemoryListener;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.DataAsAddress;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OffHeapWriteObjectAsByteArrayJUnitTest {

  @Before
  public void setUp() throws Exception {
    SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
  }

  @After
  public void tearDown() throws Exception {
    SimpleMemoryAllocatorImpl.freeOffHeapMemory();
  }
  
  private StoredObject createStoredObject(byte[] bytes, boolean isSerialized, boolean isCompressed) {
    return SimpleMemoryAllocatorImpl.getAllocator().allocateAndInitialize(bytes, isSerialized, isCompressed, null);
  }
  
  private DataInputStream createInput(HeapDataOutputStream hdos) {
    ByteArrayInputStream bais = new ByteArrayInputStream(hdos.toByteArray());
    return new DataInputStream(bais);
  }
  
  @Test
  public void testByteArrayChunk() throws IOException, ClassNotFoundException {
    byte[] expected = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    StoredObject so = createStoredObject(expected, false, false);
    assertTrue(so instanceof Chunk);
    HeapDataOutputStream hdos = new HeapDataOutputStream(new byte[1024]);
    DataSerializer.writeObjectAsByteArray(so, hdos);
    DataInputStream in = createInput(hdos);
    byte[] actual = DataSerializer.readByteArray(in);
    assertArrayEquals(expected, actual);
  }
  
  @Test
  public void testByteArrayDataAsAddress() throws IOException, ClassNotFoundException {
    byte[] expected = new byte[] {1, 2, 3};
    StoredObject so = createStoredObject(expected, false, false);
    assertTrue(so instanceof DataAsAddress);
    HeapDataOutputStream hdos = new HeapDataOutputStream(new byte[1024]);
    DataSerializer.writeObjectAsByteArray(so, hdos);
    DataInputStream in = createInput(hdos);
    byte[] actual = DataSerializer.readByteArray(in);
    assertArrayEquals(expected, actual);
  }
  
  @Test
  public void testStringChunk() throws IOException, ClassNotFoundException {
    byte[] expected = EntryEventImpl.serialize("1234567890");
    StoredObject so = createStoredObject(expected, true, false);
    assertTrue(so instanceof Chunk);
    HeapDataOutputStream hdos = new HeapDataOutputStream(new byte[1024]);
    DataSerializer.writeObjectAsByteArray(so, hdos);
    DataInputStream in = createInput(hdos);
    byte[] actual = DataSerializer.readByteArray(in);
    assertArrayEquals(expected, actual);
    assertNoMoreInput(in);
  }
  
  @Test
  public void testStringDataAsAddress() throws IOException, ClassNotFoundException {
    byte[] expected = EntryEventImpl.serialize("1234");
    StoredObject so = createStoredObject(expected, true, false);
    assertTrue(so instanceof DataAsAddress);
    HeapDataOutputStream hdos = new HeapDataOutputStream(new byte[1024]);
    DataSerializer.writeObjectAsByteArray(so, hdos);
    DataInputStream in = createInput(hdos);
    byte[] actual = DataSerializer.readByteArray(in);
    assertArrayEquals(expected, actual);
  }
  
  private void assertNoMoreInput(DataInputStream in) throws IOException {
    assertEquals(0, in.available());
  }
}
