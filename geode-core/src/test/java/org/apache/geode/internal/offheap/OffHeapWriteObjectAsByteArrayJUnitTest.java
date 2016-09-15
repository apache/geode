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
package org.apache.geode.internal.offheap;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OffHeapWriteObjectAsByteArrayJUnitTest {

  @Before
  public void setUp() throws Exception {
    MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new SlabImpl[]{new SlabImpl(1024*1024)});
  }

  @After
  public void tearDown() throws Exception {
    MemoryAllocatorImpl.freeOffHeapMemory();
  }
  
  private StoredObject createStoredObject(byte[] bytes, boolean isSerialized, boolean isCompressed) {
    return MemoryAllocatorImpl.getAllocator().allocateAndInitialize(bytes, isSerialized, isCompressed);
  }
  
  private DataInputStream createInput(HeapDataOutputStream hdos) {
    ByteArrayInputStream bais = new ByteArrayInputStream(hdos.toByteArray());
    return new DataInputStream(bais);
  }
  
  @Test
  public void testByteArrayChunk() throws IOException, ClassNotFoundException {
    byte[] expected = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    StoredObject so = createStoredObject(expected, false, false);
    assertTrue(so instanceof OffHeapStoredObject);
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
    assertTrue(so instanceof TinyStoredObject);
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
    assertTrue(so instanceof OffHeapStoredObject);
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
    assertTrue(so instanceof TinyStoredObject);
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
