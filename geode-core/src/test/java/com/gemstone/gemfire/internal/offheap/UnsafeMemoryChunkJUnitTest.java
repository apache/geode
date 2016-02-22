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
package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class UnsafeMemoryChunkJUnitTest extends MemoryChunkJUnitTestBase {

  @Override
  protected MemoryChunk createChunk(int size) {
    return new UnsafeMemoryChunk(size);
  }

  @Test
  public void testGetAddress() {
    MemoryChunk mc = createChunk(1024);
    try {
      AddressableMemoryChunk umc = (AddressableMemoryChunk) mc;
      assertNotEquals(0, umc.getMemoryAddress());
    } finally {
      mc.release();
    }
  }
  
  @Test(expected=AssertionError.class)
  public void readAbsoluteBytesFailsIfSizeLessThanZero() {
    UnsafeMemoryChunk.readAbsoluteBytes(0L, null, 0, -1);
  }
  @Test
  public void readAbsoluteBytesDoesNothingIfSizeIsZero() {
    UnsafeMemoryChunk.readAbsoluteBytes(0L, new byte[0], 0, 0);
  }
  @Test(expected=AssertionError.class)
  public void readAbsoluteBytesFailsIfSizeGreaterThanArrayLength() {
    UnsafeMemoryChunk.readAbsoluteBytes(0L, new byte[0], 0, 1);
  }
  @Test(expected=AssertionError.class)
  public void readAbsoluteBytesFailsIfByteOffsetNegative() {
    UnsafeMemoryChunk.readAbsoluteBytes(0L, new byte[0], -1, 0);
  }
  @Test(expected=AssertionError.class)
  public void readAbsoluteBytesFailsIfByteOffsetGreaterThanArrayLength() {
    UnsafeMemoryChunk.readAbsoluteBytes(0L, new byte[0], 1, 0);
  }
  
  @Test(expected=AssertionError.class)
  public void writeAbsoluteBytesFailsIfSizeLessThanZero() {
    UnsafeMemoryChunk.writeAbsoluteBytes(0L, null, 0, -1);
  }
  @Test
  public void writeAbsoluteBytesDoesNothingIfSizeIsZero() {
    UnsafeMemoryChunk.writeAbsoluteBytes(0L, new byte[0], 0, 0);
  }
  @Test(expected=AssertionError.class)
  public void writeAbsoluteBytesFailsIfSizeGreaterThanArrayLength() {
    UnsafeMemoryChunk.writeAbsoluteBytes(0L, new byte[0], 0, 1);
  }
  @Test(expected=AssertionError.class)
  public void writeAbsoluteBytesFailsIfByteOffsetNegative() {
    UnsafeMemoryChunk.writeAbsoluteBytes(0L, new byte[0], -1, 0);
  }
  @Test(expected=AssertionError.class)
  public void writeAbsoluteBytesFailsIfByteOffsetGreaterThanArrayLength() {
    UnsafeMemoryChunk.writeAbsoluteBytes(0L, new byte[0], 1, 0);
  }

}
