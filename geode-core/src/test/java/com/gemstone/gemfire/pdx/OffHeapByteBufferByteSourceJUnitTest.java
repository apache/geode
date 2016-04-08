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
package com.gemstone.gemfire.pdx;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.offheap.OffHeapStoredObject;
import com.gemstone.gemfire.internal.offheap.MemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSource;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSourceFactory;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OffHeapByteBufferByteSourceJUnitTest extends OffHeapByteSourceJUnitTest {
  
  @Override
  protected ByteSource createByteSource(byte[] bytes) {
    StoredObject so = MemoryAllocatorImpl.getAllocator().allocateAndInitialize(bytes, false, false);
    if (so instanceof OffHeapStoredObject) {
      OffHeapStoredObject c = (OffHeapStoredObject) so;
      ByteBuffer bb = c.createDirectByteBuffer();
      if (bb == null) {
        fail("could not create a direct ByteBuffer for an off-heap Chunk");
      }
      return ByteSourceFactory.create(bb);
    } else {
      // bytes are so small they can be encoded in a long (see DataAsAddress).
      // So for this test just wrap the original bytes.
      return ByteSourceFactory.wrap(bytes);
    }
  }

}
