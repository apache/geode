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

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.offheap.OffHeapStoredObject;
import com.gemstone.gemfire.internal.offheap.NullOffHeapMemoryStats;
import com.gemstone.gemfire.internal.offheap.NullOutOfOffHeapMemoryListener;
import com.gemstone.gemfire.internal.offheap.MemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.offheap.SlabImpl;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSource;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSourceFactory;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.OffHeapByteSource;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OffHeapByteSourceJUnitTest extends ByteSourceJUnitTest {

  @Before
  public final void setUp() throws Exception {
    MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new SlabImpl[]{new SlabImpl(1024*1024)});
  }

  @After
  public final void tearDown() throws Exception {
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

  @Override
  protected boolean isTestOffHeap() {
    return true;
  }
  
  @Override
  protected ByteSource createByteSource(byte[] bytes) {
    StoredObject so = MemoryAllocatorImpl.getAllocator().allocateAndInitialize(bytes, false, false);
    if (so instanceof OffHeapStoredObject) {
      // bypass the factory to make sure that OffHeapByteSource is tested
      return new OffHeapByteSource(so);
    } else {
      // bytes are so small they can be encoded in a long (see DataAsAddress).
      // So for this test just wrap the original bytes.
      return ByteSourceFactory.wrap(bytes);
    }
  }

}
