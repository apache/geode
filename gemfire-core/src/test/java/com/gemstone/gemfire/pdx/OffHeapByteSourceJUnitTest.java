package com.gemstone.gemfire.pdx;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.offheap.NullOffHeapMemoryStats;
import com.gemstone.gemfire.internal.offheap.NullOutOfOffHeapMemoryListener;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSource;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSourceFactory;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.OffHeapByteSource;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OffHeapByteSourceJUnitTest extends ByteSourceJUnitTest {

  @Before
  public void setUp() throws Exception {
    SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
  }

  @After
  public void tearDown() throws Exception {
    SimpleMemoryAllocatorImpl.freeOffHeapMemory();
  }

  @Override
  protected boolean isTestOffHeap() {
    return true;
  }
  
  @Override
  protected ByteSource createByteSource(byte[] bytes) {
    StoredObject so = SimpleMemoryAllocatorImpl.getAllocator().allocateAndInitialize(bytes, false, false, null);
    if (so instanceof Chunk) {
      // bypass the factory to make sure that OffHeapByteSource is tested
      return new OffHeapByteSource((Chunk)so);
    } else {
      // bytes are so small they can be encoded in a long (see DataAsAddress).
      // So for this test just wrap the original bytes.
      return ByteSourceFactory.wrap(bytes);
    }
  }

}
