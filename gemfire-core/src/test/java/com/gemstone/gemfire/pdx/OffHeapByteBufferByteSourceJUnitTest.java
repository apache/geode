package com.gemstone.gemfire.pdx;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSource;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSourceFactory;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OffHeapByteBufferByteSourceJUnitTest extends OffHeapByteSourceJUnitTest {
  
  @Override
  protected ByteSource createByteSource(byte[] bytes) {
    StoredObject so = SimpleMemoryAllocatorImpl.getAllocator().allocateAndInitialize(bytes, false, false, null);
    if (so instanceof Chunk) {
      Chunk c = (Chunk) so;
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
