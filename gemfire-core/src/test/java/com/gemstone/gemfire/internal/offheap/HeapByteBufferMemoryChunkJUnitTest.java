package com.gemstone.gemfire.internal.offheap;

import java.nio.ByteBuffer;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class HeapByteBufferMemoryChunkJUnitTest extends MemoryChunkJUnitTestBase {

  @Override
  protected MemoryChunk createChunk(int size) {
    return new ByteBufferMemoryChunk(ByteBuffer.allocate(size));
  }

}
