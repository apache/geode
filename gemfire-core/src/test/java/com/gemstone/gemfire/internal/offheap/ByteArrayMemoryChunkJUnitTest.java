package com.gemstone.gemfire.internal.offheap;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ByteArrayMemoryChunkJUnitTest extends MemoryChunkJUnitTestBase {
  @Override
  protected MemoryChunk createChunk(int size) {
    return new ByteArrayMemoryChunk(size);
  }

}
