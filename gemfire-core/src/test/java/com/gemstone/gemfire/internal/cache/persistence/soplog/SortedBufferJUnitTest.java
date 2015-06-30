package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory.SortedOplogConfiguration;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SortedBufferJUnitTest extends SortedReaderTestCase {
  @Override
  protected SortedReader<ByteBuffer> createReader(NavigableMap<byte[], byte[]> data) {
    SortedOplogConfiguration config = new SortedOplogConfiguration("test");
    SortedBuffer<Integer> sb = new SortedBuffer<Integer>(config, 0);
    for (Entry<byte[], byte[]> entry : data.entrySet()) {
      sb.put(entry.getKey(), entry.getValue());
    }
    return sb;
  }
}
