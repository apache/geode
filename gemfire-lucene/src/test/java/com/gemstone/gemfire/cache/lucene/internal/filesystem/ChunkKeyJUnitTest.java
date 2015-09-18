package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import static org.junit.Assert.*;

import java.util.UUID;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ChunkKeyJUnitTest {

  @Test
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    ChunkKey key = new ChunkKey(UUID.randomUUID(), 5);
    ChunkKey copy = CopyHelper.deepCopy(key);
    
    assertEquals(key, copy);
    assertEquals(key.hashCode(), copy.hashCode());
    assertEquals(key.getFileId(), copy.getFileId());
    assertEquals(key.getChunkId(), copy.getChunkId());
  }

}
