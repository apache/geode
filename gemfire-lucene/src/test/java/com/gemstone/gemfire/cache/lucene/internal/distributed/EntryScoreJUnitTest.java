package com.gemstone.gemfire.cache.lucene.internal.distributed;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class EntryScoreJUnitTest {
  @Test
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    EntryScore entry = new EntryScore("entry", .1f);
    EntryScore copy = CopyHelper.deepCopy(entry);
    Assert.assertEquals("entry", copy.getKey());
    Assert.assertEquals(.1f, copy.getScore(), 0f);
  }
}
