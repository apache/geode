package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneQueryResultsJUnitTest {
  @Test
  public void addHitTest() {
    LuceneQueryResultsImpl results = new LuceneQueryResultsImpl();
    assertEquals(0, results.getHits().size());
    results.addHit(new LuceneResultStructImpl("key", 0.1f));
    assertEquals(1, results.getHits().size());
    assertEquals(0.1f, results.getMaxScore(), 0f);
    
    results.addHit(new LuceneResultStructImpl("key", 0.2f));
    assertEquals(2, results.getHits().size());
    assertEquals(0.2f, results.getMaxScore(), 0f);
  }
}
