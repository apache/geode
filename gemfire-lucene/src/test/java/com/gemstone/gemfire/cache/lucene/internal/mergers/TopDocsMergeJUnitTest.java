package com.gemstone.gemfire.cache.lucene.internal.mergers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.cache.lucene.internal.LuceneQueryResultsImpl;
import com.gemstone.gemfire.cache.lucene.internal.LuceneResultStructImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TopDocsMergeJUnitTest {
  Mockery mockContext;

  @Before
  public void setupMock() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };
  }

  @After
  public void validateMock() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void mergeMultipleShardResults() throws Exception {
    LuceneResultStructImpl r1_1 = new LuceneResultStructImpl("k1_1", .9f);
    LuceneResultStructImpl r1_2 = new LuceneResultStructImpl("k1_2", .8f);

    LuceneQueryResultsImpl r1 = new LuceneQueryResultsImpl();
    r1.addHit(r1_1);
    r1.addHit(r1_2);

    LuceneResultStructImpl r2_1 = new LuceneResultStructImpl("k2_1", 0.85f);
    LuceneResultStructImpl r2_2 = new LuceneResultStructImpl("k2_2", 0.1f);
    LuceneQueryResultsImpl r2 = new LuceneQueryResultsImpl();
    r2.addHit(r2_1);
    r2.addHit(r2_2);

    List<LuceneQueryResults> results = new ArrayList<LuceneQueryResults>();
    results.add(r1);
    results.add(r2);

    ResultMerger<LuceneResultStruct> merger = new TopDocsResultMerger();
    LuceneQueryResults merged = merger.mergeResults(results);
    assertNotNull(merged);
    assertEquals(4, merged.size());
    assertEquals(.9f, merged.getMaxScore(), 0f);

    verifyResultOrder(merged, r1_1, r2_1, r1_2, r2_2);
  }

  @Test
  public void mergeShardAndLimitResults() throws Exception {
    LuceneResultStructImpl r1_1 = new LuceneResultStructImpl("k1_1", .9f);
    LuceneResultStructImpl r1_2 = new LuceneResultStructImpl("k1_2", .8f);

    LuceneQueryResultsImpl r1 = new LuceneQueryResultsImpl();
    r1.addHit(r1_1);
    r1.addHit(r1_2);
    assertEquals(2, r1.size());

    LuceneResultStructImpl r2_1 = new LuceneResultStructImpl("k2_1", 0.85f);
    LuceneResultStructImpl r2_2 = new LuceneResultStructImpl("k2_2", 0.1f);
    LuceneQueryResultsImpl r2 = new LuceneQueryResultsImpl();
    r2.addHit(r2_1);
    r2.addHit(r2_2);
    assertEquals(2, r2.size());

    List<LuceneQueryResults> results = new ArrayList<LuceneQueryResults>();
    results.add(r1);
    results.add(r2);

    final LuceneQuery mockQuery = mockContext.mock(LuceneQuery.class);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockQuery).getLimit();
        will(returnValue(3));
      }
    });

    ResultMerger<LuceneResultStruct> merger = new TopDocsResultMerger();
    merger.init(mockQuery);
    LuceneQueryResults merged = merger.mergeResults(results);
    assertNotNull(merged);
    assertEquals(3, merged.size());
    assertEquals(.9f, merged.getMaxScore(), 0f);

    verifyResultOrder(merged, r1_1, r2_1, r1_2);
  }

  @Test
  public void mergeDocsWithSameScoreandSameHashcode() throws Exception {
    class KeyObject {
      String str;

      public KeyObject(String str) {
        this.str = str;
      }

      @Override
      public int hashCode() {
        return str.length();
      }

      @Override
      public boolean equals(Object obj) {
        KeyObject tmp = (KeyObject) obj;
        return tmp.str.equals(this.str);
      }
      
      @Override
      public String toString() {
        return str;
      }
    }

    // Add at least two docs with different key, same score, same hashcode
    KeyObject key1 = new KeyObject("1_2");
    KeyObject key2 = new KeyObject("2_1");
    float keyScore = .5f;
    assertNotEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());

    LuceneQueryResultsImpl r1 = new LuceneQueryResultsImpl();
    LuceneResultStructImpl r1_1 = new LuceneResultStructImpl(new KeyObject("1_1"), .9f);
    LuceneResultStructImpl r1_2 = new LuceneResultStructImpl(key1, keyScore);
    LuceneResultStructImpl r1_3 = new LuceneResultStructImpl(new KeyObject("1_3"), .2f);
    r1.addHit(r1_1);
    r1.addHit(r1_2);
    r1.addHit(r1_3);

    LuceneResultStructImpl r2_1 = new LuceneResultStructImpl(key2, keyScore);
    LuceneResultStructImpl r2_2 = new LuceneResultStructImpl(new KeyObject("2_2"), .1f);
    LuceneQueryResultsImpl r2 = new LuceneQueryResultsImpl();
    r2.addHit(r2_1);
    r2.addHit(r2_2);

    List<LuceneQueryResults> results = new ArrayList<LuceneQueryResults>();
    results.add(r1);
    results.add(r2);

    ResultMerger<LuceneResultStruct> merger = new TopDocsResultMerger();
    LuceneQueryResults merged = merger.mergeResults(results);
    assertNotNull(merged);
    assertEquals(5, merged.size());

    verifyResultOrder(merged, r1_1, r2_1, r1_2, r1_3, r2_2);
  }

  private void verifyResultOrder(LuceneQueryResults merged, LuceneResultStruct... results) {
    int i = 0;
    for (LuceneResultStruct result : results) {
      LuceneResultStruct doc = merged.getHits().get(i++);
      assertEquals(result.getKey(), doc.getKey());
      assertEquals(result.getScore(), doc.getScore(), .0f);
    }
  }

  @Test
  public void verifyDefaultLimit() throws Exception {
    LuceneQueryResultsImpl r1 = new LuceneQueryResultsImpl();
    for (int i = 0; i < LuceneQueryFactory.DEFAULT_LIMIT; i++) {
      r1.addHit(new LuceneResultStructImpl("k1_" + i, .9f));
    }
    assertEquals(LuceneQueryFactory.DEFAULT_LIMIT, r1.size());

    LuceneQueryResultsImpl r2 = new LuceneQueryResultsImpl();
    for (int i = 0; i < LuceneQueryFactory.DEFAULT_LIMIT; i++) {
      r2.addHit(new LuceneResultStructImpl("k2_" + i, .9f));
    }
    assertEquals(LuceneQueryFactory.DEFAULT_LIMIT, r2.size());

    List<LuceneQueryResults> results = new ArrayList<LuceneQueryResults>();
    results.add(r1);
    results.add(r2);

    ResultMerger<LuceneResultStruct> merger = new TopDocsResultMerger();
    LuceneQueryResults merged = merger.mergeResults(results);
    assertNotNull(merged);
    assertEquals(LuceneQueryFactory.DEFAULT_LIMIT, merged.size());
    assertEquals(.9f, merged.getMaxScore(), 0f);
  }
}
