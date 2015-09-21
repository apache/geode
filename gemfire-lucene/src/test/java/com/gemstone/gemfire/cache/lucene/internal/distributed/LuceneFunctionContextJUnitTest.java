package com.gemstone.gemfire.cache.lucene.internal.distributed;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.cache.lucene.internal.StringQueryProvider;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneFunctionContextJUnitTest {
  @Test
  public void testLuceneFunctionArgsDefaults() {
    LuceneFunctionContext<IndexResultCollector> context = new LuceneFunctionContext<>();
    assertEquals(LuceneQueryFactory.DEFAULT_LIMIT, context.getLimit());
    assertEquals(DataSerializableFixedID.LUCENE_FUNCTION_CONTEXT, context.getDSFID());
  }

  @Test
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();

    LuceneIndex mockIndex = Mockito.mock(LuceneIndex.class);
    Mockito.doReturn("mockIndex").when(mockIndex).getName();
    LuceneQueryProvider provider = new StringQueryProvider(mockIndex, "text");
    CollectorManager<TopEntriesCollector> manager = new TopEntriesCollectorManager("test");
    LuceneFunctionContext<TopEntriesCollector> context = new LuceneFunctionContext<>(provider, manager, 123);

    LuceneFunctionContext<TopEntriesCollector> copy = CopyHelper.deepCopy(context);
    assertEquals(123, copy.getLimit());
    Assert.assertNotNull(copy.getQueryProvider());
    assertEquals("text", ((StringQueryProvider) copy.getQueryProvider()).getQueryString());
    assertEquals("mockIndex", ((StringQueryProvider) copy.getQueryProvider()).getIndexName());
    assertEquals(TopEntriesCollectorManager.class, copy.getCollectorManager().getClass());
    assertEquals("test", ((TopEntriesCollectorManager) copy.getCollectorManager()).getId());
  }
}
