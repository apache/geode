package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneQueryFactoryImplJUnitTest {

  @Test
  public void test() {
    Cache cache = Mockito.mock(Cache.class);
    LuceneQueryFactoryImpl f = new LuceneQueryFactoryImpl(cache);
    f.setPageSize(5);
    f.setResultLimit(25);
    String[] projection = new String[] {"a", "b"};
    f.setProjectionFields(projection);
    LuceneQuery<Object, Object> query = f.create("index", "region", new StringQueryProvider("test"));
    assertEquals(25, query.getLimit());
    assertEquals(5, query.getPageSize());
    assertArrayEquals(projection, query.getProjectedFieldNames());
    
    Mockito.verify(cache).getRegion(Mockito.eq("region"));
  }

}
