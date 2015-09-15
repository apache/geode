package com.gemstone.gemfire.cache.lucene.internal;

import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class StringQueryProviderJUnitTest {
  static final String INDEXED_REGION = "indexedRegion";

  LuceneIndex mockIndex;

  @Before
  public void initMocksAndCommonObjects() {
    mockIndex = Mockito.mock(LuceneIndex.class);
    String[] fields = { "field-1", "field-2" };
    Mockito.doReturn(fields).when(mockIndex).getFieldNames();
  }

  @Test
  public void testQueryConstruction() throws QueryException {
    StringQueryProvider provider = new StringQueryProvider("foo:bar");
    Query query = provider.getQuery();
    Assert.assertNotNull(query);
    Assert.assertEquals("foo:bar", query.toString());
  }

  @Test
  public void usesSearchableFieldsAsDefaults() throws QueryException {
    StringQueryProvider provider = new StringQueryProvider(mockIndex, "findThis");
    Query query = provider.getQuery();
    Assert.assertNotNull(query);
    Assert.assertEquals("field-1:findthis field-2:findthis", query.toString());
  }

  @Test
  public void usesCustomAnalyzer() throws QueryException {
    StringQueryProvider provider = new StringQueryProvider(mockIndex, "findThis");
    Query query = provider.getQuery();
    Assert.assertNotNull(query);
    Assert.assertEquals("field-1:findthis field-2:findthis", query.toString());
  }

  @Test(expected = QueryException.class)
  public void errorsOnMalformedQueryString() throws QueryException {
    StringQueryProvider provider = new StringQueryProvider(mockIndex, "invalid:lucene:query:string");
    provider.getQuery();
  }
}
