package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneQueryFunction;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneServiceImplJUnitTest {
  Cache cache;

  // lucene service will register query execution function on initialization
  @Test
  public void shouldRegisterQueryFunction() {
    Function function = FunctionService.getFunction(LuceneQueryFunction.ID);
    assertNull(function);

    cache = createBasicCache();
    new LuceneServiceImpl(cache);

    function = FunctionService.getFunction(LuceneQueryFunction.ID);
    assertNotNull(function);
  }

  private GemFireCacheImpl createBasicCache() {
    return (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").create();
  }

  @After
  public void destroyCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
  }
}
