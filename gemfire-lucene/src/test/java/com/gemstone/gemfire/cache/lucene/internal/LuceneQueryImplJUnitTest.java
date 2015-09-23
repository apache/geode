package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunction;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunctionContext;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollector;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class LuceneQueryImplJUnitTest {

  private Cache cache;
  private Region<Object, Object> region;
  @Before
  public void createCache() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
  }
  
  @After
  public void removeCache() {
    FunctionService.unregisterFunction(LuceneFunction.ID);
    cache.close();
  }
  @Test
  public void test() {
    //Register a fake function to observe the function invocation
    FunctionService.unregisterFunction(LuceneFunction.ID);
    TestLuceneFunction function = new TestLuceneFunction();
    FunctionService.registerFunction(function);
    
    
    StringQueryProvider provider = new StringQueryProvider();
    LuceneQueryImpl query = new LuceneQueryImpl("index", region, provider, null, 100, 20);
    LuceneQueryResults results = query.search();
    List nextPage = results.getNextPage();
    assertEquals(3, nextPage.size());
    assertEquals(.3f, results.getMaxScore(), 0.01);
    assertTrue(function.wasInvoked);
    
    LuceneFunctionContext args = (LuceneFunctionContext) function.args;
    assertEquals(provider.getQueryString(), ((StringQueryProvider) args.getQueryProvider()).getQueryString());
    assertEquals("index", args.getIndexName());
    assertEquals(100, args.getLimit());
  }

  private static class TestLuceneFunction extends FunctionAdapter {

    private boolean wasInvoked;
    private Object args;

    @Override
    public void execute(FunctionContext context) {
      this.args = context.getArguments();
      wasInvoked = true;
      TopEntriesCollector lastResult = new TopEntriesCollector();
      lastResult.collect(3, .3f);
      lastResult.collect(2, .2f);
      lastResult.collect(1, .1f);
      context.getResultSender().lastResult(lastResult);
    }

    @Override
    public String getId() {
      return LuceneFunction.ID;
    }
  }
}
