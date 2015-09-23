package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.Serializable;

import org.junit.Assert;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.InternalLuceneIndex;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

@Category(DistributedTest.class)
public class LuceneFunctionReadPathDUnitTest extends CacheTestCase {
  private static final String INDEX_NAME = "index";
  private static final String REGION_NAME = "indexedRegion";

  private static final long serialVersionUID = 1L;

  private VM server1;
  private VM server2;

  public LuceneFunctionReadPathDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
  }

  public void testEnd2EndFunctionExecution() {
    SerializableCallable createPartitionRegion = new SerializableCallable("createRegion") {
      private static final long serialVersionUID = 1L;

      public Object call() throws Exception {
        final Cache cache = getCache();
        assertNotNull(cache);
        RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        Region<Object, Object> region = regionFactory.create(REGION_NAME);
        

        LuceneService service = LuceneServiceProvider.get(cache);
        InternalLuceneIndex index = (InternalLuceneIndex) service.createIndex(INDEX_NAME, REGION_NAME, "text");
        
        
        region.put(1, new TestObject("hello world"));
        region.put(2, new TestObject("goodbye world"));
        
        //TODO - the async event queue hasn't been hooked up, so we'll fake out
        //writing the entry to the repository.
        try {
        IndexRepository repository1 = index.getRepositoryManager().getRepository(region, 1, null);
        repository1.create(1, new TestObject("hello world"));
        repository1.commit();
        IndexRepository repository2 = index.getRepositoryManager().getRepository(region, 2, null);
        repository2.create(2, new TestObject("hello world"));
        repository2.commit();
        } catch(BucketNotFoundException e) {
          //thats ok, one of the data stores does not host these buckets.
        }
        return null;
      }
    };

    server1.invoke(createPartitionRegion);
    server2.invoke(createPartitionRegion);

    SerializableCallable executeSearch = new SerializableCallable("executeSearch") {
      private static final long serialVersionUID = 1L;

      public Object call() throws Exception {
        Cache cache = getCache();
        assertNotNull(cache);
        Region<Object, Object> region = cache.getRegion(REGION_NAME);
        Assert.assertNotNull(region);

        LuceneService service = LuceneServiceProvider.get(cache);
        LuceneQuery query = service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "text:world");
        LuceneQueryResults results = query.search();
        assertEquals(2, results.size());
        
        return null;
      }
    };

    server1.invoke(executeSearch);
  }
  
  private static class TestObject implements Serializable {
    private String text;

    public TestObject(String text) {
      this.text = text;
    }
    
    
  }

}
