package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import org.junit.Assert;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.InternalLuceneIndex;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
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
        // TODO: we have to workarround it now: specify an AEQ id when creating data region
        String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
        RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        Region<Object, Object> region = regionFactory.
            addAsyncEventQueueId(aeqId). // TODO: we need it for the time being
            create(REGION_NAME);
        LuceneService service = LuceneServiceProvider.get(cache);
        InternalLuceneIndex index = (InternalLuceneIndex) service.createIndex(INDEX_NAME, REGION_NAME, "text");
        return null;
      }
    };
        
    server1.invoke(createPartitionRegion);
    

    SerializableCallable createSomeData = new SerializableCallable("createRegion") {
      private static final long serialVersionUID = 1L;

      public Object call() throws Exception {
        final Cache cache = getCache();
        Region<Object, Object> region = cache.getRegion(REGION_NAME);
        
        putInRegion(region, 1, new TestObject("hello world"));
        putInRegion(region, 113, new TestObject("hi world"));
        putInRegion(region, 2, new TestObject("goodbye world"));
        
        return null;
      }
    };

    server1.invoke(createSomeData);
    server2.invoke(createPartitionRegion);

    SerializableCallable executeSearch = new SerializableCallable("executeSearch") {
      private static final long serialVersionUID = 1L;

      public Object call() throws Exception {
        Cache cache = getCache();
        assertNotNull(cache);
        Region<Object, Object> region = cache.getRegion(REGION_NAME);
        Assert.assertNotNull(region);

        LuceneService service = LuceneServiceProvider.get(cache);
        LuceneQuery<Integer, TestObject> query = service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "text:world");
        LuceneQueryResults<Integer, TestObject> results = query.search();
        assertEquals(3, results.size());
        List<LuceneResultStruct<Integer, TestObject>> page = results.getNextPage();
        
        Map<Integer, TestObject> data = new HashMap<Integer, TestObject>();
        for(LuceneResultStruct<Integer, TestObject> row : page) {
          data.put(row.getKey(), row.getValue());
          System.out.println("GGG:"+row.getKey()+":"+row.getValue());
        }
        
        assertEquals(data, region);
        
        return null;
      }
    };

    //Make sure we can search from both members
    server1.invoke(executeSearch);
    server2.invoke(executeSearch);

    //Do a rebalance
    server1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws CancellationException, InterruptedException {
        RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();
        RebalanceResults results = op.getResults();
        assertTrue(1 < results.getTotalBucketTransfersCompleted());
        return null;
      }
    });
    
    //Make sure the search still works
    // TODO: rebalance is broken when hooked with AEQ, disable the test for the time being
//    server1.invoke(executeSearch);
//    server2.invoke(executeSearch);
  }
  
  private static void putInRegion(Region<Object, Object> region, Object key, Object value) throws BucketNotFoundException, IOException {
    region.put(key, value);
    
    //TODO - the async event queue hasn't been hooked up, so we'll fake out
    //writing the entry to the repository.
//    LuceneService service = LuceneServiceProvider.get(region.getCache());
//    InternalLuceneIndex index = (InternalLuceneIndex) service.getIndex(INDEX_NAME, REGION_NAME);
//    IndexRepository repository1 = index.getRepositoryManager().getRepository(region, 1, null);
//    repository1.create(key, value);
//    repository1.commit();
  }

  private static class TestObject implements Serializable {
    private String text;

    public TestObject(String text) {
      this.text = text;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((text == null) ? 0 : text.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TestObject other = (TestObject) obj;
      if (text == null) {
        if (other.text != null)
          return false;
      } else if (!text.equals(other.text))
        return false;
      return true;
    }

    
    
  }

}
