package com.gemstone.gemfire.cache.lucene.internal.distributed;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.StringQueryProvider;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import dunit.Host;
import dunit.SerializableRunnable;
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
    SerializableRunnable createPartitionRegion = new SerializableRunnable("createRegion") {
      private static final long serialVersionUID = 1L;

      public void run() {
        final Cache cache = getCache();
        assertNotNull(cache);
        RegionFactory<Object, Object> regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        regionFactory.create(REGION_NAME);

        LuceneService service = LuceneServiceProvider.get(cache);
        service.createIndex(INDEX_NAME, REGION_NAME);

        IndexRepository mockRepo = mock(IndexRepository.class);
        Collection<IndexRepository> repos = new ArrayList<IndexRepository>();
        repos.add(mockRepo);

        RepositoryManager mockManager = mock(RepositoryManager.class);
        // TODO avoid using repository manager mock. The manager choice depends on the region type
        LuceneFunction.setRepositoryManager(mockManager);
        try {
          Mockito.doReturn(repos).when(mockManager).getRepositories(any(Region.class));
        } catch (BucketNotFoundException e) {
          fail("", e);
        }

        try {
          Mockito.doAnswer(new Answer<Object>() {
            public Object answer(InvocationOnMock invocation) {
              Object[] args = invocation.getArguments();
              IndexResultCollector collector = (IndexResultCollector) args[2];
              collector.collect(cache.getDistributedSystem().getDistributedMember().getProcessId(), .1f);
              return null;
            }
          }).when(mockRepo).query(any(Query.class), Mockito.anyInt(), any(IndexResultCollector.class));
        } catch (IOException e) {
          fail("", e);
        }
      }
    };

    server1.invoke(createPartitionRegion);
    server2.invoke(createPartitionRegion);

    SerializableRunnable executeSearch = new SerializableRunnable("executeSearch") {
      private static final long serialVersionUID = 1L;

      public void run() {
        Cache cache = getCache();
        assertNotNull(cache);
        Region<Object, Object> region = cache.getRegion(REGION_NAME);
        Assert.assertNotNull(region);

        LuceneService service = LuceneServiceProvider.get(cache);
        LuceneIndex index = service.getIndex(INDEX_NAME, REGION_NAME);
        LuceneQueryProvider provider = new StringQueryProvider(index, "text:search");

        LuceneFunctionContext<TopEntriesCollector> context = new LuceneFunctionContext<>(provider,
            new TopEntriesCollectorManager());
        TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();

        FunctionService.onRegion(region).withArgs(context).withCollector(collector).execute(LuceneFunction.ID);
        TopEntries entries = collector.getResult();
        assertNotNull(entries);
        assertEquals(2, entries.getHits().size());
      }
    };

    server1.invoke(executeSearch);
  }
}
