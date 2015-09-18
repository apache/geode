package com.gemstone.gemfire.cache.lucene.internal.distributed;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.Query;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.internal.StringQueryProvider;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneFunctionJUnitTest {
  Mockery mocker;

  final EntryScore r1_1 = new EntryScore("key-1-1", .5f);
  final EntryScore r1_2 = new EntryScore("key-1-2", .4f);
  final EntryScore r1_3 = new EntryScore("key-1-3", .3f);
  final EntryScore r2_1 = new EntryScore("key-2-1", .45f);
  final EntryScore r2_2 = new EntryScore("key-2-2", .35f);

  RegionFunctionContext mockContext;
  ResultSender<TopEntriesCollector> mockResultSender;
  Region<Object, Object> mockRegion;

  RepositoryManager mockRepoManager;
  IndexRepository mockRepository1;
  IndexRepository mockRepository2;
  IndexResultCollector mockCollector;

  ArrayList<IndexRepository> repos;
  LuceneFunctionContext searchArgs;
  LuceneQueryProvider queryProvider;
  Query query;

  @Test
  public void testRepoQueryAndMerge() throws Exception {
    final AtomicReference<TopEntriesCollector> result = new AtomicReference<>();
    mocker.checking(new Expectations() {
      {
        oneOf(mockContext).getDataSet();
        will(returnValue(mockRegion));
        oneOf(mockContext).getArguments();
        will(returnValue(searchArgs));

        oneOf(mockRepoManager).getRepositories(mockRegion, mockContext);
        will(returnValue(repos));

        oneOf(mockContext).getResultSender();
        will(returnValue(mockResultSender));

        oneOf(mockRepository1).query(with(query), with(equal(0)), with(any(IndexResultCollector.class)));
        will(new CustomAction("streamSearchResults") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            IndexResultCollector collector = (IndexResultCollector) invocation.getParameter(2);
            collector.collect(r1_1.key, r1_1.score);
            collector.collect(r1_2.key, r1_2.score);
            collector.collect(r1_3.key, r1_3.score);
            return null;
          }
        });

        oneOf(mockRepository2).query(with(query), with(equal(0)), with(any(IndexResultCollector.class)));
        will(new CustomAction("streamSearchResults") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            IndexResultCollector collector = (IndexResultCollector) invocation.getParameter(2);
            collector.collect(r2_1.key, r2_1.score);
            collector.collect(r2_2.key, r2_2.score);
            return null;
          }
        });

        oneOf(mockResultSender).lastResult(with(any(TopEntriesCollector.class)));
        will(new CustomAction("collectResult") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            result.set((TopEntriesCollector) invocation.getParameter(0));
            return null;
          }
        });
      }
    });

    LuceneFunction function = new LuceneFunction();
    function.setRepositoryManager(mockRepoManager);

    function.execute(mockContext);
    List<EntryScore> hits = result.get().getEntries().getHits();
    assertEquals(5, hits.size());
    TopEntriesJUnitTest.verifyResultOrder(result.get().getEntries().getHits(), r1_1, r2_1, r1_2, r2_2, r1_3);
  }

  @Test
  public void testResultLimitClause() throws Exception {
    final AtomicReference<TopEntriesCollector> result = new AtomicReference<>();

    searchArgs = new LuceneFunctionContext<IndexResultCollector>(queryProvider, null, 3);

    mocker.checking(new Expectations() {
      {
        oneOf(mockContext).getDataSet();
        will(returnValue(mockRegion));
        oneOf(mockContext).getArguments();
        will(returnValue(searchArgs));

        oneOf(mockContext).getResultSender();
        will(returnValue(mockResultSender));

        oneOf(mockRepoManager).getRepositories(mockRegion, mockContext);
        will(returnValue(repos));

        oneOf(mockRepository1).query(with(query), with(equal(0)), with(any(IndexResultCollector.class)));
        will(new CustomAction("streamSearchResults") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            IndexResultCollector collector = (IndexResultCollector) invocation.getParameter(2);
            collector.collect(r1_1.key, r1_1.score);
            collector.collect(r1_2.key, r1_2.score);
            collector.collect(r1_3.key, r1_3.score);
            return null;
          }
        });

        oneOf(mockRepository2).query(with(query), with(equal(0)), with(any(IndexResultCollector.class)));
        will(new CustomAction("streamSearchResults") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            IndexResultCollector collector = (IndexResultCollector) invocation.getParameter(2);
            collector.collect(r2_1.key, r2_1.score);
            collector.collect(r2_2.key, r2_2.score);
            return null;
          }
        });

        oneOf(mockResultSender).lastResult(with(any(TopEntriesCollector.class)));
        will(new CustomAction("collectResult") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            result.set((TopEntriesCollector) invocation.getParameter(0));
            return null;
          }
        });
      }
    });

    LuceneFunction function = new LuceneFunction();
    function.setRepositoryManager(mockRepoManager);

    function.execute(mockContext);
    List<EntryScore> hits = result.get().getEntries().getHits();
    assertEquals(3, hits.size());
    TopEntriesJUnitTest.verifyResultOrder(result.get().getEntries().getHits(), r1_1, r2_1, r1_2);
  }

  @Test
  public void injectCustomCollectorManager() throws Exception {
    final CollectorManager mockManager = mocker.mock(CollectorManager.class);
    searchArgs = new LuceneFunctionContext<IndexResultCollector>(queryProvider, mockManager);
    mocker.checking(new Expectations() {
      {
        oneOf(mockContext).getDataSet();
        will(returnValue(mockRegion));
        oneOf(mockContext).getArguments();
        will(returnValue(searchArgs));
        oneOf(mockContext).getResultSender();
        will(returnValue(mockResultSender));

        oneOf(mockRepoManager).getRepositories(mockRegion, mockContext);
        repos.remove(0);
        will(returnValue(repos));

        oneOf(mockManager).newCollector("repo2");
        will(returnValue(mockCollector));
        oneOf(mockManager).reduce(with(any(Collection.class)));
        will(new CustomAction("reduce") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            Collection<IndexResultCollector> collectors = (Collection<IndexResultCollector>) invocation.getParameter(0);
            assertEquals(1, collectors.size());
            assertEquals(mockCollector, collectors.iterator().next());
            return new TopEntriesCollector(null);
          }
        });

        oneOf(mockCollector).collect("key-2-1", .45f);

        oneOf(mockRepository2).query(with(query), with(equal(0)), with(any(IndexResultCollector.class)));
        will(new CustomAction("streamSearchResults") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            IndexResultCollector collector = (IndexResultCollector) invocation.getParameter(2);
            collector.collect(r2_1.key, r2_1.score);
            return null;
          }
        });

        oneOf(mockResultSender).lastResult(with(any(TopEntriesCollector.class)));
      }
    });

    LuceneFunction function = new LuceneFunction();
    function.setRepositoryManager(mockRepoManager);

    function.execute(mockContext);
  }

  @Test
  public void testIndexRepoQueryFails() throws Exception {
    mocker.checking(new Expectations() {
      {
        oneOf(mockContext).getDataSet();
        will(returnValue(mockRegion));
        oneOf(mockContext).getArguments();
        will(returnValue(searchArgs));

        oneOf(mockRepoManager).getRepositories(mockRegion, mockContext);
        will(returnValue(repos));

        oneOf(mockContext).getResultSender();
        will(returnValue(mockResultSender));
        oneOf(mockResultSender).sendException(with(any(IOException.class)));

        oneOf(mockRepository1).query(with(query), with(equal(0)), with(any(IndexResultCollector.class)));
        will(throwException(new IOException()));
      }
    });

    LuceneFunction function = new LuceneFunction();
    function.setRepositoryManager(mockRepoManager);

    function.execute(mockContext);
  }

  @Test
  public void testBucketNotFound() throws Exception {
    mocker.checking(new Expectations() {
      {
        oneOf(mockContext).getDataSet();
        will(returnValue(mockRegion));
        oneOf(mockContext).getArguments();
        will(returnValue(searchArgs));

        oneOf(mockRepoManager).getRepositories(mockRegion, mockContext);
        will(throwException(new BucketNotFoundException("")));

        oneOf(mockContext).getResultSender();
        will(returnValue(mockResultSender));
        oneOf(mockResultSender).sendException(with(any(BucketNotFoundException.class)));
      }
    });

    LuceneFunction function = new LuceneFunction();
    function.setRepositoryManager(mockRepoManager);

    function.execute(mockContext);
  }

  @Test
  public void testReduceError() throws Exception {
    final CollectorManager mockManager = mocker.mock(CollectorManager.class);
    searchArgs = new LuceneFunctionContext<IndexResultCollector>(queryProvider, mockManager);
    mocker.checking(new Expectations() {
      {
        oneOf(mockContext).getDataSet();
        will(returnValue(mockRegion));
        oneOf(mockContext).getResultSender();
        will(returnValue(mockResultSender));
        oneOf(mockContext).getArguments();
        will(returnValue(searchArgs));

        oneOf(mockManager).newCollector("repo1");
        will(returnValue(mockCollector));
        oneOf(mockManager).reduce(with(any(Collection.class)));
        will(throwException(new IOException()));

        oneOf(mockRepoManager).getRepositories(mockRegion, mockContext);
        repos.remove(1);
        will(returnValue(repos));

        oneOf(mockRepository1).query(query, 0, mockCollector);
        oneOf(mockResultSender).sendException(with(any(IOException.class)));
      }
    });

    LuceneFunction function = new LuceneFunction();
    function.setRepositoryManager(mockRepoManager);

    function.execute(mockContext);
  }

  @Test
  public void queryProviderErrorIsHandled() throws Exception {
    queryProvider = mocker.mock(LuceneQueryProvider.class);
    searchArgs = new LuceneFunctionContext<IndexResultCollector>(queryProvider, null);
    mocker.checking(new Expectations() {
      {
        oneOf(mockContext).getDataSet();
        will(returnValue(mockRegion));
        oneOf(mockContext).getResultSender();
        will(returnValue(mockResultSender));
        oneOf(mockContext).getArguments();
        will(returnValue(searchArgs));
        
        oneOf(queryProvider).getQuery();
        will(throwException(new QueryException()));
        
        oneOf(mockResultSender).sendException(with(any(QueryException.class)));
      }
    });
    
    LuceneFunction function = new LuceneFunction();
    function.setRepositoryManager(mockRepoManager);
    
    function.execute(mockContext);
  }
  
  @Test
  public void testQueryFunctionId() {
    String id = new LuceneFunction().getId();
    assertEquals(LuceneFunction.class.getName(), id);
  }

  @Test
  public void testLuceneFunctionArgsDefaults() {
    LuceneFunctionContext context = new LuceneFunctionContext(null);
    assertEquals(LuceneQueryFactory.DEFAULT_LIMIT, context.getLimit());
  }

  @Before
  public void createMocksAndCommonObjects() throws Exception {
    mocker = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };

    mockContext = mocker.mock(RegionFunctionContext.class);
    mockResultSender = mocker.mock(ResultSender.class);
    mockRegion = mocker.mock(Region.class);

    mockRepoManager = mocker.mock(RepositoryManager.class);
    mockRepository1 = mocker.mock(IndexRepository.class, "repo1");
    mockRepository2 = mocker.mock(IndexRepository.class, "repo2");
    mockCollector = mocker.mock(IndexResultCollector.class);

    repos = new ArrayList<IndexRepository>();
    repos.add(mockRepository1);
    repos.add(mockRepository2);

    queryProvider = new StringQueryProvider("gemfire:lucene");
    query = queryProvider.getQuery();
    searchArgs = new LuceneFunctionContext<IndexResultCollector>(queryProvider);
  }

  @After
  public void validateMock() {
    mocker.assertIsSatisfied();
    mocker = null;
  }
}
