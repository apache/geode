package com.gemstone.gemfire.cache.lucene.internal.distributed;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneQueryFunctionJUnitTest {
  Mockery mocker;

  @Test
  public void testRepoQueryAndMerge() throws Exception {
    final EntryScore r1_1 = new EntryScore("key-1-1", .5f);
    final EntryScore r1_2 = new EntryScore("key-1-2", .4f);
    final EntryScore r1_3 = new EntryScore("key-1-3", .3f);
    final EntryScore r2_1 = new EntryScore("key-2-1", .45f);
    final EntryScore r2_2 = new EntryScore("key-2-2", .35f);

    final AtomicReference<TopEntries> result = new AtomicReference<>();

    final QueryMocks m = new QueryMocks();
    mocker.checking(new Expectations() {
      {
        oneOf(m.mockContext).getDataSet();
        will(returnValue(m.mockRegion));
        oneOf(m.mockContext).getArguments();
        will(returnValue(null));

        oneOf(m.mockRepoManager).getRepositories(m.mockRegion, null);
        will(returnValue(m.repos));

        oneOf(m.mockContext).getResultSender();
        will(returnValue(m.mockResultSender));

        oneOf(m.mockRepository1).query(with(aNull(Query.class)), with(equal(0)), with(any(IndexResultCollector.class)));
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
        
        oneOf(m.mockRepository2).query(with(aNull(Query.class)), with(equal(0)), with(any(IndexResultCollector.class)));
        will(new CustomAction("streamSearchResults") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            IndexResultCollector collector = (IndexResultCollector) invocation.getParameter(2);
            collector.collect(r2_1.key, r2_1.score);
            collector.collect(r2_2.key, r2_2.score);
            return null;
          }
        });

        oneOf(m.mockResultSender).lastResult(with(any(TopEntries.class)));
        will(new CustomAction("collectResult") {
          @Override
          public Object invoke(Invocation invocation) throws Throwable {
            result.set((TopEntries) invocation.getParameter(0));
            return null;
          }
        });
      }
    });

    LuceneQueryFunction function = new LuceneQueryFunction();
    function.setRepositoryManager(m.mockRepoManager);

    function.execute(m.mockContext);
    List<EntryScore> hits = result.get().getHits();
    assertEquals(5, hits.size());
    TopEntriesJUnitTest.verifyResultOrder(result.get().getHits(), r1_1, r2_1, r1_2, r2_2, r1_3);
  }

  @Test
  public void testIndexRepoQueryFails() throws Exception {
    final QueryMocks m = new QueryMocks();
    mocker.checking(new Expectations() {
      {
        oneOf(m.mockContext).getDataSet();
        will(returnValue(m.mockRegion));
        oneOf(m.mockContext).getArguments();
        will(returnValue(null));

        oneOf(m.mockRepoManager).getRepositories(m.mockRegion, null);
        will(returnValue(m.repos));

        oneOf(m.mockContext).getResultSender();
        will(returnValue(m.mockResultSender));
        oneOf(m.mockResultSender).sendException(with(any(IOException.class)));

        oneOf(m.mockRepository1).query(with(aNull(Query.class)), with(equal(0)), with(any(IndexResultCollector.class)));
        will(throwException(new IOException()));
      }
    });

    LuceneQueryFunction function = new LuceneQueryFunction();
    function.setRepositoryManager(m.mockRepoManager);

    function.execute(m.mockContext);
  }

  @Test
  public void testBucketNotFound() throws Exception {
    final QueryMocks m = new QueryMocks();
    mocker.checking(new Expectations() {
      {
        oneOf(m.mockContext).getDataSet();
        will(returnValue(m.mockRegion));
        oneOf(m.mockContext).getArguments();
        will(returnValue(null));

        oneOf(m.mockRepoManager).getRepositories(m.mockRegion, null);
        will(throwException(new BucketNotFoundException("")));

        oneOf(m.mockContext).getResultSender();
        will(returnValue(m.mockResultSender));
        oneOf(m.mockResultSender).sendException(with(any(BucketNotFoundException.class)));
      }
    });

    LuceneQueryFunction function = new LuceneQueryFunction();
    function.setRepositoryManager(m.mockRepoManager);

    function.execute(m.mockContext);
  }

  @Test
  public void testQueryFunctionId() {
    String id = new LuceneQueryFunction().getId();
    assertEquals(LuceneQueryFunction.class.getName(), id);
  }

  class QueryMocks {
    RegionFunctionContext mockContext = mocker.mock(RegionFunctionContext.class);
    ResultSender<TopEntries> mockResultSender = mocker.mock(ResultSender.class);
    Region<Object, Object> mockRegion = mocker.mock(Region.class);

    RepositoryManager mockRepoManager = mocker.mock(RepositoryManager.class);
    ArrayList<IndexRepository> repos = new ArrayList<IndexRepository>();
    IndexRepository mockRepository1 = mocker.mock(IndexRepository.class, "repo1");
    IndexRepository mockRepository2 = mocker.mock(IndexRepository.class, "repo2");

    QueryMocks() {
      repos.add(mockRepository1);
      repos.add(mockRepository2);
    }
  }

  @Before
  public void setupMock() {
    mocker = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };
  }

  @After
  public void validateMock() {
    mocker.assertIsSatisfied();
    mocker = null;
  }
}
