package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.internal.LuceneQueryResultsImpl;
import com.gemstone.gemfire.cache.lucene.internal.LuceneResultStructImpl;
import com.gemstone.gemfire.cache.lucene.internal.mergers.TopDocsResultMerger;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * {@link LuceneQueryFunction} coordinates text search on a member. It receives text search query from the coordinator
 * and arguments like region and buckets. It invokes search on the local index and provides a result collector. The
 * locally collected results are sent to the search coordinator.
 */
public class LuceneQueryFunction extends FunctionAdapter {
  private static final long serialVersionUID = 1L;
  public static final String ID = LuceneQueryFunction.class.getName();

  private static final Logger logger = LogService.getLogger();

  private RepositoryManager repoManager;

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext ctx = (RegionFunctionContext) context;
    ResultSender<LuceneQueryResults> resultSender = ctx.getResultSender();

    Region region = ctx.getDataSet();
    if (logger.isDebugEnabled()) {
      logger.debug("Executing lucene query on region:" + region.getFullPath());
    }

    LuceneSearchFunctionArgs args = (LuceneSearchFunctionArgs) ctx.getArguments();
    Set<Integer> buckets = args == null ? null : args.getBuckets();

    List<LuceneQueryResults> results = new ArrayList<>();
    try {
      Collection<IndexRepository> repositories = repoManager.getRepositories(region, buckets);
      for (IndexRepository repo : repositories) {
        ShardResultCollector collector = new ShardResultCollector();
        logger.debug("Executing search on repo: " + repo);
        repo.query(null, 0, collector);
        results.add(collector.getResult());
      }
    } catch (IOException e) {
      logger.warn("", e);
      resultSender.sendException(e);
      return;
    } catch (BucketNotFoundException e) {
      logger.warn("", e);
      resultSender.sendException(e);
      return;
    }

    TopDocsResultMerger merger = new TopDocsResultMerger();
    LuceneQueryResults merged = merger.mergeResults(results);
    resultSender.lastResult(merged);
  }

  /**
   * Collects and merges results from {@link IndexRepository}s
   */
  class ShardResultCollector implements IndexResultCollector {
    LuceneQueryResultsImpl result = new LuceneQueryResultsImpl();
    
    @Override
    public void collect(Object key, float score) {
      result.addHit(new LuceneResultStructImpl(key, score));
    }

    public LuceneQueryResults getResult() {
      return result;
    }
  }

  void setRepositoryManager(RepositoryManager manager) {
    this.repoManager = manager;
  }

  @Override
  public String getId() {
    return ID;
  }
}
