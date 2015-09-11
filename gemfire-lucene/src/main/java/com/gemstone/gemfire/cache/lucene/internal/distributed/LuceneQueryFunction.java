package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
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
    ResultSender<TopEntriesCollector> resultSender = ctx.getResultSender();

    Region region = ctx.getDataSet();
    if (logger.isDebugEnabled()) {
      logger.debug("Executing lucene query on region:" + region.getFullPath());
    }

    LuceneSearchFunctionArgs args = (LuceneSearchFunctionArgs) ctx.getArguments();

    CollectorManager manager = (args == null) ? null : args.getCollectorManager();
    if (manager == null) {
      int resultLimit = (args == null ? LuceneQueryFactory.DEFAULT_LIMIT : args.getLimit());
      manager = new TopEntriesCollectorManager(null, resultLimit);
    }

    Collection<IndexResultCollector> results = new ArrayList<>();
    try {
      Collection<IndexRepository> repositories = repoManager.getRepositories(region, ctx);
      for (IndexRepository repo : repositories) {
        IndexResultCollector collector = manager.newCollector(repo.toString());
        logger.debug("Executing search on repo: " + repo.toString());
        repo.query(null, 0, collector);
        results.add(collector);
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

    TopEntriesCollector mergedResult;
    try {
      mergedResult = (TopEntriesCollector) manager.reduce(results);
      resultSender.lastResult(mergedResult);
    } catch (IOException e) {
      logger.warn("", e);
      resultSender.sendException(e);
      return;
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
