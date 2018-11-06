/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneIndexNotFoundException;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.InternalLuceneIndex;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationInProgressException;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionResultSender;
import org.apache.geode.internal.logging.LogService;

/**
 * {@link LuceneQueryFunction} coordinates text search on a member. It receives text search query
 * from the coordinator and arguments like region and buckets. It invokes search on the local index
 * and provides a result collector. The locally collected results are sent to the search
 * coordinator.
 */
public class LuceneQueryFunction implements InternalFunction<LuceneFunctionContext> {
  private static final long serialVersionUID = 1L;
  public static final String ID = LuceneQueryFunction.class.getName();

  private static final Logger logger = LogService.getLogger();

  @Override
  public void execute(FunctionContext<LuceneFunctionContext> context) {
    if (context.getResultSender() instanceof PartitionedRegionFunctionResultSender) {
      PartitionedRegionFunctionResultSender resultSender =
          (PartitionedRegionFunctionResultSender) context.getResultSender();
      Version clientVersion = resultSender.getClientVersion();
      if (clientVersion != null) { // is a client server connection
        if (clientVersion.ordinal() < Version.GEODE_160.ordinal()) {
          execute(context, true);
          return;
        }
      }
    }
    execute(context, false);
  }

  private void handleIfRetryNeededOnException(LuceneIndexCreationInProgressException ex,
      RegionFunctionContext ctx) {
    PartitionedRegion userDataRegion = (PartitionedRegion) ctx.getDataSet();

    // get the remote members
    Set<InternalDistributedMember> remoteMembers =
        userDataRegion.getRegionAdvisor().adviseAllPRNodes();
    // Old members with version numbers 1.6 or lower cannot handle IndexCreationInProgressException
    // Hence the query waits for the repositories to be ready instead of throwing the exception
    if (!remoteMembers.isEmpty()) {
      for (InternalDistributedMember remoteMember : remoteMembers) {
        if (remoteMember.getVersionObject().ordinal() < Version.GEODE_160.ordinal()) {
          // re-execute but wait till indexing is complete
          execute(ctx, true);
          return;
        }
      }
    }
    // Cannot send IndexingInProgressException as the function may have been executed a new server
    // on behalf of an old version client.
    throw new FunctionException(new LuceneQueryException(
        "Lucene Index is not available, currently indexing"));
  }

  public void execute(FunctionContext<LuceneFunctionContext> context, boolean waitForRepository) {
    RegionFunctionContext ctx = (RegionFunctionContext) context;
    ResultSender<TopEntriesCollector> resultSender = ctx.getResultSender();

    Region region = ctx.getDataSet();

    LuceneFunctionContext<IndexResultCollector> searchContext =
        (LuceneFunctionContext) ctx.getArguments();
    if (searchContext == null) {
      throw new IllegalArgumentException("Missing search context");
    }

    LuceneQueryProvider queryProvider = searchContext.getQueryProvider();
    if (queryProvider == null) {
      throw new IllegalArgumentException("Missing query provider");
    }

    InternalLuceneIndex index = getLuceneIndex(region, searchContext);
    if (index == null) {
      throw new LuceneIndexNotFoundException(searchContext.getIndexName(), region.getFullPath());
    }
    RepositoryManager repoManager = index.getRepositoryManager();
    LuceneIndexStats stats = index.getIndexStats();

    Query query = getQuery(queryProvider, index);

    if (logger.isDebugEnabled()) {
      logger.debug("Executing lucene query: {}, on region {}", query, region.getFullPath());
    }

    int resultLimit = searchContext.getLimit();
    CollectorManager manager = (searchContext == null) ? null : searchContext.getCollectorManager();
    if (manager == null) {
      manager = new TopEntriesCollectorManager(null, resultLimit);
    }

    Collection<IndexResultCollector> results = new ArrayList<>();
    TopEntriesCollector mergedResult = null;
    try {
      long start = stats.startQuery();
      Collection<IndexRepository> repositories = null;

      try {
        repositories = repoManager.getRepositories(ctx, waitForRepository);

        for (IndexRepository repo : repositories) {
          IndexResultCollector collector = manager.newCollector(repo.toString());
          if (logger.isDebugEnabled()) {
            logger.debug("Executing search on repo: " + repo.toString());
          }
          repo.query(query, resultLimit, collector);
          results.add(collector);
        }
        mergedResult = (TopEntriesCollector) manager.reduce(results);
      } finally {
        stats.endQuery(start, mergedResult == null ? 0 : mergedResult.size());
      }
      stats.incNumberOfQueryExecuted();
      resultSender.lastResult(mergedResult);
    } catch (IOException | BucketNotFoundException | CacheClosedException
        | PrimaryBucketException e) {
      logger.debug("Exception during lucene query function", e);
      throw new InternalFunctionInvocationTargetException(e);
    } catch (LuceneIndexCreationInProgressException ex) {
      if (!waitForRepository) {
        handleIfRetryNeededOnException(ex, ctx);
      } else {
        logger.warn("The lucene query should have waited for the index to be created");
        throw new FunctionException(new LuceneQueryException(
            "Lucene Index is not available, currently indexing"));
      }
    }
  }

  private InternalLuceneIndex getLuceneIndex(final Region region,
      final LuceneFunctionContext<IndexResultCollector> searchContext) {
    LuceneService service = LuceneServiceProvider.get(region.getCache());
    InternalLuceneIndex index = null;
    try {
      index = (InternalLuceneIndex) service.getIndex(searchContext.getIndexName(),
          region.getFullPath());
      if (index == null) {
        while (service instanceof LuceneServiceImpl && (((LuceneServiceImpl) service)
            .getDefinedIndex(searchContext.getIndexName(), region.getFullPath()) != null)) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            return null;
          }
          region.getCache().getCancelCriterion().checkCancelInProgress(null);
        }
        index = (InternalLuceneIndex) service.getIndex(searchContext.getIndexName(),
            region.getFullPath());
      }
    } catch (CacheClosedException e) {
      throw new InternalFunctionInvocationTargetException(
          "Cache is closed when attempting to retrieve index:" + region.getFullPath(), e);
    }

    return index;
  }

  private Query getQuery(final LuceneQueryProvider queryProvider, final LuceneIndex index) {
    Query query = null;
    try {
      query = queryProvider.getQuery(index);
    } catch (LuceneQueryException e) {
      logger.warn("", e);
      throw new FunctionException(e);
    }
    return query;
  }


  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }
}
