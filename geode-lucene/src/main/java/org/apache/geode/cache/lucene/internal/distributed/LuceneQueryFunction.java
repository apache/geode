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

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.lucene.LuceneIndexNotFoundException;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.logging.LogService;

/**
 * {@link LuceneQueryFunction} coordinates text search on a member. It receives text search query
 * from the coordinator and arguments like region and buckets. It invokes search on the local index
 * and provides a result collector. The locally collected results are sent to the search
 * coordinator.
 */
public class LuceneQueryFunction implements Function, InternalEntity {
  private static final long serialVersionUID = 1L;
  public static final String ID = LuceneQueryFunction.class.getName();

  private static final Logger logger = LogService.getLogger();

  @Override
  public void execute(FunctionContext context) {
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

    LuceneIndexImpl index = getLuceneIndex(region, searchContext);
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
        repositories = repoManager.getRepositories(ctx);

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
    }
  }

  private LuceneIndexImpl getLuceneIndex(final Region region,
      final LuceneFunctionContext<IndexResultCollector> searchContext) {
    LuceneService service = LuceneServiceProvider.get(region.getCache());
    LuceneIndexImpl index = null;
    try {
      index =
          (LuceneIndexImpl) service.getIndex(searchContext.getIndexName(), region.getFullPath());
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
        index =
            (LuceneIndexImpl) service.getIndex(searchContext.getIndexName(), region.getFullPath());
      }
    } catch (CacheClosedException e) {
      throw new InternalFunctionInvocationTargetException(
          "Cache is closed when attempting to retrieve index:" + region.getFullPath(), e);
    }

    return index;
  }

  private Query getQuery(final LuceneQueryProvider queryProvider, final LuceneIndexImpl index) {
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

