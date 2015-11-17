/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.InternalLuceneIndex;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * {@link LuceneFunction} coordinates text search on a member. It receives text search query from the coordinator
 * and arguments like region and buckets. It invokes search on the local index and provides a result collector. The
 * locally collected results are sent to the search coordinator.
 */
public class LuceneFunction extends FunctionAdapter {
  private static final long serialVersionUID = 1L;
  public static final String ID = LuceneFunction.class.getName();

  private static final Logger logger = LogService.getLogger();

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext ctx = (RegionFunctionContext) context;
    ResultSender<TopEntriesCollector> resultSender = ctx.getResultSender();

    Region region = ctx.getDataSet();

    LuceneFunctionContext<IndexResultCollector> searchContext = (LuceneFunctionContext) ctx.getArguments();
    if (searchContext == null) {
      resultSender.sendException(new IllegalArgumentException("Missing search context"));
      return;
    }

    LuceneQueryProvider queryProvider = searchContext.getQueryProvider();
    if (queryProvider == null) {
      resultSender.sendException(new IllegalArgumentException("Missing query provider"));
      return;
    }
    
    LuceneService service = LuceneServiceProvider.get(region.getCache());
    InternalLuceneIndex index = (InternalLuceneIndex) service.getIndex(searchContext.getIndexName(), region.getFullPath());
    RepositoryManager repoManager = index.getRepositoryManager();

    Query query = null;
    try {
      query = queryProvider.getQuery(index);
    } catch (QueryException e) {
      resultSender.sendException(e);
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Executing lucene query: {}, on region {}", query, region.getFullPath());
    }

    int resultLimit = searchContext.getLimit();
    CollectorManager manager = (searchContext == null) ? null : searchContext.getCollectorManager();
    if (manager == null) {
      manager = new TopEntriesCollectorManager(null, resultLimit);
    }

    Collection<IndexResultCollector> results = new ArrayList<>();
    try {
      Collection<IndexRepository> repositories = repoManager.getRepositories(ctx);
      for (IndexRepository repo : repositories) {
        IndexResultCollector collector = manager.newCollector(repo.toString());
        logger.debug("Executing search on repo: " + repo.toString());
        repo.query(query, resultLimit, collector);
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


  @Override
  public String getId() {
    return ID;
  }
  
  @Override
  public boolean optimizeForWrite() {
    return true;
  }
}
