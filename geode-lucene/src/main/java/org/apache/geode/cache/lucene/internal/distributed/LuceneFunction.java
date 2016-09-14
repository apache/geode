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

package org.apache.geode.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.InternalLuceneIndex;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.logging.LogService;

/**
 * {@link LuceneFunction} coordinates text search on a member. It receives text search query from the coordinator
 * and arguments like region and buckets. It invokes search on the local index and provides a result collector. The
 * locally collected results are sent to the search coordinator.
 */
public class LuceneFunction extends FunctionAdapter implements InternalEntity {
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
      throw new IllegalArgumentException("Missing search context");
    }

    LuceneQueryProvider queryProvider = searchContext.getQueryProvider();
    if (queryProvider == null) {
      throw new IllegalArgumentException("Missing query provider");
    }
    
    LuceneService service = LuceneServiceProvider.get(region.getCache());
    InternalLuceneIndex index = (InternalLuceneIndex) service.getIndex(searchContext.getIndexName(), region.getFullPath());
    RepositoryManager repoManager = index.getRepositoryManager();

    Query query = null;
    try {
      query = queryProvider.getQuery(index);
    } catch (LuceneQueryException e) {
      logger.warn("", e);
      throw new FunctionException(e);
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
      TopEntriesCollector mergedResult = (TopEntriesCollector) manager.reduce(results);
      resultSender.lastResult(mergedResult);
    } catch (IOException|BucketNotFoundException e) {
      logger.warn("", e);
      throw new FunctionException(e);
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
