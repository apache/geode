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

package org.apache.geode.cache.lucene.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.LuceneResultStruct;
import org.apache.geode.cache.lucene.PageableLuceneQueryResults;
import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.cache.lucene.internal.distributed.LuceneFunctionContext;
import org.apache.geode.cache.lucene.internal.distributed.LuceneQueryFunction;
import org.apache.geode.cache.lucene.internal.distributed.TopEntries;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesCollector;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesCollectorManager;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesFunctionCollector;
import org.apache.geode.internal.logging.LogService;

public class LuceneQueryImpl<K, V> implements LuceneQuery<K, V> {
  public static final String LUCENE_QUERY_CANNOT_BE_EXECUTED_WITHIN_A_TRANSACTION =
      "Lucene Query cannot be executed within a transaction";
  Logger logger = LogService.getLogger();

  private int limit = LuceneQueryFactory.DEFAULT_LIMIT;
  private int pageSize = LuceneQueryFactory.DEFAULT_PAGESIZE;
  private String indexName;
  /* the lucene Query object to be wrapped here */
  private LuceneQueryProvider query;
  private Region<K, V> region;
  private String defaultField;

  public LuceneQueryImpl(String indexName, Region<K, V> region, LuceneQueryProvider provider,
      int limit, int pageSize) {
    this.indexName = indexName;
    this.region = region;
    this.limit = limit;
    this.pageSize = pageSize;
    this.query = provider;
  }

  @Override
  public Collection<K> findKeys() throws LuceneQueryException {
    TopEntries<K> entries = findTopEntries();
    final List<EntryScore<K>> hits = entries.getHits();

    return hits.stream().map(hit -> hit.getKey()).collect(Collectors.toList());
  }

  @Override
  public Collection<V> findValues() throws LuceneQueryException {
    final List<LuceneResultStruct<K, V>> page = findResults();

    return page.stream().map(entry -> entry.getValue()).collect(Collectors.toList());
  }

  @Override
  public List<LuceneResultStruct<K, V>> findResults() throws LuceneQueryException {
    PageableLuceneQueryResults<K, V> pages = findPages(0);
    if (!pages.hasNext()) {
      return Collections.emptyList();
    }

    return pages.next();
  }

  @Override
  public PageableLuceneQueryResults<K, V> findPages() throws LuceneQueryException {
    return findPages(pageSize);
  }

  private PageableLuceneQueryResults<K, V> findPages(int pageSize) throws LuceneQueryException {
    TopEntries<K> entries = findTopEntries();
    return newPageableResults(pageSize, entries);
  }

  protected PageableLuceneQueryResults<K, V> newPageableResults(final int pageSize,
      final TopEntries<K> entries) {
    return new PageableLuceneQueryResultsImpl<K, V>(entries.getHits(), region, pageSize);
  }

  private TopEntries<K> findTopEntries() throws LuceneQueryException {
    TopEntriesCollectorManager manager = new TopEntriesCollectorManager(null, limit);
    LuceneFunctionContext<TopEntriesCollector> context =
        new LuceneFunctionContext<>(query, indexName, manager, limit);
    if (region.getCache().getCacheTransactionManager().exists()) {
      throw new LuceneQueryException(LUCENE_QUERY_CANNOT_BE_EXECUTED_WITHIN_A_TRANSACTION);
    }

    // TODO provide a timeout to the user?
    TopEntries<K> entries = null;
    try {
      TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(context);
      ResultCollector<TopEntriesCollector, TopEntries<K>> rc =
          onRegion().setArguments(context).withCollector(collector).execute(LuceneQueryFunction.ID);
      entries = rc.getResult();
    } catch (FunctionException e) {
      if (e.getCause() instanceof LuceneQueryException) {
        throw (LuceneQueryException) e.getCause();
      } else if (e.getCause() instanceof TransactionException) {
        // When run from client with single hop disabled
        throw new LuceneQueryException(LUCENE_QUERY_CANNOT_BE_EXECUTED_WITHIN_A_TRANSACTION);
      } else if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw e;
    } catch (TransactionException e) {
      // When function execution is run from server
      throw new LuceneQueryException(LUCENE_QUERY_CANNOT_BE_EXECUTED_WITHIN_A_TRANSACTION);
    }

    return entries;
  }

  protected Execution onRegion() {
    return FunctionService.onRegion(region);
  }

  @Override
  public int getPageSize() {
    return this.pageSize;
  }

  @Override
  public int getLimit() {
    return this.limit;
  }

}
