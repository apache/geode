/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.cli.functions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneResultStruct;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.PageableLuceneQueryResults;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.cache.lucene.internal.cli.LuceneQueryInfo;
import org.apache.geode.cache.lucene.internal.cli.LuceneSearchResults;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.internal.InternalEntity;

/**
 * The LuceneSearchIndexFunction class is a function used to collect the information on a particular
 * lucene index.
 * </p>
 * 
 * @see Cache
 * @see org.apache.geode.cache.execute.Function
 * @see FunctionAdapter
 * @see FunctionContext
 * @see InternalEntity
 * @see LuceneIndexDetails
 * @see LuceneIndexInfo
 */
@SuppressWarnings("unused")
public class LuceneSearchIndexFunction<K, V> extends FunctionAdapter implements InternalEntity {

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public String getId() {
    return LuceneSearchIndexFunction.class.getName();
  }

  public void execute(final FunctionContext context) {
    Set<LuceneSearchResults> result = new HashSet<>();
    final Cache cache = getCache();
    final LuceneQueryInfo queryInfo = (LuceneQueryInfo) context.getArguments();

    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    try {
      if (luceneService.getIndex(queryInfo.getIndexName(), queryInfo.getRegionPath()) == null) {
        throw new Exception("Index " + queryInfo.getIndexName() + " not found on region "
            + queryInfo.getRegionPath());
      }
      final LuceneQuery<K, V> query = luceneService.createLuceneQueryFactory()
          .setResultLimit(queryInfo.getLimit()).create(queryInfo.getIndexName(),
              queryInfo.getRegionPath(), queryInfo.getQueryString(), queryInfo.getDefaultField());
      if (queryInfo.getKeysOnly()) {
        query.findKeys().forEach(key -> result.add(new LuceneSearchResults(key.toString())));
      } else {
        PageableLuceneQueryResults pageableLuceneQueryResults = query.findPages();
        while (pageableLuceneQueryResults.hasNext()) {
          List<LuceneResultStruct> page = pageableLuceneQueryResults.next();
          page.stream()
              .forEach(searchResult -> result
                  .add(new LuceneSearchResults<K, V>(searchResult.getKey().toString(),
                      searchResult.getValue().toString(), searchResult.getScore())));
        }
      }
      if (result != null) {
        context.getResultSender().lastResult(result);
      }
    } catch (LuceneQueryException e) {
      result.add(new LuceneSearchResults(true, e.getRootCause().getMessage()));
      context.getResultSender().lastResult(result);
    } catch (Exception e) {
      result.add(new LuceneSearchResults(true, e.getMessage()));
      context.getResultSender().lastResult(result);
    }
  }
}

