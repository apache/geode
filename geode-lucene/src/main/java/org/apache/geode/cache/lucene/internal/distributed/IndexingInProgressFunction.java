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
package org.apache.geode.cache.lucene.internal.distributed;


import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.internal.cache.execute.InternalFunction;

public class IndexingInProgressFunction implements InternalFunction<Object> {

  private static final long serialVersionUID = 1L;
  public static final String ID = IndexingInProgressFunction.class.getName();

  @Override
  public void execute(FunctionContext<Object> context) {
    RegionFunctionContext ctx = (RegionFunctionContext) context;
    ResultSender<Boolean> resultSender = ctx.getResultSender();

    Region region = ctx.getDataSet();
    Cache cache = region.getCache();
    String indexName = (String) ctx.getArguments();
    if (indexName == null) {
      throw new IllegalArgumentException("Missing index name");
    }
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    LuceneIndex luceneIndex = luceneService.getIndex(indexName, region.getFullPath());
    if (luceneIndex == null) {
      resultSender.lastResult(false);
    } else {
      resultSender.lastResult(luceneIndex.isIndexingInProgress());
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
