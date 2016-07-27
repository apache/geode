/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.cli.functions;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexCreationProfile;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexImpl;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.cache.lucene.internal.cli.LuceneIndexDetails;
import com.gemstone.gemfire.internal.InternalEntity;

/**
 * The LuceneListIndexFunction class is a function used to collect the information on all lucene indexes in
 * the entire Cache.
 * </p>
 * @see Cache
 * @see com.gemstone.gemfire.cache.execute.Function
 * @see FunctionAdapter
 * @see FunctionContext
 * @see InternalEntity
 * @see LuceneIndexDetails
 */
@SuppressWarnings("unused")
public class LuceneListIndexFunction extends FunctionAdapter implements InternalEntity {

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public String getId() {
    return LuceneListIndexFunction.class.getName();
  }

  public void execute(final FunctionContext context) {
    final Set<LuceneIndexDetails> indexDetailsSet = new HashSet<>();
    final Cache cache = getCache();
    LuceneServiceImpl service = (LuceneServiceImpl) LuceneServiceProvider.get(cache);
    for (LuceneIndex index : service.getAllIndexes()) {
      indexDetailsSet.add(new LuceneIndexDetails((LuceneIndexImpl) index));
    }

    for(LuceneIndexCreationProfile profile : service.getAllDefinedIndexes()) {
      indexDetailsSet.add(new LuceneIndexDetails(profile));
    }
    context.getResultSender().lastResult(indexDetailsSet);
  }
}
