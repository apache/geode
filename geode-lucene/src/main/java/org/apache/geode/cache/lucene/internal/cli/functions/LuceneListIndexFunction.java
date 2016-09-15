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

package org.apache.geode.cache.lucene.internal.cli.functions;

import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationProfile;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.internal.InternalEntity;

/**
 * The LuceneListIndexFunction class is a function used to collect the information on all lucene indexes in
 * the entire Cache.
 * </p>
 * @see Cache
 * @see org.apache.geode.cache.execute.Function
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
