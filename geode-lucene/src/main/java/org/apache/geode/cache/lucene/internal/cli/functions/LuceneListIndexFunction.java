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

import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationProfile;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexStatus;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.execute.InternalFunction;

/**
 * The LuceneListIndexFunction class is a function used to collect the information on all lucene
 * indexes in the entire Cache.
 * </p>
 *
 * @see Cache
 * @see org.apache.geode.cache.execute.Function
 * @see Function
 * @see FunctionContext
 * @see InternalEntity
 * @see LuceneIndexDetails
 */
@SuppressWarnings("unused")
public class LuceneListIndexFunction implements InternalFunction {

  private static final long serialVersionUID = -2320432506763893879L;

  @Override
  public String getId() {
    return LuceneListIndexFunction.class.getName();
  }

  @Override
  public void execute(final FunctionContext context) {
    final Set<LuceneIndexDetails> indexDetailsSet = new HashSet<>();
    final Cache cache = context.getCache();
    final String serverName = cache.getDistributedSystem().getDistributedMember().getName();
    LuceneServiceImpl service = (LuceneServiceImpl) LuceneServiceProvider.get(cache);
    for (LuceneIndexCreationProfile profile : service.getAllDefinedIndexes()) {
      indexDetailsSet
          .add(new LuceneIndexDetails(profile, serverName, LuceneIndexStatus.NOT_INITIALIZED));
    }
    for (LuceneIndex index : service.getAllIndexes()) {
      LuceneIndexStatus initialized;
      if (index.isIndexingInProgress()) {
        initialized = LuceneIndexStatus.INDEXING_IN_PROGRESS;
      } else {
        initialized = LuceneIndexStatus.INITIALIZED;
      }
      indexDetailsSet.add(new LuceneIndexDetails((LuceneIndexImpl) index, serverName, initialized));
    }
    context.getResultSender().lastResult(indexDetailsSet);
  }
}
