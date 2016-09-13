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
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.internal.InternalEntity;

/**
 * The LuceneDescribeIndexFunction class is a function used to collect the information on a particular lucene index.
 * </p>
 * @see Cache
 * @see org.apache.geode.cache.execute.Function
 * @see FunctionAdapter
 * @see FunctionContext
 * @see InternalEntity
 * @see LuceneIndexDetails
 * @see LuceneIndexInfo
 */
@SuppressWarnings("unused")
public class LuceneDescribeIndexFunction extends FunctionAdapter implements InternalEntity {

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public String getId() {
    return LuceneDescribeIndexFunction.class.getName();
  }

  public void execute(final FunctionContext context) {
    LuceneIndexDetails result = null;

    final Cache cache = getCache();
    final LuceneIndexInfo indexInfo = (LuceneIndexInfo) context.getArguments();
    LuceneServiceImpl service = (LuceneServiceImpl) LuceneServiceProvider.get(cache);
    LuceneIndex index = service.getIndex(indexInfo.getIndexName(), indexInfo.getRegionPath());
    LuceneIndexCreationProfile profile = service.getDefinedIndex(indexInfo.getIndexName(),indexInfo.getRegionPath());
    if (index != null) {
      result = new LuceneIndexDetails((LuceneIndexImpl) index);
    } else if (profile != null) {
     result = new LuceneIndexDetails(profile);
    }
    context.getResultSender().lastResult(result);
  }
}
