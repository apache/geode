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

package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.cli.domain.IndexDetails;

/**
 * The ListIndexFunction class is a GemFire function used to collect all the index information on all Regions across
 * the entire GemFire Cache (distributed system).
 * </p>
 * @see com.gemstone.gemfire.cache.Cache
 * @see com.gemstone.gemfire.cache.execute.Function
 * @see com.gemstone.gemfire.cache.execute.FunctionAdapter
 * @see com.gemstone.gemfire.cache.execute.FunctionContext
 * @see com.gemstone.gemfire.internal.InternalEntity
 * @see com.gemstone.gemfire.management.internal.cli.domain.IndexDetails
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class ListIndexFunction extends FunctionAdapter implements InternalEntity {

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public String getId() {
    return ListIndexFunction.class.getName();
  }

  public void execute(final FunctionContext context) {
    try {
      final Set<IndexDetails> indexDetailsSet = new HashSet<IndexDetails>();

      final Cache cache = getCache();
      final DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      for (final Index index : cache.getQueryService().getIndexes()) {
        indexDetailsSet.add(new IndexDetails(member, index));
      }

      context.getResultSender().lastResult(indexDetailsSet);
    }
    catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }

}
