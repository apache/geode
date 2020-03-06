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

package org.apache.geode.management.internal.cli.functions;

import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.Index;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.management.internal.cli.domain.IndexDetails;

/**
 * The ListIndexFunction class is a GemFire function used to collect all the index information on
 * all Regions across the entire GemFire Cache (distributed system).
 * </p>
 *
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.execute.Function
 * @see org.apache.geode.cache.execute.FunctionContext
 * @see org.apache.geode.internal.InternalEntity
 * @see org.apache.geode.management.internal.cli.domain.IndexDetails
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class ListIndexFunction implements InternalFunction<Void> {

  @Override
  public String getId() {
    return ListIndexFunction.class.getName();
  }

  @Override
  public void execute(final FunctionContext<Void> context) {
    try {
      final Set<IndexDetails> indexDetailsSet = new HashSet<>();

      final Cache cache = context.getCache();
      final DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      for (final Index index : cache.getQueryService().getIndexes()) {
        indexDetailsSet.add(new IndexDetails(member, index));
      }

      context.getResultSender().lastResult(indexDetailsSet);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }
}
