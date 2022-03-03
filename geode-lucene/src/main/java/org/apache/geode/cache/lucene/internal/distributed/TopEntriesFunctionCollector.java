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

package org.apache.geode.cache.lucene.internal.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A {@link ResultCollector} implementation for collecting and ordering {@link TopEntries}. The
 * {@link TopEntries} objects will be created by members when a {@link LuceneQuery} is executed on
 * the local data hosted by the member. The member executing this logic must have sufficient space
 * to hold all the {@link EntryScore} documents returned from the members.
 *
 * <p>
 * This class will perform a lazy merge operation. Merge will take place if the merge
 * {@link ResultCollector#getResult} is invoked or if the combined result size is more than the
 * limit set. In the later case, merge will be performed whenever {@link ResultCollector#addResult}
 * is invoked.
 */
public class TopEntriesFunctionCollector
    implements ResultCollector<TopEntriesCollector, TopEntries> {
  private static final Logger logger = LogService.getLogger();

  // Use this instance to perform reduce operation
  private final CollectorManager<TopEntriesCollector> manager;

  private final String id;

  private final Collection<TopEntriesCollector> subResults = new ArrayList<>();

  private TopEntriesCollector mergedResults;

  public TopEntriesFunctionCollector() {
    this(null);
  }

  public TopEntriesFunctionCollector(LuceneFunctionContext<TopEntriesCollector> context) {
    this(context, null);
  }

  public TopEntriesFunctionCollector(LuceneFunctionContext<TopEntriesCollector> context,
      InternalCache cache) {
    id = cache == null ? String.valueOf(hashCode()) : cache.getName();

    int limit = context == null ? 0 : context.getLimit();

    if (context != null && context.getCollectorManager() != null) {
      manager = context.getCollectorManager();
    } else {
      manager = new TopEntriesCollectorManager(id, limit);
    }
  }

  @Override
  public TopEntries getResult() throws FunctionException {
    return aggregateResults();
  }

  @Override
  public TopEntries getResult(long timeout, TimeUnit unit) throws FunctionException {
    return aggregateResults();
  }

  private TopEntries aggregateResults() {
    synchronized (subResults) {
      if (mergedResults != null) {
        return mergedResults.getEntries();
      }

      mergedResults = manager.reduce(subResults);
      return mergedResults.getEntries();
    }
  }

  @Override
  public void endResults() {}

  @Override
  public void clearResults() {
    synchronized (subResults) {
      subResults.clear();
    }
  }

  @Override
  public void addResult(DistributedMember memberID, TopEntriesCollector resultOfSingleExecution) {
    synchronized (subResults) {
      subResults.add(resultOfSingleExecution);
    }
  }

  String id() {
    return id;
  }
}
