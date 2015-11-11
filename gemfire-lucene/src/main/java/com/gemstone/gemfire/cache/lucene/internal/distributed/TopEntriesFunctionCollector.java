/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * A {@link ResultCollector} implementation for collecting and ordering {@link TopEntries}. The {@link TopEntries}
 * objects will be created by members when a {@link LuceneQuery} is executed on the local data hosted by the member. The
 * member executing this logic must have sufficient space to hold all the {@link EntryScore} documents returned from the
 * members.
 * 
 * <p>
 * This class will perform a lazy merge operation. Merge will take place if the merge {@link ResultCollector#getResult}
 * is invoked or if the combined result size is more than the limit set. In the later case, merge will be performed
 * whenever {@link ResultCollector#addResult} is invoked.
 */
public class TopEntriesFunctionCollector implements ResultCollector<TopEntriesCollector, TopEntries> {
  // Use this instance to perform reduce operation
  final CollectorManager<TopEntriesCollector> manager;

  // latch to wait till all results are collected
  private final CountDownLatch waitForResults = new CountDownLatch(1);

  final String id;

  // Instance of gemfire cache to check status and other utility methods
  final private GemFireCacheImpl cache;
  private static final Logger logger = LogService.getLogger();

  private final Collection<TopEntriesCollector> subResults = new ArrayList<>();
  private TopEntriesCollector mergedResults;

  public TopEntriesFunctionCollector() {
    this(null);
  }

  public TopEntriesFunctionCollector(LuceneFunctionContext<TopEntriesCollector> context) {
    this(context, null);
  }

  public TopEntriesFunctionCollector(LuceneFunctionContext<TopEntriesCollector> context, GemFireCacheImpl cache) {
    this.cache = cache;
    id = cache == null ? String.valueOf(this.hashCode()) : cache.getName();

    int limit = context == null ? 0 : context.getLimit();
    
    if (context != null && context.getCollectorManager() != null) {
      this.manager = context.getCollectorManager();
    } else {
      this.manager = new TopEntriesCollectorManager(id, limit);
    }
  }

  @Override
  public TopEntries getResult() throws FunctionException {
    try {
      waitForResults.await();
    } catch (InterruptedException e) {
      logger.debug("Interrupted while waiting for result collection", e);
      Thread.currentThread().interrupt();
      if (cache != null) {
        cache.getCancelCriterion().checkCancelInProgress(e);
      }
      throw new FunctionException(e);
    }

    return aggregateResults();
  }

  @Override
  public TopEntries getResult(long timeout, TimeUnit unit) throws FunctionException {
    try {
      boolean result = waitForResults.await(timeout, unit);
      if (!result) {
        throw new FunctionException("Did not receive results from all members within wait time");
      }
    } catch (InterruptedException e) {
      logger.debug("Interrupted while waiting for result collection", e);
      Thread.currentThread().interrupt();
      if (cache != null) {
        cache.getCancelCriterion().checkCancelInProgress(e);
      }
      throw new FunctionException(e);
    }

    return aggregateResults();
  }

  private TopEntries aggregateResults() {
    synchronized (subResults) {
      if (mergedResults != null) {
        return mergedResults.getEntries();
      }
      
      try {
        mergedResults = manager.reduce(subResults);
        return mergedResults.getEntries();
      } catch (IOException e) {
        logger.debug("Error while merging function execution results", e);
        throw new FunctionException(e);
      }
    }
  }

  @Override
  public void endResults() {
    synchronized (subResults) {
      waitForResults.countDown();
    }
  }

  @Override
  public void clearResults() {
    synchronized (subResults) {
      if (waitForResults.getCount() == 0) {
        throw new IllegalStateException("This collector is closed and cannot accept anymore results");
      }

      subResults.clear();
    }
  }

  @Override
  public void addResult(DistributedMember memberID, TopEntriesCollector resultOfSingleExecution) {
    synchronized (subResults) {
      if (waitForResults.getCount() == 0) {
        throw new IllegalStateException("This collector is closed and cannot accept anymore results");
      }
      subResults.add(resultOfSingleExecution);
    }
  }
}
