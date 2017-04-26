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

package org.apache.geode.cache.lucene.internal;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.logging.log4j.Logger;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.CacheObserverHolder;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy.TestHook;
import org.apache.geode.internal.logging.LogService;
import org.apache.lucene.store.AlreadyClosedException;

/**
 * An Async event queue listener that writes all of the events in batches to Lucene
 */
public class LuceneEventListener implements AsyncEventListener {

  private static LuceneExceptionObserver exceptionObserver = exception -> {
  };

  Logger logger = LogService.getLogger();

  private final RepositoryManager repositoryManager;
  private CountDownLatch isFileAndChunkRegionReady = new CountDownLatch(1);

  public LuceneEventListener(RepositoryManager repositoryManager) {
    this.repositoryManager = repositoryManager;
  }

  @Override
  public void close() {}

  @Override
  public boolean processEvents(List<AsyncEvent> events) {
    try {
      isFileAndChunkRegionReady.await();
    } catch (InterruptedException e) {
      logger.debug("Interrupted while waiting on the countdown latch to begin processing events: "
          + e.getMessage(), e);
      return false;
    }
    try {
      return process(events);
    } catch (RuntimeException e) {
      exceptionObserver.onException(e);
      throw e;
    } catch (Error e) {
      exceptionObserver.onException(e);
      throw e;
    }

  }

  public void allowProcessingOfEvents() {
    isFileAndChunkRegionReady.countDown();
  }

  protected boolean process(final List<AsyncEvent> events) {
    // Try to get a PDX instance if possible, rather than a deserialized object
    DefaultQuery.setPdxReadSerialized(true);

    Set<IndexRepository> affectedRepos = new HashSet<IndexRepository>();

    try {
      for (AsyncEvent event : events) {
        Region region = event.getRegion();
        Object key = event.getKey();
        Object callbackArgument = event.getCallbackArgument();

        IndexRepository repository = repositoryManager.getRepository(region, key, callbackArgument);

        final Entry entry = region.getEntry(key);
        Object value;
        try {
          value = entry == null ? null : entry.getValue();
        } catch (EntryDestroyedException e) {
          value = null;
        }

        if (value != null) {
          repository.update(key, value);
        } else {
          repository.delete(key);
        }

        affectedRepos.add(repository);
      }

      for (IndexRepository repo : affectedRepos) {
        repo.commit();
      }
      return true;
    } catch (BucketNotFoundException | RegionDestroyedException | PrimaryBucketException e) {
      logger.debug("Bucket not found while saving to lucene index: " + e.getMessage(), e);
      return false;
    } catch (CacheClosedException e) {
      logger.debug("Unable to save to lucene index, cache has been closed", e);
      return false;
    } catch (AlreadyClosedException e) {
      logger.debug("Unable to commit, the lucene index is already closed", e);
      return false;
    } catch (IOException e) {
      throw new InternalGemFireError("Unable to save to lucene index", e);
    } finally {
      DefaultQuery.setPdxReadSerialized(false);
    }
  }

  public static void setExceptionObserver(LuceneExceptionObserver observer) {
    if (observer == null) {
      observer = exception -> {
      };
    }

    exceptionObserver = observer;
  }

  public static LuceneExceptionObserver getExceptionObserver() {
    return exceptionObserver;
  }
}
