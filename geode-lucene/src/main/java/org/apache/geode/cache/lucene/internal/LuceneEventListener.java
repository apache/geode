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

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.logging.log4j.Logger;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.PrimaryBucketException;
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

  public LuceneEventListener(RepositoryManager repositoryManager) {
    this.repositoryManager = repositoryManager;
  }

  @Override
  public void close() {}

  @Override
  public boolean processEvents(List<AsyncEvent> events) {
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

        Object value = getValue(region.getEntry(key));
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

  private Object getValue(Region.Entry entry) {
    final EntrySnapshot es = (EntrySnapshot) entry;
    Object value;
    try {
      value = es == null ? null : es.getRawValue(true);
    } catch (EntryDestroyedException e) {
      value = null;
    }
    return value;
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
