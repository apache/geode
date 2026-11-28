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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorDeltaBuffer;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorRegistry;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * An Async event queue listener that writes all of the events in batches to Lucene
 */
public class LuceneEventListener implements AsyncEventListener {

  private static LuceneExceptionObserver exceptionObserver = exception -> {
  };
  private final InternalCache cache;

  private static final Logger logger = LogService.getLogger();

  private final RepositoryManager repositoryManager;

  public LuceneEventListener(InternalCache cache, RepositoryManager repositoryManager) {
    this.repositoryManager = repositoryManager;
    this.cache = cache;
  }

  @Override
  public void close() {}

  void logDebugMessage(String msg, Exception e) {
    logger.debug(msg, e);
  }

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
    Boolean initialPdxReadSerialized = cache.getPdxReadSerializedOverride();
    cache.setPdxReadSerializedOverride(true);

    Map<IndexRepository, List<AsyncEvent>> eventsByRepo = new LinkedHashMap<>();
    InternalLuceneIndex partitionedIndex =
        (repositoryManager instanceof PartitionedRepositoryManager)
            ? ((PartitionedRepositoryManager) repositoryManager).getIndex()
            : null;
    Tier0VectorRegistry tier0Registry =
        (partitionedIndex != null && cache != null) ? Tier0VectorRegistry.forCache(cache) : null;
    final boolean trackFlushCycles =
        tier0Registry != null && tier0Registry.isFlushCycleSamplingEnabled();

    try {
      for (AsyncEvent event : events) {
        Region region = event.getRegion();
        Object key = event.getKey();
        Object callbackArgument = event.getCallbackArgument();
        IndexRepository repository = repositoryManager.getRepository(region, key, callbackArgument);
        eventsByRepo.computeIfAbsent(repository, r -> new ArrayList<>()).add(event);
      }

      for (Map.Entry<IndexRepository, List<AsyncEvent>> repoEntry : eventsByRepo.entrySet()) {
        IndexRepository repo = repoEntry.getKey();
        List<AsyncEvent> repoEvents = repoEntry.getValue();
        int bucketId =
            (tier0Registry != null && repo.getRegion() instanceof BucketRegion)
                ? ((BucketRegion) repo.getRegion()).getId()
                : -1;
        ReentrantLock flushLock = (tier0Registry != null && bucketId >= 0)
            ? tier0Registry.getBucketFlushLock(
                partitionedIndex.getRegionPath(), partitionedIndex.getName(), bucketId)
            : null;
        if (flushLock != null) {
          flushLock.lock();
        }
        try {
          Tier0VectorDeltaBuffer<Object> buffer = null;
          Map<Object, Long> committedKeySeqs = null;
          int batchCount = 0;
          long updateNanos = 0L;

          if (tier0Registry != null && bucketId >= 0) {
            tier0Registry.awaitHydration(
                partitionedIndex.getRegionPath(), partitionedIndex.getName(), bucketId);
            buffer = tier0Registry.getBuffer(
                partitionedIndex.getRegionPath(), partitionedIndex.getName(), bucketId);
          }

          for (AsyncEvent event : repoEvents) {
            Region region = event.getRegion();
            Object key = event.getKey();

            if (buffer == null && tier0Registry != null && bucketId >= 0) {
              buffer = tier0Registry.getBuffer(
                  partitionedIndex.getRegionPath(), partitionedIndex.getName(), bucketId);
            }
            if (buffer != null) {
              // Capture the Tier-0 eviction watermark strictly BEFORE reading the live region
              // payload via region.getEntry(key) so a newer concurrent overlay write arriving
              // after the flushed payload was read can NEVER be evicted by this batch's commit.
              long seqBeforeRead = buffer.getKeySequence(key);
              if (seqBeforeRead >= 0L) {
                if (committedKeySeqs == null) {
                  committedKeySeqs = new HashMap<>();
                }
                committedKeySeqs.merge(key, seqBeforeRead, Math::max);
              }
            }

            long opStartNanos = (buffer != null && trackFlushCycles) ? System.nanoTime() : 0L;
            Object value = getValue(region.getEntry(key));
            if (value != null) {
              repo.update(key, value);
            } else {
              repo.delete(key);
            }
            if (buffer != null && trackFlushCycles) {
              batchCount++;
              updateNanos += System.nanoTime() - opStartNanos;
            }
          }

          if (buffer != null && committedKeySeqs != null && !committedKeySeqs.isEmpty()) {
            long commitStartNanos = trackFlushCycles ? System.nanoTime() : 0L;
            repo.commitAndRefreshBlocking();
            long commitEndNanos = trackFlushCycles ? System.nanoTime() : 0L;
            int preEvictTier0Size = trackFlushCycles ? buffer.getTotalSize() : 0;
            buffer.evictCommitted(committedKeySeqs);
            if (trackFlushCycles) {
              long flushDurationNanos = updateNanos + (commitEndNanos - commitStartNanos);
              tier0Registry.recordFlushCycle(
                  partitionedIndex.getRegionPath(),
                  partitionedIndex.getName(),
                  bucketId,
                  batchCount,
                  preEvictTier0Size,
                  flushDurationNanos);
            }
          } else {
            repo.commit();
          }
        } finally {
          if (flushLock != null) {
            flushLock.unlock();
          }
        }
      }
      return true;
    } catch (BucketNotFoundException | RegionDestroyedException | PrimaryBucketException e) {
      logDebugMessage("Bucket not found while saving to lucene index: " + e.getMessage(), e);
      return false;
    } catch (CacheClosedException e) {
      logDebugMessage("Unable to save to lucene index, cache has been closed", e);
      return false;
    } catch (AlreadyClosedException e) {
      logDebugMessage("Unable to commit, the lucene index is already closed", e);
      return false;
    } catch (IOException e) {
      throw new InternalGemFireError("Unable to save to lucene index", e);
    } finally {
      cache.setPdxReadSerializedOverride(initialPdxReadSerialized);
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
