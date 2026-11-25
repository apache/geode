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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneIndexNotFoundException;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.InternalLuceneIndex;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationInProgressException;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.PartitionedRepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil;
import org.apache.geode.cache.lucene.internal.vector.Tier0MetadataMatcher;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorDeltaBuffer;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorRegistry;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.ResourcePermission;

/**
 * Distributed Geode {@link org.apache.geode.cache.execute.Function} that executes hybrid vector
 * k-NN + metadata pre-filtered search per primary bucket on {@code PARTITION} regions.
 *
 * <p>
 * On each local primary bucket, execution follows the Theorem 2 linearizable read ordering:
 * <ol>
 * <li>Wait-free snapshot of the bucket's {@link Tier0VectorDeltaBuffer} {@code (E_active,
 * E_flushing)} in reverse epoch order.</li>
 * <li>Pre-filtered Lucene 9.x {@link KnnFloatVectorQuery} pushing metadata filter predicates and
 * masked-key exclusions directly into HNSW graph traversal.</li>
 * <li>Exact SIMD-friendly Tier-0 batch scoring unioned with Lucene hits under highest-epoch
 * precedence (masking tombstoned and superseded docIDs), reduced locally and across members via
 * {@link TopEntriesCollectorManager} / {@link TopEntriesFunctionCollector}.</li>
 * </ol>
 */
public class DistributedHybridKnnFunction implements InternalFunction<HybridKnnQueryContext> {
  private static final long serialVersionUID = 1L;
  public static final String ID = DistributedHybridKnnFunction.class.getName();

  /**
   * Upper bound on inline {@code MUST_NOT} key-masking clauses pushed into Lucene's
   * {@link BooleanQuery} (leaving ample headroom below Lucene's default 1024 clause limit). Above
   * this threshold, the function falls back to oversampling {@code k + maskedKeys.size()} and
   * post-masking.
   */
  static final int MAX_INLINE_MASK_TERMS = 512;
  static final int MAX_OPTIMISTIC_SEARCHER_EPOCH_RETRIES = 16;
  private static final int MAX_LUCENE_K = 100_000;
  private static final long MAX_CONCURRENT_FLUSH_WAIT_MILLIS = 500L;
  private static final AtomicLong UNSUPPORTED_FILTER_FLUSH_FALLBACK_COUNT = new AtomicLong(0L);
  private static final AtomicLong INLINE_MASK_QUERY_COUNT = new AtomicLong(0L);
  private static final AtomicLong OVERSAMPLE_FALLBACK_QUERY_COUNT = new AtomicLong(0L);

  private static final Logger logger = LogService.getLogger();

  /**
   * Returns the cumulative count of bucket flush-and-refresh fallbacks triggered when a hybrid
   * k-NN query supplied a Lucene filter AST not natively supported by {@link Tier0MetadataMatcher}.
   */
  public static long getUnsupportedFilterFlushFallbackCount() {
    return UNSUPPORTED_FILTER_FLUSH_FALLBACK_COUNT.get();
  }

  static void resetUnsupportedFilterFlushFallbackCount() {
    UNSUPPORTED_FILTER_FLUSH_FALLBACK_COUNT.set(0L);
  }

  /**
   * Returns the cumulative count of bucket queries that pushed inline {@code MUST_NOT} clauses
   * into Lucene's {@link KnnFloatVectorQuery} ({@code 0 < |T_mask| <= maxInlineTerms}).
   */
  public static long getInlineMaskQueryCount() {
    return INLINE_MASK_QUERY_COUNT.get();
  }

  /**
   * Returns the cumulative count of bucket queries that switched from inline {@code MUST_NOT}
   * masking to bounded {@code k + |T_mask|} oversampling + post-masking
   * ({@code |T_mask| > maxInlineTerms}).
   */
  public static long getOversampleFallbackQueryCount() {
    return OVERSAMPLE_FALLBACK_QUERY_COUNT.get();
  }

  /**
   * Resets the static server-side masking path telemetry counters (primarily for testing and
   * benchmark instrumentation).
   */
  public static void resetMaskTelemetry() {
    INLINE_MASK_QUERY_COUNT.set(0L);
    OVERSAMPLE_FALLBACK_QUERY_COUNT.set(0L);
  }

  private static final Comparator<EntryScore<Object>> SCORE_DESC_COMPARATOR = (a, b) -> {
    int cmp = Float.compare(b.getScore(), a.getScore());
    if (cmp != 0) {
      return cmp;
    }
    return String.valueOf(a.getKey()).compareTo(String.valueOf(b.getKey()));
  };

  /**
   * Executes a distributed hybrid k-NN query across all primary buckets of {@code region}, merging
   * per-member results in a coordinator priority queue ({@link TopEntriesFunctionCollector}).
   */
  @SuppressWarnings("unchecked")
  public static <K> TopEntries<K> executeOnRegion(Region<K, ?> region,
      HybridKnnQueryContext queryContext) {
    Objects.requireNonNull(region, "region must not be null");
    Objects.requireNonNull(queryContext, "queryContext must not be null");

    TopEntriesCollectorManager manager = new TopEntriesCollectorManager(null, queryContext.getK());
    LuceneFunctionContext<TopEntriesCollector> collectorContext =
        new LuceneFunctionContext<>(null, queryContext.getIndexName(), manager,
            queryContext.getK());
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(collectorContext);

    ResultCollector<TopEntriesCollector, TopEntries<K>> rc = FunctionService.onRegion(region)
        .setArguments(queryContext)
        .withCollector(collector)
        .execute(ID);
    return rc.getResult();
  }

  @Override
  public void execute(FunctionContext<HybridKnnQueryContext> context) {
    execute(context, false);
  }

  public void execute(FunctionContext<HybridKnnQueryContext> context, boolean waitForRepository) {
    if (!(context instanceof RegionFunctionContext)) {
      throw new IllegalArgumentException(
          "DistributedHybridKnnFunction must be executed on a Region");
    }
    RegionFunctionContext ctx = (RegionFunctionContext) context;
    ResultSender<TopEntriesCollector> resultSender = ctx.getResultSender();
    Region<?, ?> region = ctx.getDataSet();

    HybridKnnQueryContext knnContext = (HybridKnnQueryContext) ctx.getArguments();
    if (knnContext == null) {
      throw new IllegalArgumentException("Missing HybridKnnQueryContext argument");
    }

    Cache cache = context.getCache();
    InternalLuceneIndex index = getLuceneIndex(cache, region, knnContext.getIndexName());
    if (index == null) {
      throw new LuceneIndexNotFoundException(knnContext.getIndexName(), region.getFullPath());
    }

    RepositoryManager repoManager = index.getRepositoryManager();
    LuceneIndexStats stats = index.getIndexStats();
    int k = knnContext.getK();
    float[] queryVector = knnContext.getQueryVector();

    Analyzer indexAnalyzer =
        (index instanceof LuceneIndexImpl) ? ((LuceneIndexImpl) index).getAnalyzer() : null;
    Query parsedFilterQuery = parseFilterQueryString(knnContext, indexAnalyzer);
    Query userFilterQuery =
        buildUserFilterQuery(parsedFilterQuery, knnContext.getMetadataEqualityFilters());
    Predicate<Map<String, Object>> tier0FilterPredicate = Tier0MetadataMatcher
        .buildPredicate(knnContext.getMetadataEqualityFilters(), parsedFilterQuery);

    TopEntriesCollectorManager manager = new TopEntriesCollectorManager(null, k);
    Collection<TopEntriesCollector> bucketCollectors = new ArrayList<>();
    TopEntriesCollector mergedResult = null;
    Tier0VectorRegistry registry =
        knnContext.isIncludeTier0() ? Tier0VectorRegistry.forCache(cache) : null;

    try {
      long start = stats.startQuery();
      try {
        Map<Integer, IndexRepository> reposByBucket =
            resolveRepositoriesByBucket(repoManager, ctx, waitForRepository);

        for (Map.Entry<Integer, IndexRepository> bucketEntry : reposByBucket.entrySet()) {
          int bucketId = bucketEntry.getKey();
          IndexRepository repo = bucketEntry.getValue();

          TopEntriesCollector bucketCollector = executeOnBucket(
              cache, region, bucketId, repo, knnContext, queryVector, userFilterQuery,
              tier0FilterPredicate, registry, manager);
          bucketCollectors.add(bucketCollector);
        }

        mergedResult = manager.reduce(bucketCollectors);
      } finally {
        stats.endQuery(start, mergedResult == null ? 0 : mergedResult.size());
      }
      stats.incNumberOfQueryExecuted();
      resultSender.lastResult(mergedResult);
    } catch (IOException | BucketNotFoundException | CacheClosedException
        | PrimaryBucketException e) {
      logger.debug("Exception during distributed hybrid k-NN query function", e);
      throw new InternalFunctionInvocationTargetException(e);
    } catch (LuceneIndexCreationInProgressException ex) {
      throw new FunctionException(
          new LuceneQueryException("Lucene Index is not available, currently indexing"));
    }
  }

  private TopEntriesCollector executeOnBucket(Cache cache, Region<?, ?> region, int bucketId,
      IndexRepository repo, HybridKnnQueryContext knnContext, float[] queryVector,
      Query userFilterQuery, Predicate<Map<String, Object>> tier0FilterPredicate,
      Tier0VectorRegistry registry, TopEntriesCollectorManager manager) throws IOException {
    int k = knnContext.getK();
    String regionPath = region.getFullPath();
    boolean unsupportedFilter =
        userFilterQuery != null && !Tier0MetadataMatcher.isSupported(userFilterQuery);

    // Theorem 2 ordering: wait for any in-flight primary promotion hydration, then bind the
    // Tier-0 snapshot and Lucene IndexSearcher acquisition to a single consistent epoch/cut.
    // For unsupported filters (Path B), acquire the per-bucket write barrier around flush +
    // snapshot + IndexSearcher acquisition so T_mask is empty at the searcher cut. For supported
    // filters, validate IndexRepository.getSearcherEpoch() optimistically up to 16 times across
    // snapshot -> searcher acquisition, falling back to the per-bucket write barrier if contended.
    Tier0VectorDeltaBuffer.Snapshot<Object> tier0Snapshot = null;
    Set<Object> maskedKeys = Collections.emptySet();
    Tier0VectorDeltaBuffer<Object> pathBBuffer = null;
    final AtomicBoolean pathBWriteReleased = new AtomicBoolean(false);
    Runnable pathBReleaseBarrier = null;
    RawListResultCollector luceneRawCollector = null;
    boolean usedInlineMask = false;
    boolean usedOversampleFallback = false;
    try {
      Tier0VectorDeltaBuffer<Object> buffer = null;
      if (registry != null) {
        registry.awaitHydration(regionPath, knnContext.getIndexName(), bucketId);
        buffer = registry.getBuffer(regionPath, knnContext.getIndexName(), bucketId);
      }

      if (unsupportedFilter && buffer != null && repo != null) {
        buffer.lockPathBWrite();
        pathBBuffer = buffer;
        final Tier0VectorDeltaBuffer<Object> lockedBuffer = buffer;
        pathBReleaseBarrier = () -> {
          if (pathBWriteReleased.compareAndSet(false, true)) {
            lockedBuffer.unlockPathBWrite();
          }
        };
        tier0Snapshot = buffer.snapshot();
        if (!tier0Snapshot.isEmpty()) {
          // Instead of silently dropping fresh Tier-0 entries via fail-closed evaluation on an
          // unsupported Lucene Query AST (e.g., FuzzyQuery/SpanQuery), synchronously flush the
          // bucket into Lucene + blocking-refresh while holding the write barrier so
          // IndexSearcher evaluates the filter with T_mask empty at the searcher cut.
          flushBucketForUnsupportedFilter(
              cache, region, regionPath, knnContext.getIndexName(), bucketId, registry,
              buffer, repo);
          tier0Snapshot = buffer.snapshot();
        }
        maskedKeys = tier0Snapshot.getMaskedKeys();
        PreparedBucketQuery prepared = prepareBucketQuery(
            knnContext, k, queryVector, userFilterQuery, maskedKeys);
        usedInlineMask = prepared.usedInlineMask;
        usedOversampleFallback = prepared.usedOversampleFallback;
        luceneRawCollector = new RawListResultCollector("bucket-" + bucketId, prepared.luceneK);
        if (prepared.executeFp32Rerank) {
          repo.queryWithFp32Rerank(prepared.knnQuery, prepared.luceneK, prepared.queryK,
              knnContext.getVectorFieldName(), queryVector, knnContext.getSimilarityFunction(),
              luceneRawCollector, pathBReleaseBarrier);
        } else {
          repo.query(prepared.knnQuery, prepared.luceneK, luceneRawCollector, pathBReleaseBarrier);
        }
      } else {
        boolean queryCompleted = false;
        for (int attempt = 0; attempt < MAX_OPTIMISTIC_SEARCHER_EPOCH_RETRIES; attempt++) {
          long epoch0 = repo != null ? repo.getSearcherEpoch() : 0L;
          if (repo != null && (epoch0 & 1L) != 0L) {
            Thread.onSpinWait();
            continue;
          }
          tier0Snapshot = buffer != null ? buffer.snapshot() : null;
          maskedKeys =
              tier0Snapshot != null ? tier0Snapshot.getMaskedKeys() : Collections.emptySet();
          PreparedBucketQuery prepared = prepareBucketQuery(
              knnContext, k, queryVector, userFilterQuery, maskedKeys);
          RawListResultCollector attemptCollector =
              new RawListResultCollector("bucket-" + bucketId, prepared.luceneK);
          if (repo == null) {
            luceneRawCollector = attemptCollector;
            usedInlineMask = prepared.usedInlineMask;
            usedOversampleFallback = prepared.usedOversampleFallback;
            queryCompleted = true;
            break;
          }
          boolean epochMatched = prepared.executeFp32Rerank
              ? repo.tryQueryWithFp32RerankAtEpoch(epoch0, prepared.knnQuery, prepared.luceneK,
                  prepared.queryK, knnContext.getVectorFieldName(), queryVector,
                  knnContext.getSimilarityFunction(), attemptCollector)
              : repo.tryQueryAtEpoch(epoch0, prepared.knnQuery, prepared.luceneK, attemptCollector);
          if (epochMatched) {
            luceneRawCollector = attemptCollector;
            usedInlineMask = prepared.usedInlineMask;
            usedOversampleFallback = prepared.usedOversampleFallback;
            queryCompleted = true;
            break;
          }
          Thread.onSpinWait();
        }

        if (!queryCompleted && repo != null) {
          if (buffer != null) {
            buffer.lockPathBWrite();
            pathBBuffer = buffer;
            final Tier0VectorDeltaBuffer<Object> lockedBuffer = buffer;
            pathBReleaseBarrier = () -> {
              if (pathBWriteReleased.compareAndSet(false, true)) {
                lockedBuffer.unlockPathBWrite();
              }
            };
          }
          long deadlineNanos = System.nanoTime() + MAX_CONCURRENT_FLUSH_WAIT_MILLIS * 1_000_000L;
          while (!queryCompleted) {
            long fallbackEpoch = repo.getSearcherEpoch();
            if ((fallbackEpoch & 1L) != 0L && System.nanoTime() < deadlineNanos) {
              Thread.yield();
              continue;
            }
            tier0Snapshot = buffer != null ? buffer.snapshot() : null;
            maskedKeys =
                tier0Snapshot != null ? tier0Snapshot.getMaskedKeys() : Collections.emptySet();
            PreparedBucketQuery prepared = prepareBucketQuery(
                knnContext, k, queryVector, userFilterQuery, maskedKeys);
            RawListResultCollector attemptCollector =
                new RawListResultCollector("bucket-" + bucketId, prepared.luceneK);
            boolean epochMatched = false;
            if ((fallbackEpoch & 1L) == 0L) {
              epochMatched = prepared.executeFp32Rerank
                  ? repo.tryQueryWithFp32RerankAtEpoch(fallbackEpoch, prepared.knnQuery,
                      prepared.luceneK, prepared.queryK, knnContext.getVectorFieldName(),
                      queryVector, knnContext.getSimilarityFunction(), attemptCollector,
                      pathBReleaseBarrier)
                  : repo.tryQueryAtEpoch(fallbackEpoch, prepared.knnQuery, prepared.luceneK,
                      attemptCollector, pathBReleaseBarrier);
            }
            if (epochMatched) {
              luceneRawCollector = attemptCollector;
              usedInlineMask = prepared.usedInlineMask;
              usedOversampleFallback = prepared.usedOversampleFallback;
              queryCompleted = true;
            } else if (((fallbackEpoch & 1L) == 0L && repo.getSearcherEpoch() == fallbackEpoch)
                || System.nanoTime() >= deadlineNanos) {
              // Either repository is a non-epoch-tracking implementation (e.g. unstubbed mock) or
              // continuous commit contention hit bounded wait: run once under the write barrier.
              RawListResultCollector finalCollector =
                  new RawListResultCollector("bucket-" + bucketId, prepared.luceneK);
              if (prepared.executeFp32Rerank) {
                repo.queryWithFp32Rerank(prepared.knnQuery, prepared.luceneK, prepared.queryK,
                    knnContext.getVectorFieldName(), queryVector,
                    knnContext.getSimilarityFunction(), finalCollector, pathBReleaseBarrier);
              } else {
                repo.query(
                    prepared.knnQuery, prepared.luceneK, finalCollector, pathBReleaseBarrier);
              }
              luceneRawCollector = finalCollector;
              usedInlineMask = prepared.usedInlineMask;
              usedOversampleFallback = prepared.usedOversampleFallback;
              queryCompleted = true;
            } else {
              Thread.yield();
            }
          }
        }
      }
    } finally {
      if (pathBReleaseBarrier != null) {
        pathBReleaseBarrier.run();
      } else if (pathBBuffer != null && pathBWriteReleased.compareAndSet(false, true)) {
        pathBBuffer.unlockPathBWrite();
      }
    }

    if (usedInlineMask) {
      INLINE_MASK_QUERY_COUNT.incrementAndGet();
      if (registry != null) {
        registry.incrementInlineMaskQueryCount();
      }
    } else if (usedOversampleFallback) {
      OVERSAMPLE_FALLBACK_QUERY_COUNT.incrementAndGet();
      if (registry != null) {
        registry.incrementOversampleFallbackQueryCount();
      }
    }

    // Union Lucene HNSW hits (masked by Tier-0 keys) with exact Tier-0 delta hits
    Map<Object, EntryScore<Object>> mergedByKey = new HashMap<>();
    if (luceneRawCollector != null) {
      for (EntryScore<Object> hit : luceneRawCollector.getHits()) {
        if (!maskedKeys.contains(hit.getKey())) {
          mergedByKey.put(hit.getKey(), hit);
        }
      }
    }

    if (tier0Snapshot != null && !tier0Snapshot.getLiveEntries().isEmpty()) {
      List<EntryScore<Object>> tier0Hits = tier0Snapshot.search(
          queryVector, k, knnContext.getSimilarityFunction(), tier0FilterPredicate);
      for (EntryScore<Object> hit : tier0Hits) {
        // Tier-0 always takes highest-epoch precedence over committed Lucene segments
        mergedByKey.put(hit.getKey(), hit);
      }
    }

    List<EntryScore<Object>> sorted = new ArrayList<>(mergedByKey.values());
    sorted.sort(SCORE_DESC_COMPARATOR);

    TopEntriesCollector bucketCollector = manager.newCollector("bucket-" + bucketId);
    int limit = Math.min(k, sorted.size());
    for (int i = 0; i < limit; i++) {
      bucketCollector.collect(sorted.get(i));
    }
    return bucketCollector;
  }

  private static PreparedBucketQuery prepareBucketQuery(HybridKnnQueryContext knnContext, int k,
      float[] queryVector, Query userFilterQuery, Set<Object> maskedKeys) {
    int maxInlineTerms =
        Math.min(MAX_INLINE_MASK_TERMS, Math.max(0, IndexSearcher.getMaxClauseCount() - 2));
    int luceneK;
    boolean usedInlineMask = false;
    boolean usedOversampleFallback = false;
    Query effectiveFilterQuery;
    if (maskedKeys.isEmpty()) {
      effectiveFilterQuery = userFilterQuery;
      luceneK = k;
    } else if (maskedKeys.size() <= maxInlineTerms) {
      effectiveFilterQuery = buildMaskedFilterQuery(userFilterQuery, maskedKeys);
      luceneK = k;
      usedInlineMask = true;
    } else {
      effectiveFilterQuery = userFilterQuery;
      luceneK = (int) Math.min((long) k + maskedKeys.size(), MAX_LUCENE_K);
      usedOversampleFallback = true;
    }

    int rerankFactor = knnContext.getRerankOversampleFactor();
    boolean executeFp32Rerank =
        rerankFactor > HybridKnnQueryContext.DEFAULT_RERANK_OVERSAMPLE_FACTOR
            || knnContext.isRerankBeamCandidates();
    int queryK = luceneK;
    if (rerankFactor > HybridKnnQueryContext.DEFAULT_RERANK_OVERSAMPLE_FACTOR) {
      long targetOversample = usedOversampleFallback
          ? (long) k * rerankFactor + maskedKeys.size()
          : (long) k * rerankFactor;
      queryK = Math.max(luceneK, (int) Math.min(targetOversample, MAX_LUCENE_K));
    }

    KnnFloatVectorQuery knnQuery = effectiveFilterQuery == null
        ? new KnnFloatVectorQuery(knnContext.getVectorFieldName(), queryVector, queryK)
        : new KnnFloatVectorQuery(knnContext.getVectorFieldName(), queryVector, queryK,
            effectiveFilterQuery);

    return new PreparedBucketQuery(
        knnQuery, luceneK, queryK, executeFp32Rerank, usedInlineMask, usedOversampleFallback);
  }

  private static final class PreparedBucketQuery {
    final KnnFloatVectorQuery knnQuery;
    final int luceneK;
    final int queryK;
    final boolean executeFp32Rerank;
    final boolean usedInlineMask;
    final boolean usedOversampleFallback;

    PreparedBucketQuery(KnnFloatVectorQuery knnQuery, int luceneK, int queryK,
        boolean executeFp32Rerank, boolean usedInlineMask, boolean usedOversampleFallback) {
      this.knnQuery = knnQuery;
      this.luceneK = luceneK;
      this.queryK = queryK;
      this.executeFp32Rerank = executeFp32Rerank;
      this.usedInlineMask = usedInlineMask;
      this.usedOversampleFallback = usedOversampleFallback;
    }
  }

  private void flushBucketForUnsupportedFilter(Cache cache, Region<?, ?> region, String regionPath,
      String indexName, int bucketId, Tier0VectorRegistry registry,
      Tier0VectorDeltaBuffer<Object> buffer, IndexRepository repo) throws IOException {
    long deadlineNanos = System.nanoTime() + MAX_CONCURRENT_FLUSH_WAIT_MILLIS * 1_000_000L;
    boolean flushedOrRefreshed = false;
    while (true) {
      try {
        boolean flushed = registry.flushBucket(regionPath, indexName, bucketId, repo,
            entry -> readRegionValueForFlush(cache, region, bucketId, entry.getKey()));
        if (!flushed) {
          repo.commitAndRefreshBlocking();
        }
        flushedOrRefreshed = true;
        break;
      } catch (IllegalStateException flushAlreadyInProgress) {
        boolean inProgressMsg = flushAlreadyInProgress.getMessage() != null
            && flushAlreadyInProgress.getMessage().contains("Flush already in progress");
        if (System.nanoTime() >= deadlineNanos || (!inProgressMsg && !buffer.isFlushing())) {
          repo.commitAndRefreshBlocking();
          flushedOrRefreshed = true;
          break;
        }
        if (!buffer.isFlushing()) {
          // Concurrent flush completed while we were racing to beginFlush; retry immediately.
          continue;
        }
        try {
          Thread.sleep(1L);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    if (flushedOrRefreshed) {
      registry.incrementUnsupportedFilterFlushFallbackCount();
      UNSUPPORTED_FILTER_FLUSH_FALLBACK_COUNT.incrementAndGet();
    }
  }

  private Object readRegionValueForFlush(Cache cache, Region<?, ?> region, int bucketId,
      Object key) {
    InternalCache internalCache = (cache instanceof InternalCache) ? (InternalCache) cache : null;
    boolean initialPdxReadSerialized = false;
    if (internalCache != null) {
      initialPdxReadSerialized = internalCache.getPdxReadSerializedOverride();
      internalCache.setPdxReadSerializedOverride(true);
    }
    try {
      if (region instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion) region;
        if (pr.getDataStore() != null) {
          BucketRegion localBucket = pr.getDataStore().getLocalBucketById(bucketId);
          if (localBucket != null) {
            // Read lock-free from VM memory first to preserve strict lock ordering against
            // RegionMapPut/Destroy (synchronized(RegionEntry) -> CacheListener -> readLock).
            RegionEntry re = localBucket.getRegionEntry(key);
            if (re != null) {
              if (re.isRemoved()) {
                return null;
              }
              Object vmVal = re.getValueInVM(localBucket);
              if (vmVal == null || vmVal == Token.TOMBSTONE || Token.isInvalidOrRemoved(vmVal)) {
                return null;
              }
              if (vmVal != Token.NOT_AVAILABLE) {
                if (vmVal instanceof CachedDeserializable) {
                  Object deserialized =
                      ((CachedDeserializable) vmVal).getDeserializedValue(null, null);
                  if (deserialized == null || deserialized == Token.TOMBSTONE
                      || Token.isInvalidOrRemoved(deserialized)) {
                    return null;
                  }
                  return deserialized;
                }
                return vmVal;
              }
              // Only fault in from disk when evicted (Token.NOT_AVAILABLE).
              return extractEntryValue(localBucket.getEntry(key));
            }
            return extractEntryValue(localBucket.getEntry(key));
          }
        }
      }
      Region.Entry<?, ?> entry = region.getEntry(key);
      if (entry != null) {
        return extractEntryValue(entry);
      }
      return region.get(key);
    } finally {
      if (internalCache != null) {
        internalCache.setPdxReadSerializedOverride(initialPdxReadSerialized);
      }
    }
  }

  private Object extractEntryValue(Region.Entry<?, ?> entry) {
    if (entry == null) {
      return null;
    }
    try {
      if (entry instanceof EntrySnapshot) {
        return ((EntrySnapshot) entry).getRawValue(true);
      }
      return entry.getValue();
    } catch (EntryDestroyedException e) {
      return null;
    }
  }

  private Map<Integer, IndexRepository> resolveRepositoriesByBucket(RepositoryManager repoManager,
      RegionFunctionContext ctx, boolean waitForRepository) throws BucketNotFoundException {
    if (repoManager instanceof PartitionedRepositoryManager) {
      return ((PartitionedRepositoryManager) repoManager)
          .getRepositoriesByBucket(ctx, waitForRepository);
    }
    Collection<IndexRepository> repos = repoManager.getRepositories(ctx, waitForRepository);
    if (repos == null || repos.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<Integer, IndexRepository> map = new LinkedHashMap<>(repos.size());
    int syntheticBucket = 0;
    for (IndexRepository repo : repos) {
      int bucketId;
      if (repo != null && repo.getRegion() instanceof BucketRegion) {
        bucketId = ((BucketRegion) repo.getRegion()).getId();
      } else {
        bucketId = syntheticBucket++;
      }
      map.put(bucketId, repo);
    }
    return map;
  }

  static Query parseFilterQueryString(HybridKnnQueryContext knnContext, Analyzer analyzer) {
    String filterQueryString = knnContext.getFilterQueryString();
    if (filterQueryString == null || filterQueryString.trim().isEmpty()) {
      return null;
    }
    String defaultField = knnContext.getDefaultFilterField();
    if (defaultField == null) {
      defaultField = "";
    }
    Analyzer effectiveAnalyzer = analyzer != null ? analyzer : new StandardAnalyzer();
    try {
      return new QueryParser(defaultField, effectiveAnalyzer).parse(filterQueryString);
    } catch (ParseException e) {
      throw new FunctionException(
          new LuceneQueryException("Failed to parse hybrid k-NN filter query: " + filterQueryString,
              e));
    }
  }

  static Query buildUserFilterQuery(Query parsedFilterQuery,
      Map<String, String> metadataEqualityFilters) {
    boolean hasEquality = metadataEqualityFilters != null && !metadataEqualityFilters.isEmpty();
    if (!hasEquality) {
      return parsedFilterQuery;
    }
    if (parsedFilterQuery == null && metadataEqualityFilters.size() == 1) {
      Map.Entry<String, String> single = metadataEqualityFilters.entrySet().iterator().next();
      return new TermQuery(new Term(single.getKey(), single.getValue()));
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    if (parsedFilterQuery != null) {
      builder.add(parsedFilterQuery, BooleanClause.Occur.FILTER);
    }
    for (Map.Entry<String, String> filter : metadataEqualityFilters.entrySet()) {
      builder.add(new TermQuery(new Term(filter.getKey(), filter.getValue())),
          BooleanClause.Occur.FILTER);
    }
    return builder.build();
  }

  static Query buildMaskedFilterQuery(Query userFilterQuery, Set<Object> maskedKeys) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(userFilterQuery != null ? userFilterQuery : new MatchAllDocsQuery(),
        BooleanClause.Occur.FILTER);
    for (Object key : maskedKeys) {
      builder.add(new TermQuery(SerializerUtil.toKeyTerm(key)), BooleanClause.Occur.MUST_NOT);
    }
    return builder.build();
  }

  private InternalLuceneIndex getLuceneIndex(final Cache cache, final Region<?, ?> region,
      final String indexName) {
    LuceneService service = LuceneServiceProvider.get(cache);
    InternalLuceneIndex index;
    try {
      index = (InternalLuceneIndex) service.getIndex(indexName, region.getFullPath());
      if (index == null) {
        while (service instanceof LuceneServiceImpl && (((LuceneServiceImpl) service)
            .getDefinedIndex(indexName, region.getFullPath()) != null)) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
          }
          cache.getCancelCriterion().checkCancelInProgress(null);
        }
        index = (InternalLuceneIndex) service.getIndex(indexName, region.getFullPath());
      }
    } catch (CacheClosedException e) {
      throw new InternalFunctionInvocationTargetException(
          "Cache is closed when attempting to retrieve index:" + region.getFullPath(), e);
    }
    return index;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singletonList(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, regionName));
  }

  private static final class RawListResultCollector implements IndexResultCollector {
    private final String name;
    private final List<EntryScore<Object>> hits;

    RawListResultCollector(String name, int initialCapacity) {
      this.name = name;
      this.hits = new ArrayList<>(Math.min(initialCapacity, 256));
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void collect(Object key, float score) {
      hits.add(new EntryScore<>(key, score));
    }

    @Override
    public int size() {
      return hits.size();
    }

    List<EntryScore<Object>> getHits() {
      return hits;
    }
  }
}
