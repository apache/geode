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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
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
import org.apache.lucene.store.AlreadyClosedException;

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
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
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
  static final int MAX_OWNERSHIP_FENCE_RETRIES = 16;
  static volatile IntConsumer beforeAcquireSearcherHookForTest;
  private static final int MAX_LUCENE_K = 100_000;
  private static final long MAX_CONCURRENT_FLUSH_WAIT_MILLIS = 500L;
  static volatile long maxConcurrentFlushWaitMillisForTest = MAX_CONCURRENT_FLUSH_WAIT_MILLIS;
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
              cache, region, repoManager, ctx, waitForRepository, bucketId, repo, knnContext,
              queryVector, userFilterQuery, tier0FilterPredicate, registry, manager);
          bucketCollectors.add(bucketCollector);
        }

        mergedResult = manager.reduce(bucketCollectors);
      } finally {
        stats.endQuery(start, mergedResult == null ? 0 : mergedResult.size());
      }
      stats.incNumberOfQueryExecuted();
      resultSender.lastResult(mergedResult);
    } catch (IOException | BucketNotFoundException | CacheClosedException
        | PrimaryBucketException | AlreadyClosedException e) {
      logger.debug("Exception during distributed hybrid k-NN query function", e);
      throw new InternalFunctionInvocationTargetException(e);
    } catch (LuceneIndexCreationInProgressException ex) {
      throw new FunctionException(
          new LuceneQueryException("Lucene Index is not available, currently indexing"));
    }
  }

  private TopEntriesCollector executeOnBucket(Cache cache, Region<?, ?> region,
      RepositoryManager repoManager, RegionFunctionContext ctx, boolean waitForRepository,
      int bucketId, IndexRepository initialRepo, HybridKnnQueryContext knnContext,
      float[] queryVector, Query userFilterQuery,
      Predicate<Map<String, Object>> tier0FilterPredicate, Tier0VectorRegistry registry,
      TopEntriesCollectorManager manager)
      throws IOException, BucketNotFoundException, PrimaryBucketException {
    int k = knnContext.getK();
    String regionPath = region.getFullPath();
    String indexName = knnContext.getIndexName();
    boolean unsupportedFilter =
        userFilterQuery != null && !Tier0MetadataMatcher.isSupported(userFilterQuery);

    // Theorem 2 ordering with per-bucket ownership/generation fencing:
    // Wait for any in-flight primary promotion hydration, capture the monotonic
    // ownershipGeneration, and bind Tier-0 snapshot + Lucene IndexSearcher acquisition to a single
    // consistent primary ownership term and searcherEpoch on BOTH Path A (optimistic + fallback)
    // and Path B (unsupported-filter write barrier). Any demotion or re-promotion before or during
    // searcher acquisition invalidates the attempt and either fails over via PrimaryBucketException
    // (if no longer locally primary) or refreshes repo/buffer/generation and retries.
    IndexRepository repo = initialRepo;
    Tier0VectorDeltaBuffer.Snapshot<Object> tier0Snapshot = null;
    Set<Object> maskedKeys = Collections.emptySet();
    RawListResultCollector luceneRawCollector = null;
    boolean usedInlineMask = false;
    boolean usedOversampleFallback = false;

    boolean cutCompleted = false;
    for (int ownershipAttempt = 0; ownershipAttempt < MAX_OWNERSHIP_FENCE_RETRIES
        && !cutCompleted; ownershipAttempt++) {
      // Unconditionally reset candidate/mask state at the start of every ownership attempt.
      tier0Snapshot = null;
      maskedKeys = Collections.emptySet();
      ensureBucketLocallyPrimary(region, bucketId);
      if (ownershipAttempt > 0 || (repo != null && repo.isClosed())) {
        repo = refreshBucketRepository(repoManager, ctx, waitForRepository, bucketId, repo);
      }

      Tier0VectorDeltaBuffer<Object> pathBBuffer = null;
      final AtomicBoolean pathBWriteReleased = new AtomicBoolean(false);
      final AtomicBoolean ownershipInvalidated = new AtomicBoolean(false);
      final AtomicBoolean retryExhausted = new AtomicBoolean(false);
      Runnable pathBReleaseBarrier = null;
      try {
        Tier0VectorDeltaBuffer<Object> buffer =
            resolveBucketBuffer(registry, regionPath, indexName, bucketId);
        ensureBucketLocallyPrimary(region, bucketId);

        if (unsupportedFilter && buffer != null && repo != null) {
          // Path B (R2 M1): reset live candidates and mask set unconditionally upon entry,
          // strictly before acquiring the write barrier or inspecting the buffer/drain loop.
          tier0Snapshot = null;
          maskedKeys = Collections.emptySet();
          LockedBucketBuffer locked = acquireLatestPathBWriteBarrier(
              region, registry, regionPath, indexName, bucketId);
          pathBBuffer = locked.buffer;
          buffer = pathBBuffer;
          final long expectedGen = locked.ownershipGeneration;
          final Tier0VectorDeltaBuffer<Object> expectedBuffer = buffer;
          final IndexRepository activeRepo = repo;
          if (buffer != null) {
            final Tier0VectorDeltaBuffer<Object> lockedBuffer = buffer;
            pathBReleaseBarrier = () -> {
              if (pathBWriteReleased.compareAndSet(false, true)) {
                lockedBuffer.unlockPathBWrite();
              }
            };
          }
          if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId, expectedGen,
              expectedBuffer, activeRepo)) {
            throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
          }
          if (buffer != null) {
            tier0Snapshot = buffer.snapshot();
            if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId, expectedGen,
                expectedBuffer, activeRepo)) {
              throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
            }
            if (!tier0Snapshot.isEmpty() || buffer.getTotalSize() > 0 || buffer.isFlushing()) {
              // Instead of silently dropping fresh Tier-0 entries via fail-closed evaluation on an
              // unsupported Lucene Query AST (e.g., FuzzyQuery/SpanQuery), synchronously drain BOTH
              // E_flushing and E_active into Lucene + blocking-refresh while holding the write
              // barrier so IndexSearcher evaluates the filter with T_mask empty at the searcher
              // cut (even when > B_max keys are pending or after a prior commit/refresh abort).
              flushBucketForUnsupportedFilter(
                  cache, region, regionPath, indexName, bucketId, registry,
                  buffer, activeRepo, expectedGen, ownershipInvalidated, retryExhausted);
              if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                  expectedGen, expectedBuffer, activeRepo)) {
                throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
              }
              tier0Snapshot = buffer.snapshot();
            }
            if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId, expectedGen,
                expectedBuffer, activeRepo)) {
              throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
            }
            maskedKeys = tier0Snapshot.getMaskedKeys();
          } else {
            tier0Snapshot = null;
            maskedKeys = Collections.emptySet();
          }
          PreparedBucketQuery prepared = prepareBucketQuery(
              knnContext, k, queryVector, userFilterQuery, maskedKeys);
          RawListResultCollector attemptCollector =
              new RawListResultCollector("bucket-" + bucketId, prepared.luceneK);
          invokeBeforeAcquireSearcherHookForTest(bucketId);
          final Runnable releaseBarrier = pathBReleaseBarrier;
          final AtomicBoolean postAcquireValidated = new AtomicBoolean(false);
          Runnable postAcquireValidatorAndRelease = () -> {
            try {
              if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                  expectedGen, expectedBuffer, activeRepo)) {
                throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
              }
              postAcquireValidated.set(true);
            } finally {
              if (releaseBarrier != null) {
                releaseBarrier.run();
              }
            }
          };
          executeRepoQuery(activeRepo, prepared, knnContext, queryVector, attemptCollector,
              postAcquireValidatorAndRelease);
          if (!postAcquireValidated.get()
              && !isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                  expectedGen, expectedBuffer, activeRepo)) {
            throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
          }
          luceneRawCollector = attemptCollector;
          usedInlineMask = prepared.usedInlineMask;
          usedOversampleFallback = prepared.usedOversampleFallback;
          cutCompleted = true;
        } else {
          boolean queryCompleted = false;
          for (int attempt = 0; attempt < MAX_OPTIMISTIC_SEARCHER_EPOCH_RETRIES; attempt++) {
            ensureBucketLocallyPrimary(region, bucketId);
            if (registry != null) {
              registry.awaitHydration(regionPath, indexName, bucketId);
              ensureBucketLocallyPrimary(region, bucketId);
            }
            final long expectedGen = registry != null
                ? registry.getOwnershipGeneration(regionPath, indexName, bucketId)
                : 0L;
            if (registry != null && registry.isHydrating(regionPath, indexName, bucketId)) {
              Thread.onSpinWait();
              continue;
            }
            buffer = registry != null ? registry.getBuffer(regionPath, indexName, bucketId) : null;
            if (unsupportedFilter && buffer != null && repo != null) {
              // Buffer appeared concurrently on an unsupported-filter query; switch to Path B.
              throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
            }
            if (repo != null && repo.isClosed()) {
              throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
            }
            final long epoch0 = repo != null ? repo.getSearcherEpoch() : 0L;
            if (repo != null && (epoch0 & 1L) != 0L) {
              Thread.onSpinWait();
              continue;
            }
            final Tier0VectorDeltaBuffer<Object> expectedBuffer = buffer;
            final IndexRepository activeRepo = repo;
            if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId, expectedGen,
                expectedBuffer, activeRepo)) {
              throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
            }
            tier0Snapshot = buffer != null ? buffer.snapshot() : null;
            if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId, expectedGen,
                expectedBuffer, activeRepo)) {
              throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
            }
            maskedKeys =
                tier0Snapshot != null ? tier0Snapshot.getMaskedKeys() : Collections.emptySet();
            PreparedBucketQuery prepared = prepareBucketQuery(
                knnContext, k, queryVector, userFilterQuery, maskedKeys);
            RawListResultCollector attemptCollector =
                new RawListResultCollector("bucket-" + bucketId, prepared.luceneK);
            if (activeRepo == null) {
              luceneRawCollector = attemptCollector;
              usedInlineMask = prepared.usedInlineMask;
              usedOversampleFallback = prepared.usedOversampleFallback;
              queryCompleted = true;
              break;
            }
            invokeBeforeAcquireSearcherHookForTest(bucketId);
            final AtomicBoolean postAcquireValidated = new AtomicBoolean(false);
            Runnable postAcquireValidator = () -> {
              if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                  expectedGen, expectedBuffer, activeRepo)
                  || activeRepo.getSearcherEpoch() != epoch0) {
                throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
              }
              postAcquireValidated.set(true);
            };
            if (tryExecuteRepoQueryAtEpoch(activeRepo, epoch0, prepared, knnContext, queryVector,
                attemptCollector, postAcquireValidator)) {
              if (!postAcquireValidated.get()
                  && !isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                      expectedGen, expectedBuffer, activeRepo)) {
                throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
              }
              luceneRawCollector = attemptCollector;
              usedInlineMask = prepared.usedInlineMask;
              usedOversampleFallback = prepared.usedOversampleFallback;
              queryCompleted = true;
              break;
            }
            if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId, expectedGen,
                expectedBuffer, activeRepo)) {
              throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
            }
            Thread.onSpinWait();
          }

          if (!queryCompleted && repo != null) {
            // Path-B write-barrier fallback (R2 M1): unconditionally clear any invalidated
            // snapshot/mask state left over from exhausted optimistic attempts before acquiring
            // the write barrier, even if the buffer is already empty.
            tier0Snapshot = null;
            maskedKeys = Collections.emptySet();
            LockedBucketBuffer locked = acquireLatestPathBWriteBarrier(
                region, registry, regionPath, indexName, bucketId);
            pathBBuffer = locked.buffer;
            buffer = pathBBuffer;
            final long expectedGen = locked.ownershipGeneration;
            final Tier0VectorDeltaBuffer<Object> expectedBuffer = buffer;
            final IndexRepository activeRepo = repo;
            if (buffer != null) {
              final Tier0VectorDeltaBuffer<Object> lockedBuffer = buffer;
              pathBReleaseBarrier = () -> {
                if (pathBWriteReleased.compareAndSet(false, true)) {
                  lockedBuffer.unlockPathBWrite();
                }
              };
            }
            final Runnable releaseBarrier = pathBReleaseBarrier;
            long deadlineNanos =
                System.nanoTime() + maxConcurrentFlushWaitMillisForTest * 1_000_000L;
            while (!queryCompleted) {
              if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                  expectedGen, expectedBuffer, activeRepo)) {
                throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
              }
              long fallbackEpoch = activeRepo.getSearcherEpoch();
              if ((fallbackEpoch & 1L) != 0L) {
                if (System.nanoTime() >= deadlineNanos) {
                  retryExhausted.set(true);
                  throw new PrimaryBucketException("Bucket " + bucketId + " on region "
                      + regionPath + " exhausted searcher-epoch wait while commit in progress");
                }
                Thread.yield();
                continue;
              }
              tier0Snapshot = buffer != null ? buffer.snapshot() : null;
              if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                  expectedGen, expectedBuffer, activeRepo)) {
                throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
              }
              maskedKeys =
                  tier0Snapshot != null ? tier0Snapshot.getMaskedKeys() : Collections.emptySet();
              PreparedBucketQuery prepared = prepareBucketQuery(
                  knnContext, k, queryVector, userFilterQuery, maskedKeys);
              RawListResultCollector attemptCollector =
                  new RawListResultCollector("bucket-" + bucketId, prepared.luceneK);
              final AtomicBoolean postAcquireValidated = new AtomicBoolean(false);
              final long checkedFallbackEpoch = fallbackEpoch;
              Runnable epochPostAcquireValidatorAndRelease = () -> {
                try {
                  if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                      expectedGen, expectedBuffer, activeRepo)
                      || activeRepo.getSearcherEpoch() != checkedFallbackEpoch) {
                    throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
                  }
                  postAcquireValidated.set(true);
                } finally {
                  if (releaseBarrier != null) {
                    releaseBarrier.run();
                  }
                }
              };
              boolean epochMatched = tryExecuteRepoQueryAtEpoch(activeRepo, fallbackEpoch, prepared,
                  knnContext, queryVector, attemptCollector, epochPostAcquireValidatorAndRelease);
              if (epochMatched) {
                if (!postAcquireValidated.get()
                    && !isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                        expectedGen, expectedBuffer, activeRepo)) {
                  throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
                }
                luceneRawCollector = attemptCollector;
                usedInlineMask = prepared.usedInlineMask;
                usedOversampleFallback = prepared.usedOversampleFallback;
                queryCompleted = true;
              } else if ((fallbackEpoch & 1L) == 0L
                  && activeRepo.getSearcherEpoch() == fallbackEpoch) {
                // Repository is a non-epoch-tracking implementation (e.g. unstubbed mock): run
                // once under the write barrier while validating the ownership cut.
                final AtomicBoolean fallbackPostAcquireValidated = new AtomicBoolean(false);
                Runnable fallbackPostAcquireValidatorAndRelease = () -> {
                  try {
                    if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                        expectedGen, expectedBuffer, activeRepo)) {
                      throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
                    }
                    fallbackPostAcquireValidated.set(true);
                  } finally {
                    if (releaseBarrier != null) {
                      releaseBarrier.run();
                    }
                  }
                };
                RawListResultCollector finalCollector =
                    new RawListResultCollector("bucket-" + bucketId, prepared.luceneK);
                executeRepoQuery(activeRepo, prepared, knnContext, queryVector, finalCollector,
                    fallbackPostAcquireValidatorAndRelease);
                if (!fallbackPostAcquireValidated.get()
                    && !isOwnershipCutValid(region, registry, regionPath, indexName, bucketId,
                        expectedGen, expectedBuffer, activeRepo)) {
                  throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
                }
                luceneRawCollector = finalCollector;
                usedInlineMask = prepared.usedInlineMask;
                usedOversampleFallback = prepared.usedOversampleFallback;
                queryCompleted = true;
              } else if (System.nanoTime() >= deadlineNanos) {
                retryExhausted.set(true);
                throw new PrimaryBucketException("Bucket " + bucketId + " on region " + regionPath
                    + " exhausted searcher-epoch contention wait without a quiescent cut");
              } else {
                Thread.yield();
              }
            }
          }
          cutCompleted = queryCompleted;
        }
      } catch (PrimaryBucketException | AlreadyClosedException ownershipChanged) {
        if (retryExhausted.get()) {
          throw ownershipChanged;
        }
        if (registry != null && (ownershipInvalidated.get()
            || ownershipChanged instanceof AlreadyClosedException
            || isBucketLocallyPrimary(region, bucketId))) {
          registry.incrementOwnershipFenceRetryCount();
        }
        ensureBucketLocallyPrimary(region, bucketId);
        if (ownershipAttempt + 1 >= MAX_OWNERSHIP_FENCE_RETRIES) {
          throw new PrimaryBucketException("Bucket " + bucketId + " on region " + regionPath
              + " ownership generation changed repeatedly during hybrid k-NN query");
        }
        Thread.onSpinWait();
      } finally {
        if (pathBReleaseBarrier != null) {
          pathBReleaseBarrier.run();
        } else if (pathBBuffer != null && pathBWriteReleased.compareAndSet(false, true)) {
          pathBBuffer.unlockPathBWrite();
        }
      }
    }

    if (!cutCompleted) {
      throw new PrimaryBucketException("Bucket " + bucketId + " on region " + regionPath
          + " failed to establish a validated hybrid k-NN cut after "
          + MAX_OWNERSHIP_FENCE_RETRIES + " attempts");
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

  private static final class LockedBucketBuffer {
    final Tier0VectorDeltaBuffer<Object> buffer;
    final long ownershipGeneration;

    LockedBucketBuffer(Tier0VectorDeltaBuffer<Object> buffer, long ownershipGeneration) {
      this.buffer = buffer;
      this.ownershipGeneration = ownershipGeneration;
    }
  }

  private static Tier0VectorDeltaBuffer<Object> resolveBucketBuffer(
      Tier0VectorRegistry registry, String regionPath, String indexName, int bucketId) {
    if (registry == null) {
      return null;
    }
    registry.awaitHydration(regionPath, indexName, bucketId);
    return registry.getBuffer(regionPath, indexName, bucketId);
  }

  private static boolean isBucketLocallyPrimary(Region<?, ?> region, int bucketId) {
    if (!(region instanceof PartitionedRegion)) {
      return true;
    }
    PartitionedRegion pr = (PartitionedRegion) region;
    if (pr.isDestroyed()) {
      return false;
    }
    PartitionedRegionDataStore dataStore = pr.getDataStore();
    if (dataStore == null) {
      return true;
    }
    BucketRegion bucket = dataStore.getLocalBucketById(bucketId);
    if (bucket == null || bucket.isDestroyed()) {
      return false;
    }
    if (bucket.getBucketAdvisor() != null) {
      return bucket.getBucketAdvisor().isPrimary();
    }
    return true;
  }

  private static void ensureBucketLocallyPrimary(Region<?, ?> region, int bucketId)
      throws PrimaryBucketException {
    if (!isBucketLocallyPrimary(region, bucketId)) {
      throw new PrimaryBucketException(
          "Bucket " + bucketId + " on region " + region.getFullPath() + " is no longer primary");
    }
  }

  private static boolean isOwnershipCutValid(Region<?, ?> region, Tier0VectorRegistry registry,
      String regionPath, String indexName, int bucketId, long expectedGen,
      Tier0VectorDeltaBuffer<Object> expectedBuffer, IndexRepository repo) {
    if (!isBucketLocallyPrimary(region, bucketId)) {
      return false;
    }
    if (repo != null && repo.isClosed()) {
      return false;
    }
    if (registry != null) {
      if (registry.getOwnershipGeneration(regionPath, indexName, bucketId) != expectedGen) {
        return false;
      }
      if (registry.isHydrating(regionPath, indexName, bucketId)) {
        return false;
      }
      if (registry.getBuffer(regionPath, indexName, bucketId) != expectedBuffer) {
        return false;
      }
    }
    return true;
  }

  private static PrimaryBucketException ownershipChangedSignal(
      AtomicBoolean ownershipInvalidated, String regionPath, int bucketId) {
    ownershipInvalidated.set(true);
    return new PrimaryBucketException("Bucket " + bucketId + " on region " + regionPath
        + " ownership generation changed during hybrid k-NN query");
  }

  private static void invokeBeforeAcquireSearcherHookForTest(int bucketId) {
    IntConsumer hook = beforeAcquireSearcherHookForTest;
    if (hook != null) {
      hook.accept(bucketId);
    }
  }

  private IndexRepository refreshBucketRepository(RepositoryManager repoManager,
      RegionFunctionContext ctx, boolean waitForRepository, int bucketId,
      IndexRepository currentRepo) throws BucketNotFoundException {
    if (repoManager == null) {
      return currentRepo;
    }
    Map<Integer, IndexRepository> repos =
        resolveRepositoriesByBucket(repoManager, ctx, waitForRepository);
    if (repos != null && repos.containsKey(bucketId)) {
      return repos.get(bucketId);
    }
    return currentRepo;
  }

  private static LockedBucketBuffer acquireLatestPathBWriteBarrier(Region<?, ?> region,
      Tier0VectorRegistry registry, String regionPath, String indexName, int bucketId)
      throws PrimaryBucketException {
    while (true) {
      ensureBucketLocallyPrimary(region, bucketId);
      if (registry == null) {
        return new LockedBucketBuffer(null, 0L);
      }
      registry.awaitHydration(regionPath, indexName, bucketId);
      ensureBucketLocallyPrimary(region, bucketId);
      long expectedGen = registry.getOwnershipGeneration(regionPath, indexName, bucketId);
      if (registry.isHydrating(regionPath, indexName, bucketId)) {
        continue;
      }
      Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(regionPath, indexName, bucketId);
      if (buffer == null) {
        if (isBucketLocallyPrimary(region, bucketId)
            && registry.getOwnershipGeneration(regionPath, indexName, bucketId) == expectedGen
            && !registry.isHydrating(regionPath, indexName, bucketId)) {
          return new LockedBucketBuffer(null, expectedGen);
        }
        continue;
      }
      buffer.lockPathBWrite();
      if (isBucketLocallyPrimary(region, bucketId)
          && registry.getOwnershipGeneration(regionPath, indexName, bucketId) == expectedGen
          && !registry.isHydrating(regionPath, indexName, bucketId)
          && registry.getBuffer(regionPath, indexName, bucketId) == buffer) {
        return new LockedBucketBuffer(buffer, expectedGen);
      }
      buffer.unlockPathBWrite();
    }
  }

  private static boolean tryExecuteRepoQueryAtEpoch(IndexRepository repo, long expectedEpoch,
      PreparedBucketQuery prepared, HybridKnnQueryContext knnContext, float[] queryVector,
      RawListResultCollector collector, Runnable onSearcherAcquired) throws IOException {
    if (prepared.executeFp32Rerank) {
      return repo.tryQueryWithFp32RerankAtEpoch(expectedEpoch, prepared.knnQuery,
          prepared.luceneK, prepared.queryK, knnContext.getVectorFieldName(), queryVector,
          knnContext.getSimilarityFunction(), collector, onSearcherAcquired);
    }
    return repo.tryQueryAtEpoch(
        expectedEpoch, prepared.knnQuery, prepared.luceneK, collector, onSearcherAcquired);
  }

  private static void executeRepoQuery(IndexRepository repo, PreparedBucketQuery prepared,
      HybridKnnQueryContext knnContext, float[] queryVector, RawListResultCollector collector,
      Runnable onSearcherAcquired) throws IOException {
    if (prepared.executeFp32Rerank) {
      repo.queryWithFp32Rerank(prepared.knnQuery, prepared.luceneK, prepared.queryK,
          knnContext.getVectorFieldName(), queryVector, knnContext.getSimilarityFunction(),
          collector, onSearcherAcquired);
    } else {
      repo.query(prepared.knnQuery, prepared.luceneK, collector, onSearcherAcquired);
    }
  }

  private void flushBucketForUnsupportedFilter(Cache cache, Region<?, ?> region, String regionPath,
      String indexName, int bucketId, Tier0VectorRegistry registry,
      Tier0VectorDeltaBuffer<Object> buffer, IndexRepository repo, long expectedGen,
      AtomicBoolean ownershipInvalidated, AtomicBoolean retryExhausted)
      throws IOException, PrimaryBucketException {
    long deadlineNanos = System.nanoTime() + maxConcurrentFlushWaitMillisForTest * 1_000_000L;
    boolean flushedOrRefreshed = false;
    while (true) {
      if (!isOwnershipCutValid(region, registry, regionPath, indexName, bucketId, expectedGen,
          buffer, repo)) {
        throw ownershipChangedSignal(ownershipInvalidated, regionPath, bucketId);
      }
      try {
        boolean flushed = registry.flushBuffer(buffer, repo, entry -> {
          // Drain strictly the immutable payload already captured inside FlushBatch<K> when
          // present (never reading live BucketRegion or acquiring RegionEntry locks under
          // pathBWrite), falling back to readRegionValueForFlush only for legacy metadata-only
          // unit-test puts where regionValue was omitted.
          Object captured = entry.getRegionValue();
          if (captured != null) {
            return (captured instanceof CachedDeserializable)
                ? ((CachedDeserializable) captured).getDeserializedValue(null, null)
                : captured;
          }
          return readRegionValueForFlush(cache, region, bucketId, entry.getKey());
        }, !flushedOrRefreshed);
        flushedOrRefreshed = true;
        // Loop until BOTH E_active and E_flushing are completely empty so T_mask = empty before
        // acquiring IndexSearcher, even when > B_max keys are buffered or after a prior abort.
        if (buffer.getTotalSize() == 0 && !buffer.isFlushing()) {
          break;
        }
        if (System.nanoTime() >= deadlineNanos) {
          retryExhausted.set(true);
          throw new PrimaryBucketException("Bucket " + bucketId + " on region " + regionPath
              + " exhausted Path-B drain wait before Tier-0 emptied (remaining="
              + buffer.getTotalSize() + ")");
        }
        if (!flushed) {
          Thread.onSpinWait();
        }
      } catch (IllegalStateException flushAlreadyInProgress) {
        boolean inProgressMsg = flushAlreadyInProgress.getMessage() != null
            && flushAlreadyInProgress.getMessage().contains("Flush already in progress");
        boolean flushInProgress = buffer.isFlushing() || buffer.isFlushLocked();
        if (!inProgressMsg && !flushInProgress) {
          throw flushAlreadyInProgress;
        }
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
          retryExhausted.set(true);
          throw new PrimaryBucketException("Bucket " + bucketId + " on region " + regionPath
              + " exhausted Path-B concurrent flush wait without draining Tier-0");
        }
        if (!flushInProgress) {
          // Concurrent flush completed while we were racing to acquire flushLock/beginFlush.
          continue;
        }
        boolean heldPathB = buffer.isPathBWriteHeldByCurrentThread();
        if (heldPathB) {
          // Temporarily drop pathBWrite so the active flusher holding flushLock (or completing
          // E_flushing) can acquire pathBBarrierLock.readLock() cleanly without AB-BA deadlock.
          buffer.unlockPathBWrite();
        }
        try {
          if (buffer.isFlushLocked()) {
            ReentrantLock flushLock = buffer.getFlushLock();
            long waitNanos = Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(10L));
            if (flushLock.tryLock(waitNanos, TimeUnit.NANOSECONDS)) {
              flushLock.unlock();
            }
          } else {
            Thread.sleep(1L);
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          retryExhausted.set(true);
          throw new PrimaryBucketException("Bucket " + bucketId + " on region " + regionPath
              + " interrupted while waiting for in-flight flush on Path-B");
        } finally {
          if (heldPathB) {
            buffer.lockPathBWrite();
          }
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
