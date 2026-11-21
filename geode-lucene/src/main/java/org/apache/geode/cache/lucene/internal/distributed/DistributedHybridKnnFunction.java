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
import java.util.function.Predicate;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
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
import org.apache.geode.internal.cache.PrimaryBucketException;
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
  private static final int MAX_LUCENE_K = 100_000;

  private static final Logger logger = LogService.getLogger();

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
              bucketId, repo, region.getFullPath(), knnContext, queryVector, userFilterQuery,
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

  private TopEntriesCollector executeOnBucket(int bucketId, IndexRepository repo, String regionPath,
      HybridKnnQueryContext knnContext, float[] queryVector, Query userFilterQuery,
      Predicate<Map<String, Object>> tier0FilterPredicate, Tier0VectorRegistry registry,
      TopEntriesCollectorManager manager) throws IOException {
    int k = knnContext.getK();

    // Theorem 2 ordering: snapshot Tier-0 BEFORE querying Lucene to guarantee zero visibility gap
    // across concurrent dual-epoch swaps and SearcherManager.maybeRefreshBlocking() transitions.
    Tier0VectorDeltaBuffer.Snapshot<Object> tier0Snapshot = null;
    Set<Object> maskedKeys = Collections.emptySet();
    if (registry != null) {
      Tier0VectorDeltaBuffer<Object> buffer =
          registry.getBuffer(regionPath, knnContext.getIndexName(), bucketId);
      if (buffer != null) {
        tier0Snapshot = buffer.snapshot();
        maskedKeys = tier0Snapshot.getMaskedKeys();
      }
    }

    // Build pre-filtered Lucene 9.x KnnFloatVectorQuery
    int luceneK;
    Query effectiveFilterQuery;
    if (maskedKeys.isEmpty()) {
      effectiveFilterQuery = userFilterQuery;
      luceneK = k;
    } else if (maskedKeys.size() <= MAX_INLINE_MASK_TERMS) {
      effectiveFilterQuery = buildMaskedFilterQuery(userFilterQuery, maskedKeys);
      luceneK = k;
    } else {
      effectiveFilterQuery = userFilterQuery;
      luceneK = (int) Math.min((long) k + maskedKeys.size(), MAX_LUCENE_K);
    }

    KnnFloatVectorQuery knnQuery = effectiveFilterQuery == null
        ? new KnnFloatVectorQuery(knnContext.getVectorFieldName(), queryVector, luceneK)
        : new KnnFloatVectorQuery(knnContext.getVectorFieldName(), queryVector, luceneK,
            effectiveFilterQuery);

    RawListResultCollector luceneRawCollector =
        new RawListResultCollector("bucket-" + bucketId, luceneK);
    if (repo != null) {
      repo.query(knnQuery, luceneK, luceneRawCollector);
    }

    // Union Lucene HNSW hits (masked by Tier-0 keys) with exact Tier-0 delta hits
    Map<Object, EntryScore<Object>> mergedByKey = new HashMap<>();
    for (EntryScore<Object> hit : luceneRawCollector.getHits()) {
      if (!maskedKeys.contains(hit.getKey())) {
        mergedByKey.put(hit.getKey(), hit);
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
