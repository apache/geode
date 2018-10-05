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

import static org.apache.geode.internal.DataSerializableFixedID.CREATE_REGION_MESSAGE_LUCENE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.AlreadyClosedException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneIndexDestroyedException;
import org.apache.geode.cache.lucene.LuceneIndexExistsException;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.directory.DumpDirectoryFiles;
import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.cache.lucene.internal.distributed.IndexingInProgressFunction;
import org.apache.geode.cache.lucene.internal.distributed.LuceneFunctionContext;
import org.apache.geode.cache.lucene.internal.distributed.LuceneQueryFunction;
import org.apache.geode.cache.lucene.internal.distributed.TopEntries;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesCollector;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesCollectorManager;
import org.apache.geode.cache.lucene.internal.distributed.WaitUntilFlushedFunction;
import org.apache.geode.cache.lucene.internal.distributed.WaitUntilFlushedFunctionContext;
import org.apache.geode.cache.lucene.internal.filesystem.ChunkKey;
import org.apache.geode.cache.lucene.internal.filesystem.File;
import org.apache.geode.cache.lucene.internal.management.LuceneServiceMBean;
import org.apache.geode.cache.lucene.internal.management.ManagementIndexListener;
import org.apache.geode.cache.lucene.internal.results.LuceneGetPageFunction;
import org.apache.geode.cache.lucene.internal.results.PageResults;
import org.apache.geode.cache.lucene.internal.xml.LuceneServiceXmlGenerator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.cache.RegionListener;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

/**
 * Implementation of LuceneService to create lucene index and query.
 *
 *
 * @since GemFire 8.5
 */
public class LuceneServiceImpl implements InternalLuceneService {
  public static LuceneIndexImplFactory luceneIndexFactory = new LuceneIndexImplFactory();
  private static final Logger logger = LogService.getLogger();

  private InternalCache cache;
  private final Map<String, LuceneIndex> indexMap = new ConcurrentHashMap<>();
  private final Map<String, LuceneIndexCreationProfile> definedIndexMap = new ConcurrentHashMap<>();
  private IndexListener managementListener;
  public static boolean LUCENE_REINDEX =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "luceneReindex");

  public LuceneServiceImpl() {}

  @Override
  public org.apache.geode.cache.lucene.LuceneIndexFactory createIndexFactory() {
    return new LuceneIndexFactoryImpl(this);
  }

  @Override
  public Cache getCache() {
    return this.cache;
  }

  public void init(final Cache cache) {
    if (cache == null) {
      throw new IllegalStateException("cache is null");
    }
    cache.getCancelCriterion().checkCancelInProgress(null);

    this.cache = (InternalCache) cache;

    FunctionService.registerFunction(new LuceneQueryFunction());
    FunctionService.registerFunction(new LuceneGetPageFunction());
    FunctionService.registerFunction(new WaitUntilFlushedFunction());
    FunctionService.registerFunction(new IndexingInProgressFunction());
    FunctionService.registerFunction(new DumpDirectoryFiles());
    registerDataSerializables();
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    LuceneServiceMBean mbean = new LuceneServiceMBean(this);
    this.managementListener = new ManagementIndexListener(mbean);
    return mbean;
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return InternalLuceneService.class;
  }

  public void beforeRegionDestroyed(Region region) {
    List<LuceneIndex> indexes = getIndexes(region.getFullPath());
    if (!indexes.isEmpty()) {
      String indexNames = indexes.stream().map(i -> i.getName()).collect(Collectors.joining(","));
      throw new IllegalStateException(
          String.format(
              "Region %s cannot be destroyed because it defines Lucene index(es) [%s]. Destroy all Lucene indexes before destroying the region.",
              region.getFullPath(), indexNames));
    }
  }

  public void cleanupFailedInitialization(Region region) {
    List<LuceneIndexCreationProfile> definedIndexes = getDefinedIndexes(region.getFullPath());
    for (LuceneIndexCreationProfile definedIndex : definedIndexes) {
      // Get the AsyncEventQueue
      String aeqId = LuceneServiceImpl.getUniqueIndexName(definedIndex.getIndexName(),
          definedIndex.getRegionPath());
      AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
      // Stop and remove the AsyncEventQueue if it exists
      if (aeq != null) {
        aeq.stop();
        this.cache.removeAsyncEventQueue(aeq);
      }
    }
  }

  public static String getUniqueIndexName(String indexName, String regionPath) {
    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }
    return indexName + "#" + regionPath.replace('/', '_');
  }

  public static String getUniqueIndexRegionName(String indexName, String regionPath,
      String regionSuffix) {
    return getUniqueIndexName(indexName, regionPath) + regionSuffix;
  }

  public void createIndex(String indexName, String regionPath, Map<String, Analyzer> fieldAnalyzers,
      LuceneSerializer serializer, boolean allowOnExistingRegion) {
    if (fieldAnalyzers == null || fieldAnalyzers.isEmpty()) {
      throw new IllegalArgumentException("At least one field must be indexed");
    }
    Analyzer analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), fieldAnalyzers);
    Set<String> fieldsSet = fieldAnalyzers.keySet();
    String[] fields = fieldsSet.toArray(new String[fieldsSet.size()]);

    createIndex(indexName, regionPath, analyzer, fieldAnalyzers, serializer, allowOnExistingRegion,
        fields);
  }

  public void createIndex(final String indexName, String regionPath, final Analyzer analyzer,
      final Map<String, Analyzer> fieldAnalyzers, final LuceneSerializer serializer,
      boolean allowOnExistingRegion, final String... fields) {

    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }

    // We must always register the index (this is where IndexAlreadyExistsException is detected)
    registerDefinedIndex(indexName, regionPath, new LuceneIndexCreationProfile(indexName,
        regionPath, fields, analyzer, fieldAnalyzers, serializer));
    try {
      // If the region does not yet exist, install LuceneRegionListener and return
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionPath);
      if (region == null) {
        LuceneRegionListener regionListener = new LuceneRegionListener(this, cache, indexName,
            regionPath, fields, analyzer, fieldAnalyzers, serializer);
        cache.addRegionListener(regionListener);
        return;
      } else if (allowOnExistingRegion) {
        validateAllMembersAreTheSameVersion(region);
      }

      if (!allowOnExistingRegion) {
        definedIndexMap.remove(LuceneServiceImpl.getUniqueIndexName(indexName, regionPath));
        throw new IllegalStateException("The lucene index must be created before region");
      }

      // do work normally handled by LuceneRegionListener (if region already exists)
      createIndexOnExistingRegion(region, indexName, regionPath, fields, analyzer, fieldAnalyzers,
          serializer);
    } catch (Exception exception) {
      definedIndexMap.remove(LuceneServiceImpl.getUniqueIndexName(indexName, regionPath));
      throw exception;
    }
  }

  protected void validateAllMembersAreTheSameVersion(PartitionedRegion region) {
    Set<InternalDistributedMember> remoteMembers = region.getRegionAdvisor().adviseAllPRNodes();
    Version localVersion =
        cache.getDistributionManager().getDistributionManagerId().getVersionObject();
    if (!remoteMembers.isEmpty()) {
      for (InternalDistributedMember remoteMember : remoteMembers) {
        if (!remoteMember.getVersionObject().equals(localVersion)) {
          throw new IllegalStateException(
              "The lucene index cannot be created on a existing region if all members hosting the region : "
                  + region.getFullPath() + ", are not the same Apache Geode version ");
        }
      }
    }
  }

  private void createIndexOnExistingRegion(PartitionedRegion region, String indexName,
      String regionPath, String[] fields, Analyzer analyzer, Map<String, Analyzer> fieldAnalyzers,
      LuceneSerializer serializer) {
    validateRegionAttributes(region.getAttributes());

    LuceneIndexCreationProfile luceneIndexCreationProfile = new LuceneIndexCreationProfile(
        indexName, regionPath, fields, analyzer, fieldAnalyzers, serializer);

    Runnable validateIndexProfile =
        getIndexValidationRunnable(region, indexName, luceneIndexCreationProfile);
    region.executeSynchronizedOperationOnCacheProfiles(validateIndexProfile);

    String aeqId = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath);
    region.updatePRConfigWithNewGatewaySender(aeqId);
    LuceneIndexImpl luceneIndex = beforeDataRegionCreated(indexName, regionPath,
        region.getAttributes(), analyzer, fieldAnalyzers, aeqId, serializer, fields);

    try {
      afterDataRegionCreated(luceneIndex);
    } catch (LuceneIndexDestroyedException e) {
      logger.warn(String.format("Lucene index %s on region %s was destroyed while being created",
          indexName, regionPath));
      return;
    }

    createLuceneIndexOnDataRegion(region, luceneIndex);
  }

  private Runnable getIndexValidationRunnable(PartitionedRegion region, String indexName,
      LuceneIndexCreationProfile luceneIndexCreationProfile) {
    return () -> {
      region.addCacheServiceProfile(luceneIndexCreationProfile);
      try {
        validateLuceneIndexProfile(region);
      } catch (IllegalStateException e) {
        region.removeCacheServiceProfile(luceneIndexCreationProfile.getId());
        throw new UnsupportedOperationException(
            String.format(
                "Lucene index %s cannot be created because its parameters are incompatible with another Lucene index",
                indexName),
            e);
      }
    };
  }

  protected void validateLuceneIndexProfile(PartitionedRegion region) {
    new CreateRegionProcessorForLucene(region).initializeRegion();
  }

  protected boolean createLuceneIndexOnDataRegion(final PartitionedRegion userRegion,
      final InternalLuceneIndex luceneIndex) {
    try {
      if (userRegion.getDataStore() == null) {
        return true;
      }
      PartitionedRepositoryManager repositoryManager =
          (PartitionedRepositoryManager) luceneIndex.getRepositoryManager();
      Set<Integer> primaryBucketIds = userRegion.getDataStore().getAllLocalPrimaryBucketIds();
      Iterator primaryBucketIterator = primaryBucketIds.iterator();
      while (primaryBucketIterator.hasNext()) {
        int primaryBucketId = (Integer) primaryBucketIterator.next();
        try {
          BucketRegion userBucket = userRegion.getDataStore().getLocalBucketById(primaryBucketId);
          if (userBucket == null) {
            throw new BucketNotFoundException(
                "Bucket ID : " + primaryBucketId + " not found during lucene indexing");
          }
          /**
           *
           * Calling getRepository will in turn call computeRepository
           * which is responsible for indexing the user region.
           *
           **/
          repositoryManager.getRepository(primaryBucketId);
        } catch (BucketNotFoundException | PrimaryBucketException e) {
          logger.debug("Bucket ID : " + primaryBucketId
              + " not found while saving to lucene index: " + e.getMessage(), e);
        }
      }
      return true;
    } catch (RegionDestroyedException e) {
      logger.debug("Bucket not found while saving to lucene index: " + e.getMessage(), e);
      return false;
    } catch (CacheClosedException e) {
      logger.debug("Unable to save to lucene index, cache has been closed", e);
      return false;
    } catch (AlreadyClosedException e) {
      logger.debug("Unable to commit, the lucene index is already closed", e);
      return false;
    }
  }

  static void validateRegionAttributes(RegionAttributes attrs) {
    if (!attrs.getDataPolicy().withPartitioning()) {
      // replicated region
      throw new UnsupportedOperationException(
          "Lucene indexes on replicated regions are not supported");
    }

    // For now we cannot support eviction with local destroy.
    // Eviction with overflow to disk still needs to be supported
    EvictionAttributes evictionAttributes = attrs.getEvictionAttributes();
    EvictionAlgorithm evictionAlgorithm = evictionAttributes.getAlgorithm();
    if (evictionAlgorithm != EvictionAlgorithm.NONE
        && evictionAttributes.getAction().isLocalDestroy()) {
      throw new UnsupportedOperationException(
          "Lucene indexes on regions with eviction and action local destroy are not supported");
    }
  }

  /**
   * Finish creating the lucene index after the data region is created .
   *
   * Public because this is called by the Xml parsing code
   */
  public void afterDataRegionCreated(InternalLuceneIndex index) {
    index.initialize();

    if (this.managementListener != null) {
      this.managementListener.afterIndexCreated(index);
    }

    String aeqId = LuceneServiceImpl.getUniqueIndexName(index.getName(), index.getRegionPath());

    ((LuceneIndexImpl) index).getDataRegion().addAsyncEventQueueId(aeqId, true);
    PartitionedRepositoryManager repositoryManager =
        (PartitionedRepositoryManager) index.getRepositoryManager();
    repositoryManager.allowRepositoryComputation();
    registerIndex(index);
  }

  public LuceneIndexImpl beforeDataRegionCreated(final String indexName, final String regionPath,
      RegionAttributes attributes, final Analyzer analyzer,
      final Map<String, Analyzer> fieldAnalyzers, String aeqId, final LuceneSerializer serializer,
      final String... fields) {
    LuceneIndexImpl index = createIndexObject(indexName, regionPath);
    index.setSearchableFields(fields);
    index.setAnalyzer(analyzer);
    index.setFieldAnalyzers(fieldAnalyzers);
    index.setLuceneSerializer(serializer);
    index.setupRepositoryManager(serializer);
    index.createAEQ(attributes, aeqId);
    return index;

  }

  private LuceneIndexImpl createIndexObject(String indexName, String regionPath) {
    return luceneIndexFactory.create(indexName, regionPath, cache);
  }

  private void registerDefinedIndex(final String indexName, final String regionPath,
      final LuceneIndexCreationProfile luceneIndexCreationProfile) {
    String regionAndIndex = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath);
    if (definedIndexMap.containsKey(regionAndIndex) || indexMap.containsKey(regionAndIndex)) {
      throw new LuceneIndexExistsException(indexName, regionPath);
    }
    definedIndexMap.put(regionAndIndex, luceneIndexCreationProfile);
  }

  @Override
  public LuceneIndex getIndex(String indexName, String regionPath) {
    Region region = cache.getRegion(regionPath);
    if (region == null) {
      return null;
    }
    return indexMap.get(getUniqueIndexName(indexName, region.getFullPath()));
  }

  @Override
  public Collection<LuceneIndex> getAllIndexes() {
    return indexMap.values();
  }

  public List<LuceneIndex> getIndexes(String regionPath) {
    List<LuceneIndex> indexes = new ArrayList();
    for (LuceneIndex index : getAllIndexes()) {
      if (index.getRegionPath().equals(regionPath)) {
        indexes.add(index);
      }
    }
    return Collections.unmodifiableList(indexes);
  }

  public List<LuceneIndexCreationProfile> getDefinedIndexes(String regionPath) {
    List<LuceneIndexCreationProfile> profiles = new ArrayList();
    for (LuceneIndexCreationProfile profile : getAllDefinedIndexes()) {
      if (profile.getRegionPath().equals(regionPath)) {
        profiles.add(profile);
      }
    }
    return Collections.unmodifiableList(profiles);
  }

  @Override
  public void destroyIndex(String indexName, String regionPath) {
    destroyIndex(indexName, regionPath, true);
  }

  protected void destroyIndex(String indexName, String regionPath, boolean initiator) {
    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }
    LuceneIndexImpl indexImpl = (LuceneIndexImpl) getIndex(indexName, regionPath);
    if (indexImpl == null) {
      destroyDefinedIndex(indexName, regionPath);
    } else {
      indexImpl.destroy(initiator);
      removeFromIndexMap(indexImpl);
      logger.info(String.format("Destroyed Lucene index %s from %s region %s",
          indexName, "initialized", regionPath));
    }
  }

  public void destroyDefinedIndex(String indexName, String regionPath) {
    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }
    String uniqueIndexName = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath);
    if (definedIndexMap.containsKey(uniqueIndexName)) {
      definedIndexMap.remove(uniqueIndexName);
      RegionListener listenerToRemove = getRegionListener(indexName, regionPath);
      if (listenerToRemove != null) {
        cache.removeRegionListener(listenerToRemove);
      }
      logger.info(String.format("Destroyed Lucene index %s from %s region %s",
          indexName, "defined", regionPath));
    } else {
      throw new IllegalArgumentException(
          String.format("Lucene index %s was not found in region %s", indexName,
              regionPath));
    }
  }

  protected RegionListener getRegionListener(String indexName, String regionPath) {
    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }
    RegionListener rl = null;
    for (RegionListener listener : cache.getRegionListeners()) {
      if (listener instanceof LuceneRegionListener) {
        LuceneRegionListener lrl = (LuceneRegionListener) listener;
        if (lrl.getRegionPath().equals(regionPath) && lrl.getIndexName().equals(indexName)) {
          rl = lrl;
          break;
        }
      }
    }
    return rl;
  }

  @Override
  public void destroyIndexes(String regionPath) {
    destroyIndexes(regionPath, true);
  }

  protected void destroyIndexes(String regionPath, boolean initiator) {
    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }
    List<LuceneIndexImpl> indexesToDestroy = new ArrayList<>();
    for (LuceneIndex index : getAllIndexes()) {
      if (index.getRegionPath().equals(regionPath)) {
        LuceneIndexImpl indexImpl = (LuceneIndexImpl) index;
        indexImpl.destroy(initiator);
        indexesToDestroy.add(indexImpl);
      }
    }

    // If list is empty throw an exception; otherwise iterate and destroy the defined index
    if (indexesToDestroy.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("No Lucene indexes were found in region %s",
              regionPath));
    } else {
      for (LuceneIndex index : indexesToDestroy) {
        removeFromIndexMap(index);
        logger.info(String.format("Destroyed Lucene index %s from %s region %s",
            index.getName(), "initialized", regionPath));
      }
    }
  }

  public void destroyDefinedIndexes(String regionPath) {
    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }

    // Iterate the defined indexes to get the ones for the regionPath
    List<LuceneIndexCreationProfile> indexesToDestroy = new ArrayList<>();
    for (Map.Entry<String, LuceneIndexCreationProfile> entry : definedIndexMap.entrySet()) {
      if (entry.getValue().getRegionPath().equals(regionPath)) {
        indexesToDestroy.add(entry.getValue());
      }
    }

    // If list is empty throw an exception; otherwise iterate and destroy the defined index
    if (indexesToDestroy.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("No Lucene indexes were found in region %s",
              regionPath));
    } else {
      for (LuceneIndexCreationProfile profile : indexesToDestroy) {
        destroyDefinedIndex(profile.getIndexName(), profile.getRegionPath());
      }
    }
  }

  private void removeFromIndexMap(LuceneIndex index) {
    indexMap.remove(getUniqueIndexName(index.getName(), index.getRegionPath()));
  }

  @Override
  public LuceneQueryFactory createLuceneQueryFactory() {
    return new LuceneQueryFactoryImpl(cache);
  }

  @Override
  public XmlGenerator<Cache> getXmlGenerator() {
    return new LuceneServiceXmlGenerator();
  }

  @Override
  public void beforeCreate(Extensible<Cache> source, Cache cache) {
    // Nothing to do here.
  }

  @Override
  public void onCreate(Extensible<Cache> source, Extensible<Cache> target) {
    // This is called when CacheCreation (source) is turned into a GemfireCacheImpl (target)
    // nothing to do there.
  }

  private boolean hasIndexBeenDestroyed(String uniqueIndexName) {
    return !definedIndexMap.containsKey(uniqueIndexName);
  }

  private void registerIndex(LuceneIndex index) {
    String regionAndIndex = getUniqueIndexName(index.getName(), index.getRegionPath());
    if (hasIndexBeenDestroyed(regionAndIndex)) {
      ((InternalLuceneIndex) index).destroy(true);
      throw new LuceneIndexDestroyedException(index.getName(), index.getRegionPath());
    } else {
      if (!indexMap.containsKey(regionAndIndex)) {
        indexMap.put(regionAndIndex, index);
      }
      definedIndexMap.remove(regionAndIndex);
    }
  }

  public void unregisterIndex(final String region) {
    if (indexMap.containsKey(region))
      indexMap.remove(region);
  }

  /** Public for test purposes */
  public static void registerDataSerializables() {
    DSFIDFactory.registerDSFID(CREATE_REGION_MESSAGE_LUCENE,
        CreateRegionProcessorForLucene.CreateRegionMessage.class);
    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_CHUNK_KEY, ChunkKey.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_FILE, File.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_FUNCTION_CONTEXT,
        LuceneFunctionContext.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_STRING_QUERY_PROVIDER,
        StringQueryProvider.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_TOP_ENTRIES_COLLECTOR_MANAGER,
        TopEntriesCollectorManager.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_ENTRY_SCORE, EntryScore.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_TOP_ENTRIES, TopEntries.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_TOP_ENTRIES_COLLECTOR,
        TopEntriesCollector.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.WAIT_UNTIL_FLUSHED_FUNCTION_CONTEXT,
        WaitUntilFlushedFunctionContext.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.DESTROY_LUCENE_INDEX_MESSAGE,
        DestroyLuceneIndexMessage.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_PAGE_RESULTS, PageResults.class);

    DSFIDFactory.registerDSFID(DataSerializableFixedID.LUCENE_RESULT_STRUCT,
        LuceneResultStructImpl.class);
  }

  public Collection<LuceneIndexCreationProfile> getAllDefinedIndexes() {
    return definedIndexMap.values();
  }

  public LuceneIndexCreationProfile getDefinedIndex(String indexName, String regionPath) {
    return definedIndexMap.get(getUniqueIndexName(indexName, regionPath));
  }

  public boolean waitUntilFlushed(String indexName, String regionPath, long timeout, TimeUnit unit)
      throws InterruptedException {
    Region dataRegion = this.cache.getRegion(regionPath);
    if (dataRegion == null) {
      logger.info("Data region " + regionPath + " not found");
      return false;
    }

    WaitUntilFlushedFunctionContext context =
        new WaitUntilFlushedFunctionContext(indexName, timeout, unit);
    Execution execution = FunctionService.onRegion(dataRegion);
    ResultCollector rs = execution.setArguments(context).execute(WaitUntilFlushedFunction.ID);
    List<Boolean> results = (List<Boolean>) rs.getResult();
    for (Boolean oneResult : results) {
      if (oneResult == false) {
        return false;
      }
    }
    return true;
  }

  public boolean isIndexingInProgress(String indexName, String regionPath) {
    Region region = this.cache.getRegion(regionPath);
    if (region == null) {
      logger.info("Data region " + regionPath + " not found");
      return false;
    }
    // If it is called from a client then we assume that all servers are already
    // rolled to a version more than or equal to client's
    // hence we don't need to validate the servers.
    if (!cache.isClient()) {
      // Also a check for PartitionedRegion. As we cannot use the same method calls to
      // to get the members hosting the region for RR (future implementation)
      if (region instanceof PartitionedRegion) {
        PartitionedRegion dataRegion = (PartitionedRegion) region;
        // Validate all members are Apache Geode v1.7.0 or above
        Set<InternalDistributedMember> remoteMembers =
            dataRegion.getRegionAdvisor().adviseAllPRNodes();
        if (isAnyRemoteMemberVersionLessThanGeode1_7_0(remoteMembers)) {
          throw new IllegalStateException(
              String.format(
                  "Lucene indexing in progress status cannot be determined if all members hosting the user data region : %s, are not above Apache Geode 1.6.0 version ",
                  regionPath));
        }
      }
    }
    Execution execution = FunctionService.onRegion(region);
    ResultCollector resultCollector =
        execution.setArguments(indexName).execute(IndexingInProgressFunction.ID);
    List<Boolean> results = (List<Boolean>) resultCollector.getResult();
    for (Boolean result : results) {
      if (result == true) {
        return true;
      }
    }
    return false;
  }

  private boolean isAnyRemoteMemberVersionLessThanGeode1_7_0(
      Set<InternalDistributedMember> remoteMembers) {
    for (InternalDistributedMember remoteMember : remoteMembers) {
      if (remoteMember.getVersionObject().ordinal() < Version.GEODE_170.ordinal()) {
        return true;
      }
    }
    return false;
  }
}
