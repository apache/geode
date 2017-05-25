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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.geode.cache.lucene.LuceneIndexExistsException;
import org.apache.geode.cache.lucene.internal.distributed.LuceneQueryFunction;
import org.apache.geode.cache.lucene.internal.management.LuceneServiceMBean;
import org.apache.geode.cache.lucene.internal.management.ManagementIndexListener;
import org.apache.geode.cache.lucene.internal.results.LuceneGetPageFunction;
import org.apache.geode.cache.lucene.internal.results.PageResults;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.internal.directory.DumpDirectoryFiles;
import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.cache.lucene.internal.distributed.LuceneFunctionContext;
import org.apache.geode.cache.lucene.internal.distributed.TopEntries;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesCollector;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesCollectorManager;
import org.apache.geode.cache.lucene.internal.distributed.WaitUntilFlushedFunction;
import org.apache.geode.cache.lucene.internal.distributed.WaitUntilFlushedFunctionContext;
import org.apache.geode.cache.lucene.internal.filesystem.ChunkKey;
import org.apache.geode.cache.lucene.internal.filesystem.File;
import org.apache.geode.cache.lucene.internal.xml.LuceneServiceXmlGenerator;
import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.RegionListener;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;

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
  private final HashMap<String, LuceneIndex> indexMap = new HashMap<String, LuceneIndex>();
  private final HashMap<String, LuceneIndexCreationProfile> definedIndexMap = new HashMap<>();
  private IndexListener managementListener;

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
      throw new IllegalStateException(LocalizedStrings.CqService_CACHE_IS_NULL.toLocalizedString());
    }
    cache.getCancelCriterion().checkCancelInProgress(null);

    this.cache = (InternalCache) cache;

    FunctionService.registerFunction(new LuceneQueryFunction());
    FunctionService.registerFunction(new LuceneGetPageFunction());
    FunctionService.registerFunction(new WaitUntilFlushedFunction());
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

  public static String getUniqueIndexName(String indexName, String regionPath) {
    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }
    String name = indexName + "#" + regionPath.replace('/', '_');
    return name;
  }

  public static String getUniqueIndexRegionName(String indexName, String regionPath,
      String regionSuffix) {
    return getUniqueIndexName(indexName, regionPath) + regionSuffix;
  }

  public enum validateCommandParameters {
    REGION_PATH, INDEX_NAME;

    public void validateName(String name) {
      if (name == null) {
        throw new IllegalArgumentException(
            LocalizedStrings.LocalRegion_NAME_CANNOT_BE_NULL.toLocalizedString());
      }
      if (name.isEmpty()) {
        throw new IllegalArgumentException(
            LocalizedStrings.LocalRegion_NAME_CANNOT_BE_EMPTY.toLocalizedString());
      }

      boolean iae = false;
      String msg =
          " names may only be alphanumeric, must not begin with double-underscores, but can contain hyphens";
      Matcher matcher = null;
      switch (this) {
        case REGION_PATH:
          matcher = Pattern.compile("[aA-zZ0-9-_./]+").matcher(name);
          msg = "Region" + msg + ", underscores, or forward slashes: ";
          iae = name.startsWith("__") || !matcher.matches();
          break;
        case INDEX_NAME:
          matcher = Pattern.compile("[aA-zZ0-9-_.]+").matcher(name);
          msg = "Index" + msg + " or underscores: ";
          iae = name.startsWith("__") || !matcher.matches();
          break;
        default:
          throw new IllegalArgumentException("Illegal option for validateName function");
      }

      // Ensure the region only contains valid characters
      if (iae) {
        throw new IllegalArgumentException(msg + name);
      }
    }
  }

  public void createIndex(String indexName, String regionPath,
      Map<String, Analyzer> fieldAnalyzers) {
    if (fieldAnalyzers == null || fieldAnalyzers.isEmpty()) {
      throw new IllegalArgumentException("At least one field must be indexed");
    }
    Analyzer analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), fieldAnalyzers);
    Set<String> fieldsSet = fieldAnalyzers.keySet();
    String[] fields = (String[]) fieldsSet.toArray(new String[fieldsSet.size()]);

    createIndex(indexName, regionPath, analyzer, fieldAnalyzers, fields);
  }

  public void createIndex(final String indexName, String regionPath, final Analyzer analyzer,
      final Map<String, Analyzer> fieldAnalyzers, final String... fields) {

    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }

    registerDefinedIndex(indexName, regionPath,
        new LuceneIndexCreationProfile(indexName, regionPath, fields, analyzer, fieldAnalyzers));

    Region region = cache.getRegion(regionPath);
    if (region != null) {
      definedIndexMap.remove(LuceneServiceImpl.getUniqueIndexName(indexName, regionPath));
      throw new IllegalStateException("The lucene index must be created before region");
    }

    cache.addRegionListener(new LuceneRegionListener(this, cache, indexName, regionPath, fields,
        analyzer, fieldAnalyzers));
  }

  /**
   * Finish creating the lucene index after the data region is created .
   * 
   * Public because this is called by the Xml parsing code
   */
  public void afterDataRegionCreated(LuceneIndexImpl index) {
    index.initialize();
    registerIndex(index);
    if (this.managementListener != null) {
      this.managementListener.afterIndexCreated(index);
    }

  }

  public LuceneIndexImpl beforeDataRegionCreated(final String indexName, final String regionPath,
      RegionAttributes attributes, final Analyzer analyzer,
      final Map<String, Analyzer> fieldAnalyzers, String aeqId, final String... fields) {
    LuceneIndexImpl index = createIndexObject(indexName, regionPath);
    index.setSearchableFields(fields);
    index.setAnalyzer(analyzer);
    index.setFieldAnalyzers(fieldAnalyzers);
    index.initializeAEQ(attributes, aeqId);
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
      logger.info(LocalizedStrings.LuceneService_DESTROYED_INDEX_0_FROM_1_REGION_2
          .toLocalizedString(indexName, "initialized", regionPath));
    }
  }

  public void destroyDefinedIndex(String indexName, String regionPath) {
    if (!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }
    String uniqueIndexName = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath);
    if (definedIndexMap.containsKey(uniqueIndexName)) {
      definedIndexMap.remove(uniqueIndexName);
      RegionListener listenerToRemove = null;
      for (RegionListener listener : cache.getRegionListeners()) {
        if (listener instanceof LuceneRegionListener) {
          LuceneRegionListener lrl = (LuceneRegionListener) listener;
          if (lrl.getRegionPath().equals(regionPath) && lrl.getIndexName().equals(indexName)) {
            listenerToRemove = lrl;
            break;
          }
        }
      }
      if (listenerToRemove != null) {
        cache.removeRegionListener(listenerToRemove);
      }
      logger.info(LocalizedStrings.LuceneService_DESTROYED_INDEX_0_FROM_1_REGION_2
          .toLocalizedString(indexName, "defined", regionPath));
    } else {
      throw new IllegalArgumentException(
          LocalizedStrings.LuceneService_INDEX_0_NOT_FOUND_IN_REGION_1.toLocalizedString(indexName,
              regionPath));
    }
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
          LocalizedStrings.LuceneService_NO_INDEXES_WERE_FOUND_IN_REGION_0
              .toLocalizedString(regionPath));
    } else {
      for (LuceneIndex index : indexesToDestroy) {
        removeFromIndexMap(index);
        logger.info(LocalizedStrings.LuceneService_DESTROYED_INDEX_0_FROM_1_REGION_2
            .toLocalizedString(index.getName(), "initialized", regionPath));
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
          LocalizedStrings.LuceneService_NO_INDEXES_WERE_FOUND_IN_REGION_0
              .toLocalizedString(regionPath));
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

  public void registerIndex(LuceneIndex index) {
    String regionAndIndex = getUniqueIndexName(index.getName(), index.getRegionPath());
    if (!indexMap.containsKey(regionAndIndex)) {
      indexMap.put(regionAndIndex, index);
    }
    definedIndexMap.remove(regionAndIndex);
  }

  public void unregisterIndex(final String region) {
    if (indexMap.containsKey(region))
      indexMap.remove(region);
  }

  /** Public for test purposes */
  public static void registerDataSerializables() {
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
}
