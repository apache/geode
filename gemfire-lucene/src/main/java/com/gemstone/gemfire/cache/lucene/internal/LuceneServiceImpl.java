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

package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.internal.distributed.EntryScore;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunction;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunctionContext;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntries;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollector;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollectorManager;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.xml.LuceneServiceXmlGenerator;
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.CacheService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionListener;
import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Implementation of LuceneService to create lucene index and query.
 * 
 * @author Xiaojian Zhou
 * 
 * @since 8.5
 */
public class LuceneServiceImpl implements InternalLuceneService {
  private static final Logger logger = LogService.getLogger();
  
  private GemFireCacheImpl cache;
  private final HashMap<String, LuceneIndex> indexMap = new HashMap<String, LuceneIndex>();;
  
  public LuceneServiceImpl() {
    
  }

  public void init(final Cache cache) {
    if (cache == null) {
      throw new IllegalStateException(LocalizedStrings.CqService_CACHE_IS_NULL.toLocalizedString());
    }
    GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
    gfc.getCancelCriterion().checkCancelInProgress(null);

    this.cache = gfc;

    FunctionService.registerFunction(new LuceneFunction());
    registerDataSerializables();
  }
  
  @Override
  public Class<? extends CacheService> getInterface() {
    return InternalLuceneService.class;
  }

  public static String getUniqueIndexName(String indexName, String regionPath) {
    if (!regionPath.startsWith("/")) {
      regionPath = "/"+regionPath;
    }
    String name = indexName + "#" + regionPath.replace('/', '_');
    return name;
  }

  @Override
  public void createIndex(String indexName, String regionPath, String... fields) {
    StandardAnalyzer analyzer = new StandardAnalyzer();
    
    createIndex(indexName, regionPath, analyzer, fields);
  }
  
  @Override
  public void createIndex(String indexName, String regionPath, Map<String, Analyzer> analyzerPerField) {
    Analyzer analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), analyzerPerField);
    String[] fields = (String[])analyzerPerField.keySet().toArray(new String[analyzerPerField.keySet().size()]);

    createIndex(indexName, regionPath, analyzer, fields);
  }

  private void createIndex(final String indexName, String regionPath,
      final Analyzer analyzer, final String... fields) {

    if(!regionPath.startsWith("/")) {
      regionPath = "/" + regionPath;
    }
    Region region = cache.getRegion(regionPath);
    if(region != null) {
      throw new IllegalStateException("The lucene index must be created before region");
    }
    
    final String dataRegionPath = regionPath;
    cache.addRegionListener(new RegionListener() {
      @Override
      public RegionAttributes beforeCreate(Region parent, String regionName,
          RegionAttributes attrs, InternalRegionArguments internalRegionArgs) {
        String path = parent == null ? "/" + regionName : parent.getFullPath() + "/" + regionName;
        if(path.equals(dataRegionPath)) {
          String aeqId = LuceneServiceImpl.getUniqueIndexName(indexName, dataRegionPath);
          AttributesFactory af = new AttributesFactory(attrs);
          af.addAsyncEventQueueId(aeqId);
          return af.create();
        } else {
          return attrs;
        }
      }
      
      @Override
      public void afterCreate(Region region) {
        if(region.getFullPath().equals(dataRegionPath)) {
          afterDataRegionCreated(indexName, analyzer, dataRegionPath, fields);
          cache.removeRegionListener(this);
        }
      }
    });
    
  }
  
  /**
   * Finish creating the lucene index after the data region is created .
   * 
   * Public because this is called by the Xml parsing code
   */
  public void afterDataRegionCreated(final String indexName,
      final Analyzer analyzer, final String dataRegionPath,
      final String... fields) {
    LuceneIndexImpl index = createIndexRegions(indexName, dataRegionPath);
    index.setSearchableFields(fields);
    // for this API, set index to use the default StandardAnalyzer for each field
    index.setAnalyzer(analyzer);
    index.initialize();
    registerIndex(index);
  }
  
  private LuceneIndexImpl createIndexRegions(String indexName, String regionPath) {
    Region dataregion = this.cache.getRegion(regionPath);
    if (dataregion == null) {
      logger.info("Data region "+regionPath+" not found");
      return null;
    }
    //Convert the region name into a canonical form
    
    regionPath = dataregion.getFullPath();
    LuceneIndexImpl index = null;
    if (dataregion instanceof PartitionedRegion) {
      // partitioned region
      index = new LuceneIndexForPartitionedRegion(indexName, regionPath, cache);
    } else {
      // replicated region
      index = new LuceneIndexForReplicatedRegion(indexName, regionPath, cache);
    }
    return index;
  }

  @Override
  public LuceneIndex getIndex(String indexName, String regionPath) {
    Region region = cache.getRegion(regionPath);
    if(region == null) {
      return null;
    }
    return indexMap.get(getUniqueIndexName(indexName, region.getFullPath()));
  }

  @Override
  public Collection<LuceneIndex> getAllIndexes() {
    return indexMap.values();
  }

  @Override
  public void destroyIndex(LuceneIndex index) {
    LuceneIndexImpl indexImpl = (LuceneIndexImpl) index;
    indexMap.remove(getUniqueIndexName(index.getName(), index.getRegionPath()));
//    indexImpl.close();
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
  public void onCreate(Extensible<Cache> source, Extensible<Cache> target) {
    //This is called when CacheCreation (source) is turned into a GemfireCacheImpl (target)
    //nothing to do there.
  }
  
  public void registerIndex(LuceneIndex index){
    String regionAndIndex = getUniqueIndexName(index.getName(), index.getRegionPath()); 
    if( !indexMap.containsKey( regionAndIndex )) {
      indexMap.put(regionAndIndex, index);
    }
  }

  public void unregisterIndex(final String region){
    if( indexMap.containsKey( region )) indexMap.remove( region );
  }

  /**Public for test purposes */
  public static void registerDataSerializables() {
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.LUCENE_CHUNK_KEY,
        ChunkKey.class);
    
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.LUCENE_FILE,
        File.class);
    
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.LUCENE_FUNCTION_CONTEXT,
        LuceneFunctionContext.class);
    
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.LUCENE_STRING_QUERY_PROVIDER,
        StringQueryProvider.class);
    
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.LUCENE_TOP_ENTRIES_COLLECTOR_MANAGER,
        TopEntriesCollectorManager.class);
    
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.LUCENE_ENTRY_SCORE,
        EntryScore.class);
    
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.LUCENE_TOP_ENTRIES,
        TopEntries.class);
    
    DSFIDFactory.registerDSFID(
        DataSerializableFixedID.LUCENE_TOP_ENTRIES_COLLECTOR,
        TopEntriesCollector.class);
  }
}
