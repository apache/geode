package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
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
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
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
  private final Cache cache;

  private final HashMap<String, LuceneIndex> indexMap;
  
  private static final Logger logger = LogService.getLogger();

  public LuceneServiceImpl(final Cache cache) {
    if (cache == null) {
      throw new IllegalStateException(LocalizedStrings.CqService_CACHE_IS_NULL.toLocalizedString());
    }
    GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
    gfc.getCancelCriterion().checkCancelInProgress(null);

    this.cache = gfc;

    FunctionService.registerFunction(new LuceneFunction());
    registerDataSerializables();

    // Initialize the Map which maintains indexes
    this.indexMap = new HashMap<String, LuceneIndex>();
  }
  
  public static String getUniqueIndexName(String indexName, String regionPath) {
    String name = indexName + "#" + regionPath.replace('/', '_');
    return name;
  }

  @Override
  public LuceneIndex createIndex(String indexName, String regionPath, String... fields) {
    LuceneIndexImpl index = createIndexRegions(indexName, regionPath);
    if (index == null) {
      return null;
    }
    for (String field:fields) {
      index.addSearchableField(field);
    }
    // for this API, set index to use the default StandardAnalyzer for each field
    index.setAnalyzer(null);
    index.initialize();
    registerIndex(index);
    return index;
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
  public LuceneIndex createIndex(String indexName, String regionPath, Map<String, Analyzer> analyzerPerField) {
    LuceneIndexImpl index = createIndexRegions(indexName, regionPath);
    if (index == null) {
      return null;
    }
    
    Analyzer analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), analyzerPerField);
    for (String field:analyzerPerField.keySet()) {
      index.addSearchableField(field);
    }
    index.setAnalyzer(analyzer);
    index.initialize();
    registerIndex(index);
    return index;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void onCreate(Extensible<Cache> source, Extensible<Cache> target) {
    // TODO Auto-generated method stub

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
