package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Implementation of LuceneService to create lucene index and query. 
 * 
 * @author Xiaojian Zhou
 * 
 * @since 8.5
 */
public class LuceneServiceImpl implements LuceneService {
  private final Cache cache;
  private static LuceneServiceImpl instance;

  private final HashMap<String, LuceneIndex>  indexMap;

  private LuceneServiceImpl(final Cache cache) {
    if (cache == null) {
      throw new IllegalStateException(LocalizedStrings.CqService_CACHE_IS_NULL.toLocalizedString());
    }
    GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
    gfc.getCancelCriterion().checkCancelInProgress(null);

    this.cache = gfc;

    
    // Initialize the Map which maintains indexes
    this.indexMap = new HashMap<String, LuceneIndex>();
  }
  
  public static synchronized LuceneServiceImpl getInstance(final Cache cache) {
    if (instance == null) {
      instance = new LuceneServiceImpl(cache);
    }
    return instance;
  }
  
  public String getUniqueIndexName(String indexName, String regionName) {
    String name = indexName+"#"+regionName.replace('/', '_');
    return name;
  }
  
  @Override
  public LuceneIndex createIndex(String indexName, String regionName, String... fields) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public LuceneIndex getIndex(String indexName, String regionName) {
    return indexMap.get(getUniqueIndexName(indexName, regionName));
  }

  @Override
  public Collection<LuceneIndex> getAllIndexes() {
    return indexMap.values();
  }

  @Override
  public LuceneIndex createIndex(String indexName, String regionName, 
      Map<String, Analyzer> analyzerPerField) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void destroyIndex(LuceneIndex index) {
    LuceneIndexImpl indexImpl = (LuceneIndexImpl)index;
    indexMap.remove(getUniqueIndexName(index.getName(), index.getRegionName()));
    indexImpl.close();
  }

  @Override
  public LuceneQueryFactory createLuceneQueryFactory() {
    return new LuceneQueryFactoryImpl();
  }

}
