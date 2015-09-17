package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public abstract class LuceneIndexImpl implements LuceneIndex {

  static private final boolean CREATE_CACHE = Boolean.getBoolean("lucene.createCache");
  static private final boolean USE_FS = Boolean.getBoolean("lucene.useFileSystem");
  
  protected HashSet<String> searchableFieldNames = new HashSet<String>();
  protected HashSet<String> searchablePDXFieldNames = new HashSet<String>();
  
  /* searchable fields should belong to a specific index
   */
  Region<String, File> fileRegion;
  Region<ChunkKey, byte[]> chunkRegion;
  
  protected String indexName;
  protected String regionPath;
  protected boolean hasInitialized = false;

  @Override
  public String getName() {
    return this.indexName;
  }

  @Override
  public String getRegionPath() {
    return this.regionPath;
  }
  
  protected void addSearchableField(String field) {
    searchableFieldNames.add(field);
  }
  
  protected void addSearchablePDXField(String field) {
    searchablePDXFieldNames.add(field);
  }

  @Override
  public String[] getFieldNames() {
    String[] fieldNames = new String[searchableFieldNames.size()];
    return searchableFieldNames.toArray(fieldNames);
  }

  @Override
  public String[] getPDXFieldNames() {
    String[] pdxFieldNames = new String[searchablePDXFieldNames.size()];;
    return searchablePDXFieldNames.toArray(pdxFieldNames);
  }
  
  @Override
  public Map<String, Analyzer> getFieldAnalyzerMap() {
    // TODO Auto-generated method stub
    // Will do that later: Gester
    return null;
  }

  @Override
  public Collection<IndexRepository> getRepository(RegionFunctionContext ctx) {
    return null;
  }

}
