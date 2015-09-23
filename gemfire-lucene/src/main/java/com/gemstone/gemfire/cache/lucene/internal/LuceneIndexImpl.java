package com.gemstone.gemfire.cache.lucene.internal;

import java.util.HashSet;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;

public abstract class LuceneIndexImpl implements InternalLuceneIndex {

  static private final boolean CREATE_CACHE = Boolean.getBoolean("lucene.createCache");
  static private final boolean USE_FS = Boolean.getBoolean("lucene.useFileSystem");
  
  protected HashSet<String> searchableFieldNames = new HashSet<String>();
  protected RepositoryManager repositoryManager;
  protected Analyzer analyzer;
  
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
  
  @Override
  public String[] getFieldNames() {
    String[] fieldNames = new String[searchableFieldNames.size()];
    return searchableFieldNames.toArray(fieldNames);
  }

  @Override
  public Map<String, Analyzer> getFieldAnalyzerMap() {
    // TODO Auto-generated method stub
    // Will do that later: Gester
    return null;
  }

  public RepositoryManager getRepositoryManager() {
    return this.repositoryManager;
  }
  
  public void setAnalyzer(Analyzer analyzer) {
    if (analyzer == null) {
      this.analyzer = new StandardAnalyzer();
    } else {
      this.analyzer = analyzer;
    }
  }

  public Analyzer getAnalyzer() {
    return this.analyzer;
  }

  protected void initialize() {
  }
}
