package com.gemstone.gemfire.cache.lucene.internal;

import java.util.HashSet;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.xml.LuceneIndexCreation;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.logging.LogService;

public abstract class LuceneIndexImpl implements InternalLuceneIndex {

  static private final boolean CREATE_CACHE = Boolean.getBoolean("lucene.createCache");
  static private final boolean USE_FS = Boolean.getBoolean("lucene.useFileSystem");
  
  protected static final Logger logger = LogService.getLogger();
  
//  protected HashSet<String> searchableFieldNames = new HashSet<String>();
  String[] searchableFieldNames;
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
  
  protected void setSearchableFields(String[] fields) {
    searchableFieldNames = fields;
  }
  
  @Override
  public String[] getFieldNames() {
    return searchableFieldNames;
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

  protected abstract void initialize();
  
  /**
   * Register an extension with the region
   * so that xml will be generated for this index.
   */
  protected void addExtension(PartitionedRegion dataRegion) {
    LuceneIndexCreation creation = new LuceneIndexCreation();
    creation.setName(this.getName());
    creation.addFieldNames(this.getFieldNames());
    creation.setRegion(dataRegion);
    creation.setFieldFieldAnalyzerMap(this.getFieldAnalyzerMap());
    dataRegion.getExtensionPoint().addExtension(creation);
  }
}
