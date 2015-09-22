package com.gemstone.gemfire.cache.lucene.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;

/* wrapper of IndexWriter */
public class LuceneIndexForReplicatedRegion extends LuceneIndexImpl {

  public LuceneIndexForReplicatedRegion(String indexName, String regionPath, Cache cache) {
    // TODO Auto-generated constructor stub
  }

  public void initialize() {
    // TODO Auto-generated method stub
    
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Map<String, Analyzer> getFieldAnalyzerMap() {
    // TODO Auto-generated method stub
    return null;
  }

}
