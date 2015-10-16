package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;

import com.gemstone.gemfire.cache.Cache;

/* wrapper of IndexWriter */
public class LuceneIndexForReplicatedRegion extends LuceneIndexImpl {

  public LuceneIndexForReplicatedRegion(String indexName, String regionPath, Cache cache) {
    throw new UnsupportedOperationException("Lucene indexes on replicated regions is not yet implemented");
  }

  public void initialize() {
    throw new UnsupportedOperationException("Lucene indexes on replicated regions is not yet implemented");
  }

  public void close() {
    throw new UnsupportedOperationException("Lucene indexes on replicated regions is not yet implemented");
  }

  @Override
  public Map<String, Analyzer> getFieldAnalyzerMap() {
    throw new UnsupportedOperationException("Lucene indexes on replicated regions is not yet implemented");
  }

}
