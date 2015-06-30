package com.gemstone.gemfire.cache.lucene.internal;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;

import com.gemstone.gemfire.cache.lucene.LuceneIndex;

/* wrapper of IndexWriter */
public class LuceneIndexImpl implements LuceneIndex {

  /* searchable fields should belong to a specific index
   */
  HashSet<String> searchableFieldNames;
  
  HashSet<String> searchablePDXFieldNames;
  
  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getRegionName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getFieldNames() {
    // TODO Auto-generated method stub
    return null;
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
