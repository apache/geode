package com.gemstone.gemfire.cache.lucene.internal;

import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;

public interface InternalLuceneIndex extends LuceneIndex {
  
  public RepositoryManager getRepositoryManager();

}
