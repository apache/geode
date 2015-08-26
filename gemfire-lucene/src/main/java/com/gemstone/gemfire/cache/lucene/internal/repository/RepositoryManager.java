package com.gemstone.gemfire.cache.lucene.internal.repository;

import com.gemstone.gemfire.cache.Region;

public interface RepositoryManager {

  SingleIndexRepository getRepository(Region region, Object key);

}
