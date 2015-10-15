package com.gemstone.gemfire.cache.lucene.internal;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.internal.cache.CacheService;
import com.gemstone.gemfire.internal.cache.extension.Extension;

public interface InternalLuceneService extends LuceneService, Extension<Cache>, CacheService {

}
