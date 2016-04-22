/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene;

import com.gemstone.gemfire.annotations.Experimental;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.internal.InternalLuceneService;
import com.gemstone.gemfire.internal.cache.InternalCache;

/**
 * Class for retrieving or creating the currently running
 * instance of the LuceneService.
 *
 */
@Experimental
public class LuceneServiceProvider {
  
  /**
   * Retrieve or create the lucene service for this cache
   */
  public static LuceneService get(Cache cache) {
    InternalCache internalCache = (InternalCache) cache;
    return internalCache.getService(InternalLuceneService.class);
  }
  
  private LuceneServiceProvider() {
    
  }
}
