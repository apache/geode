/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.lucene;

import java.util.Iterator;
import java.util.ServiceLoader;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.lucene.LuceneServiceFactory;

public class LuceneServiceProvider {
  
  private static final LuceneServiceFactory factory;

  static {
    ServiceLoader<LuceneServiceFactory> loader = ServiceLoader.load(LuceneServiceFactory.class);
    Iterator<LuceneServiceFactory> itr = loader.iterator();
    if(!itr.hasNext()) {
      factory = null;
    } else {
      factory = itr.next();
      factory.initialize();
    }
  }
  
  public static LuceneService create(Cache cache) {
    
    if(factory == null) {
      return null;
    }
    
    return factory.create(cache);
  }
  
  private LuceneServiceProvider() {
    
  }
}
