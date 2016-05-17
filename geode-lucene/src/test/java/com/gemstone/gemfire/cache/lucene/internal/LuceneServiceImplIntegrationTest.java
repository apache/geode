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

package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunction;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(IntegrationTest.class)
public class LuceneServiceImplIntegrationTest {

  Cache cache;
  ClientCache clientCache;
  LuceneServiceImpl service = null;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void luceneServiceProviderGetShouldAcceptClientCacheAsAParameter() {
    clientCache = getClientCache();
    LuceneService luceneService = LuceneServiceProvider.get(clientCache);
    assertNotNull(luceneService);
  }

  // lucene service will register query execution function on initialization
  @Test
  public void shouldRegisterQueryFunction() {
    Function function = FunctionService.getFunction(LuceneFunction.ID);
    assertNull(function);

    cache = getCache();
    new LuceneServiceImpl().init(cache);

    function = FunctionService.getFunction(LuceneFunction.ID);
    assertNotNull(function);
  }

  @After
  public void destroyService() {
    if (null != service) {
      service = null;
    }
  }

  @After
  public void destroyCache() {
    if (null != cache && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
    if (null != clientCache && !clientCache.isClosed()) {
      clientCache.close();
      clientCache = null;
    }
  }

  private ClientCache getClientCache() {
    if (null == clientCache) {
      clientCache = new ClientCacheFactory().set("mcast-port", "0").create();
    }
    else {
      return clientCache;
    }
    return clientCache;
  }

  private Cache getCache() {
    if (null == cache) {
      cache = new CacheFactory().set("mcast-port", "0").create();
    }
    return cache;
  }

}


