/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache.lucene.internal;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.distributed.LuceneQueryFunction;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
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

  @Test
  public void getCacheShouldReturnTheCorrectCache() {
    cache = getCache();
    new LuceneServiceImpl().init(cache);
    LuceneService service = LuceneServiceProvider.get(cache);
    assertTrue(service.getCache().equals(cache));
  }

  // lucene service will register query execution function on initialization
  @Test
  public void shouldRegisterQueryFunction() {
    Function function = FunctionService.getFunction(LuceneQueryFunction.ID);
    assertNull(function);

    cache = getCache();
    new LuceneServiceImpl().init(cache);

    function = FunctionService.getFunction(LuceneQueryFunction.ID);
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
      clientCache = new ClientCacheFactory().set(MCAST_PORT, "0").create();
    } else {
      return clientCache;
    }
    return clientCache;
  }

  private Cache getCache() {
    if (null == cache) {
      cache = new CacheFactory().set(MCAST_PORT, "0").create();
    }
    return cache;
  }

}
