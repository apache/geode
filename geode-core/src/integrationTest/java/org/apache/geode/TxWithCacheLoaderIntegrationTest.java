/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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
package org.apache.geode;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionId;

public class TxWithCacheLoaderIntegrationTest {

  private static final String REGION_NAME =
      TxWithCacheLoaderIntegrationTest.class.getSimpleName() + "_Region";
  private static final String VALUE2 = "value2";
  private static final String KEY1 = "key1";
  private static final String KEY2 = "key2";
  private static final String VALUE1 = "value1";

  private Cache cache;
  private CacheTransactionManager transactionManager;

  private Region<String, String> region;

  @Before
  public void setUp() throws Exception {
    cache = new CacheFactory().create();
    transactionManager = cache.getCacheTransactionManager();
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.close();
    }
    cache = null;
    region = null;
  }

  protected Properties getConfig() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(MCAST_PORT, "0");
    return config;
  }


  @Test
  public void txWithCacheLoaderThrowsIfThereIsConflict() {
    region = createRegion();
    // first transaction
    transactionManager.begin();
    region.put(KEY1, VALUE1);
    assertThat(region.get(KEY1)).isEqualTo(VALUE1);
    TransactionId txId = transactionManager.suspend();

    // second transaction with cacheLoader
    transactionManager.begin();
    region.getAttributesMutator().setCacheLoader(new MyLoader<>());
    assertThat(region.get(KEY1)).isEqualTo(KEY1);

    transactionManager.commit();

    // resume first transaction
    transactionManager.resume(txId);
    Throwable thrown = catchThrowable(transactionManager::commit);
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
    assertThat(region.get(KEY1)).isEqualTo(KEY1);
  }

  @Test
  public void txWithCacheLoaderSucceedsIfThereIsNoConflict() {
    Region<String, String> region = createRegion();
    // first transaction
    transactionManager.begin();
    region.put(KEY2, VALUE2);
    assertThat(region.get(KEY2)).isEqualTo(VALUE2);
    TransactionId txId = transactionManager.suspend();

    // second transaction with cacheLoader
    transactionManager.begin();
    region.getAttributesMutator().setCacheLoader(new MyLoader<>());
    assertThat(region.get(KEY1)).isEqualTo(KEY1);

    transactionManager.commit();

    // resume first transaction
    transactionManager.resume(txId);
    transactionManager.commit();
    assertThat(region.get(KEY1)).isEqualTo(KEY1);
    assertThat(region.get(KEY2)).isEqualTo(VALUE2);
  }

  private static class MyLoader<Object> implements CacheLoader<Object, Object> {

    @Override
    public void close() {}

    @Override
    public Object load(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
      return helper.getKey();
    }
  }

  private Region<String, String> createRegion() {
    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    return rf.create(REGION_NAME);
  }
}
