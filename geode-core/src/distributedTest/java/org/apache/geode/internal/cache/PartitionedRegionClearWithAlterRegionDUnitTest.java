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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class PartitionedRegionClearWithAlterRegionDUnitTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  private VM server1;

  private VM server2;

  private static final String REGION_NAME = "testRegion";

  private static final int NUM_ENTRIES = 1000000;


  @Before
  public void initialize() {
    server1 = VM.getVM(0);
    server2 = VM.getVM(1);

    server1.invoke(() -> {
      cacheRule.createCache();
      cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true).create(REGION_NAME);
    });

    server2.invoke(() -> {
      cacheRule.createCache();
      cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true).create(REGION_NAME);
    });

    server1.invoke(() -> {
      populateRegion();
      Region region = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(region.size()).isEqualTo(NUM_ENTRIES);
    });

    server2.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(region.size()).isEqualTo(NUM_ENTRIES);
    });
  }

  @Test
  public void testClearRegionWhileAddingCacheLoader() throws InterruptedException {

    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      cacheRule.getCache().getRegion(REGION_NAME).clear();
    });

    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      alterRegionSetCacheLoader();
    });

    asyncInvocation1.await();
    asyncInvocation2.await();
  }

  private void populateRegion() {
    Region region = cacheRule.getCache().getRegion(REGION_NAME);
    IntStream.range(0, NUM_ENTRIES).forEach(i -> region.put(i, i));
  }

  private void alterRegionSetCacheLoader() {
    Region region = cacheRule.getCache().getRegion(REGION_NAME);
    AttributesMutator attributesMutator = region.getAttributesMutator();
    attributesMutator.setCacheLoader(new TestCacheLoader());
  }

  private void alterRegionSetCacheWriter() {
    Region region = cacheRule.getCache().getRegion(REGION_NAME);
    AttributesMutator attributesMutator = region.getAttributesMutator();
    attributesMutator.setCacheWriter(new TestCacheWriter());
  }

  private void alterRegionSetCacheListener() {
    Region region = cacheRule.getCache().getRegion(REGION_NAME);
    AttributesMutator attributesMutator = region.getAttributesMutator();
    attributesMutator.addCacheListener(new TestCacheListener());
  }

  private class TestCacheLoader implements CacheLoader {

    @Override
    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return new Integer(NUM_ENTRIES);
    }
  }

  private class TestCacheWriter implements CacheWriter {

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {

    }

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {

    }

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {

    }

    @Override
    public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {

    }

    @Override
    public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
      System.out.println("beforeRegionClear");
    }
  }

  private class TestCacheListener implements CacheListener {

    @Override
    public void afterCreate(EntryEvent event) {

    }

    @Override
    public void afterUpdate(EntryEvent event) {

    }

    @Override
    public void afterInvalidate(EntryEvent event) {

    }

    @Override
    public void afterDestroy(EntryEvent event) {

    }

    @Override
    public void afterRegionInvalidate(RegionEvent event) {

    }

    @Override
    public void afterRegionDestroy(RegionEvent event) {

    }

    @Override
    public void afterRegionClear(RegionEvent event) {
      System.out.println("afterRegionClear");
    }

    @Override
    public void afterRegionCreate(RegionEvent event) {

    }

    @Override
    public void afterRegionLive(RegionEvent event) {

    }
  }

  @Test
  public void testClearRegionWhileAddingCacheWriter() throws InterruptedException {
    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      cacheRule.getCache().getRegion(REGION_NAME).clear();
    });

    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      alterRegionSetCacheWriter();
    });

    asyncInvocation1.await();
    asyncInvocation2.await();
  }

  @Test
  public void testClearRegionWhileAddingCacheListener() throws InterruptedException {
    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      cacheRule.getCache().getRegion(REGION_NAME).clear();
    });

    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      alterRegionSetCacheListener();
    });

    asyncInvocation1.await();
    asyncInvocation2.await();
  }

  @Test
  public void testClearRegionWhileChangingEviction() throws InterruptedException {
    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      Region region = cacheRule.getCache().getRegion(REGION_NAME);
      AttributesMutator attributesMutator = region.getAttributesMutator();
      attributesMutator.getEvictionAttributesMutator().setMaximum(1);
    });

    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      cacheRule.getCache().getRegion(REGION_NAME).clear();
    });

    asyncInvocation1.await();
    asyncInvocation2.await();
  }

  @Test
  public void testClearRegionWhileChangingRegionTTLExpiration() throws InterruptedException {
    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      Region region = cacheRule.getCache().getRegion(REGION_NAME);
      AttributesMutator attributesMutator = region.getAttributesMutator();
      ExpirationAttributes expirationAttributes = new ExpirationAttributes();
      attributesMutator.setRegionTimeToLive(expirationAttributes);
    });

    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      cacheRule.getCache().getRegion(REGION_NAME).clear();
    });

    asyncInvocation1.await();
    asyncInvocation2.await();
  }

  @Test
  public void testClearRegionWhileChangingEntryTTLExpiration() throws InterruptedException {
    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      Region region = cacheRule.getCache().getRegion(REGION_NAME);
      AttributesMutator attributesMutator = region.getAttributesMutator();
      ExpirationAttributes expirationAttributes = new ExpirationAttributes();
      attributesMutator.setEntryTimeToLive(expirationAttributes);
    });

    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      cacheRule.getCache().getRegion(REGION_NAME).clear();
    });

    asyncInvocation1.await();
    asyncInvocation2.await();
  }

  @Test
  public void testClearRegionWhileChangingRegionIdleExpiration() throws InterruptedException {
    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      Region region = cacheRule.getCache().getRegion(REGION_NAME);
      AttributesMutator attributesMutator = region.getAttributesMutator();
      ExpirationAttributes expirationAttributes = new ExpirationAttributes();
      attributesMutator.setRegionIdleTimeout(expirationAttributes);
    });

    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      cacheRule.getCache().getRegion(REGION_NAME).clear();
    });

    asyncInvocation1.await();
    asyncInvocation2.await();
  }

  @Test
  public void testClearRegionWhileChangingEntryIdleExpiration() throws InterruptedException {
    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      Region region = cacheRule.getCache().getRegion(REGION_NAME);
      AttributesMutator attributesMutator = region.getAttributesMutator();
      ExpirationAttributes expirationAttributes = new ExpirationAttributes(1, ExpirationAction.DESTROY);
      attributesMutator.setEntryIdleTimeout(expirationAttributes);
    });

    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      cacheRule.getCache().getRegion(REGION_NAME).clear();
    });

    asyncInvocation1.await();
    asyncInvocation2.await();
  }

  // TODO: member join and leave when these operations are in progress

}
