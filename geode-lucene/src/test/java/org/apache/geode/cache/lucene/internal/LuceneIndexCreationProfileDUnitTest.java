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
package org.apache.geode.cache.lucene.internal;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.IncompatibleCacheServiceProfileException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({DistributedTest.class, LuceneTest.class})
public class LuceneIndexCreationProfileDUnitTest implements Serializable {

  private static final String INDEX_NAME = "index";
  private static final String REGION_NAME = "region";

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule(2);

  @Rule
  public CacheRule cacheRule = CacheRule.builder()
      .addSystemProperty(DistributionConfig.GEMFIRE_PREFIX + "luceneReindex", "true")
      .createCacheInAll().disconnectAfter().build();

  @Test
  public void testConcurrentIndexCreationWithDifferentProfiles() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    vm0.invoke(this::setupCacheAndRegion);
    vm1.invoke(this::setupCacheAndRegion);

    vm0.invoke(() -> {
      Region<Object, Object> region = cacheRule.getCache().getRegion(REGION_NAME);
      for (int i = 0; i < 113; i++) {
        region.put(i, i);
      }
    });

    AsyncInvocation<Boolean> asyncInvocation0 = vm0.invokeAsync(() -> {
      PartitionedRegion region1 = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);
      try {
        region1.addCacheServiceProfile(getOneFieldLuceneIndexCreationProfile());
        return false;
      } catch (IncompatibleCacheServiceProfileException e) {
        e.printStackTrace();
        return true;
      }
    });

    AsyncInvocation<Boolean> asyncInvocation1 = vm1.invokeAsync(() -> {
      PartitionedRegion region2 = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);
      try {
        region2.addCacheServiceProfile(getTwoFieldLuceneIndexCreationProfile());
        return false;
      } catch (IncompatibleCacheServiceProfileException e) {
        e.printStackTrace();
        return true;
      }
    });

    Awaitility.waitAtMost(30, TimeUnit.SECONDS)
        .until(() -> asyncInvocation0.get() && asyncInvocation1.get());
  }

  private void setupCacheAndRegion() {
    InternalCache cache = cacheRule.getCache();
    cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

  private LuceneIndexCreationProfile getOneFieldLuceneIndexCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME, new String[] {"field1"},
        new StandardAnalyzer(), null, null);
  }

  private LuceneIndexCreationProfile getTwoFieldLuceneIndexCreationProfile() {
    return new LuceneIndexCreationProfile(INDEX_NAME, REGION_NAME,
        new String[] {"field1", "field2"}, new StandardAnalyzer(), null, null);
  }
}
