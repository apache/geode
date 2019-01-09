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
package org.apache.geode.internal.cache.eviction;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.EvictionTest;

@Category({EvictionTest.class})
public class RegionEntryEvictionIntegrationTest {

  private Cache cache;

  private Region<String, String> region;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setup() {
    cache = new CacheFactory().set("locators", "").set("mcast-port", "0").create();
  }

  @After
  public void cleaup() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
  }

  @Test
  public void verifyMostRecentEntryIsNotEvicted() {
    region = createRegion();
    region.create("one", "one");
    region.create("twoo", "twoo");
    region.put("one", "one");
    region.put("twoo", "twoo");

    region.create("threee", "threee");
    assertThat(region.size()).isEqualTo(2);
    assertThat(region.containsKey("threee")).isTrue();
  }

  @Test
  public void evictionWithCustomExpiryCallingGetValueDoesNotDeadLock() throws Exception {
    int numThreads = 2;
    int entries = 1000;
    int evictionCount = 1;
    region = createRegionWithCustomExpiration(evictionCount);

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    ArrayList<Future> futures = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      futures.add(executorService.submit(() -> doCreates(region, entries)));
    }

    for (int i = 0; i < numThreads; i++) {
      int ops = (Integer) futures.get(i).get(2, TimeUnit.MINUTES);
      assertThat(ops).isEqualTo(entries);
    }

    executorService.shutdown();
  }

  private int doCreates(Region region, int entries) {
    String key = Thread.currentThread().getName();
    for (int i = 1; i < entries; i++) {
      region.create(key + i, "value " + i);
    }
    return entries;
  }


  private Region<String, String> createRegion() {
    return cache.<String, String>createRegionFactory(RegionShortcut.REPLICATE)
        .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(2))
        .create(testName.getMethodName());
  }

  private Region<String, String> createRegionWithCustomExpiration(int evictionCount) {
    return cache.<String, String>createRegionFactory(RegionShortcut.REPLICATE)
        .setCustomEntryIdleTimeout(new CustomExpiry<String, String>() {
          @Override
          public ExpirationAttributes getExpiry(Region.Entry<String, String> entry) {
            entry.getValue();
            return null;
          }

          @Override
          public void close() {

        }
        }).setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(evictionCount))
        .create(testName.getMethodName());
  }
}
