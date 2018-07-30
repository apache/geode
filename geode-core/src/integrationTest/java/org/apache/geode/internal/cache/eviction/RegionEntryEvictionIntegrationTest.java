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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.EvictionTest;

@Category({EvictionTest.class})
public class RegionEntryEvictionIntegrationTest {
  private Region<String, String> region;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    region = createRegion();
  }

  @Test
  public void verifyMostRecentEntryIsNotEvicted() {
    region.create("one", "one");
    region.create("twoo", "twoo");
    region.put("one", "one");
    region.put("twoo", "twoo");

    region.create("threee", "threee");
    assertThat(region.size()).isEqualTo(2);
    assertThat(region.containsKey("threee")).isTrue();
  }

  private Region<String, String> createRegion() throws Exception {
    Cache cache = new CacheFactory().set("locators", "").set("mcast-port", "0").create();
    return cache.<String, String>createRegionFactory(RegionShortcut.REPLICATE)
        .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(2))
        .create(testName.getMethodName());
  }

}
