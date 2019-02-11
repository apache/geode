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
package org.apache.geode.cache;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.geode.internal.cache.GemFireCacheImpl;

public class CacheStatisticsTest {

  @Test
  public void whenRegionPuts_increasesStatisticsPutCounter() {
    CacheFactory cacheFactory = new CacheFactory();
    Region<String, String> region =
        cacheFactory.create().<String, String>createRegionFactory(RegionShortcut.PARTITION)
            .create("region");


    GemFireCacheImpl regionService = (GemFireCacheImpl) region.getRegionService();
    long oldPuts = regionService.getCachePerfStats().getPuts();

    region.put("some", "value");

    long newPuts = regionService.getCachePerfStats().getPuts();

    assertEquals(oldPuts + 1, newPuts);
  }
}
