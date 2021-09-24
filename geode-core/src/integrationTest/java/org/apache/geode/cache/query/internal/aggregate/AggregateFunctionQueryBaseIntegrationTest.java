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
package org.apache.geode.cache.query.internal.aggregate;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;
import org.apache.geode.util.internal.GeodeGlossary;

@Category(OQLQueryTest.class)
@RunWith(GeodeParamsRunner.class)
public abstract class AggregateFunctionQueryBaseIntegrationTest {
  static final String firstRegionName = "portfolio1";
  static final String secondRegionName = "portfolio2";
  Map<Integer, Object> regionOneLocalCopy = new HashMap<>();
  Map<Integer, Object> regionTwoLocalCopy = new HashMap<>();

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
  }

  static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = new ConcurrentHashMap<>();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }

  void createRegion(String regionName, RegionShortcut regionShortcut) {
    Cache cache = server.getCache();
    cache.<Integer, Object>createRegionFactory(regionShortcut).create(regionName);
  }

  void populateRegion(String regionName, Map<Integer, Object> regionData) {
    Cache cache = server.getCache();
    Region<Integer, Object> region = cache.getRegion(regionName);
    regionData.forEach(region::put);

    await().untilAsserted(() -> assertThat(region.size()).isEqualTo(regionData.size()));
  }

  void createAndPopulateRegion(String regionName, RegionShortcut regionShortcut,
      Map<Integer, Object> regionData) {
    Cache cache = server.getCache();
    Region<Integer, Object> region =
        cache.<Integer, Object>createRegionFactory(regionShortcut).create(regionName);
    regionData.forEach(region::put);

    await().untilAsserted(() -> assertThat(region.size()).isEqualTo(regionData.size()));
  }
}
