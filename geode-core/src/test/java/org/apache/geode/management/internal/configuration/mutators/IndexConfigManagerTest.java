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

package org.apache.geode.management.internal.configuration.mutators;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.Index;

public class IndexConfigManagerTest {

  private CacheConfig cacheConfig;
  private Index index;
  private IndexConfigManager manager;

  @Before
  public void before() throws Exception {
    cacheConfig = new CacheConfig();
    index = new Index();
    manager = new IndexConfigManager(null);
  }

  @Test
  public void emptyConfig() throws Exception {
    assertThat(manager.list(index, cacheConfig)).isEmpty();
  }

  @Test
  public void listWithoutFilter() throws Exception {
    setupCacheConfig();
    List<Index> list = manager.list(index, cacheConfig);
    assertThat(list).hasSize(2)
        .extracting(i -> i.getName())
        .containsExactlyInAnyOrder("index1", "index2");
    assertThat(list).extracting(i -> i.getRegionName())
        .containsExactlyInAnyOrder("region1", "region2");
  }

  @Test
  public void listWithRegionName() throws Exception {
    setupCacheConfig();
    index.setRegionPath("region1");
    List<Index> list = manager.list(index, cacheConfig);
    assertThat(list).hasSize(1);
    assertThat(list.get(0).getName()).isEqualTo("index1");
    assertThat(list.get(0).getRegionName()).isEqualTo("region1");
  }

  @Test
  public void listWithIndexName() throws Exception {
    setupCacheConfig();
    index.setName("index2");
    List<Index> list = manager.list(index, cacheConfig);
    assertThat(list).hasSize(1);
    assertThat(list.get(0).getName()).isEqualTo("index2");
    assertThat(list.get(0).getRegionName()).isEqualTo("region2");
  }

  @Test
  public void listWithUnMatchingFilter() throws Exception {
    setupCacheConfig();
    index.setName("index1");
    index.setRegionPath("region2");
    List<Index> list = manager.list(index, cacheConfig);
    assertThat(list).hasSize(0);
  }

  @Test
  public void listWithRegionNameAndIndexName() throws Exception {
    setupCacheConfig();
    index.setName("index2");
    index.setRegionPath("region2");
    List<Index> list = manager.list(index, cacheConfig);
    assertThat(list).hasSize(1);
    assertThat(list.get(0).getName()).isEqualTo("index2");
    assertThat(list.get(0).getRegionName()).isEqualTo("region2");
  }

  private void setupCacheConfig() {
    RegionConfig region1 = new RegionConfig("region1", "REPLICATE");
    RegionConfig.Index index1 = new RegionConfig.Index();
    index1.setName("index1");
    index1.setFromClause("/region1");
    region1.getIndexes().add(index1);

    RegionConfig region2 = new RegionConfig("region2", "REPLICATE");
    RegionConfig.Index index2 = new RegionConfig.Index();
    index2.setName("index2");
    index2.setFromClause("/region2");
    region2.getIndexes().add(index2);
    cacheConfig.getRegions().add(region1);
    cacheConfig.getRegions().add(region2);
  }
}
