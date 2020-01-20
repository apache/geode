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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;

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
  public void emptyConfig() {
    assertThat(manager.list(index, cacheConfig)).isEmpty();
  }

  @Test
  public void listWithoutFilter() {
    setupCacheConfig();
    List<Index> list = manager.list(index, cacheConfig);
    assertSoftly(softly -> {
      softly.assertThat(list).hasSize(2)
          .extracting(Index::getName)
          .containsExactlyInAnyOrder("index1", "index2");
      softly.assertThat(list).extracting(Index::getRegionName)
          .containsExactlyInAnyOrder("region1", "region2");
    });
  }

  @Test
  public void listWithRegionName() {
    setupCacheConfig();
    index.setRegionPath("region1");
    List<Index> list = manager.list(index, cacheConfig);
    assertSoftly(softly -> {
      softly.assertThat(list).hasSize(1);
      softly.assertThat(list.get(0).getName()).isEqualTo("index1");
      softly.assertThat(list.get(0).getRegionName()).isEqualTo("region1");
    });
  }

  @Test
  public void listWithIndexName() {
    setupCacheConfig();
    index.setName("index2");
    List<Index> list = manager.list(index, cacheConfig);
    assertSoftly(softly -> {
      softly.assertThat(list).hasSize(1);
      softly.assertThat(list.get(0).getName()).isEqualTo("index2");
      softly.assertThat(list.get(0).getRegionName()).isEqualTo("region2");
    });
  }

  @Test
  public void listWithUnMatchingFilter() {
    setupCacheConfig();
    index.setName("index1");
    index.setRegionPath("region2");
    List<Index> list = manager.list(index, cacheConfig);
    assertThat(list).hasSize(0);
  }

  @Test
  public void listWithRegionNameAndIndexName() {
    setupCacheConfig();
    index.setName("index2");
    index.setRegionPath("region2");
    List<Index> list = manager.list(index, cacheConfig);
    assertSoftly(softly -> {
      softly.assertThat(list).hasSize(1);
      softly.assertThat(list.get(0).getName()).isEqualTo("index2");
      softly.assertThat(list.get(0).getRegionName()).isEqualTo("region2");
    });

  }

  @Test
  public void add_regionExists() {
    setupCacheConfig();
    index.setName("index3");
    index.setIndexType(IndexType.RANGE);
    index.setRegionPath("region1");

    manager.add(index, cacheConfig);
    List<Index> indices = manager.list(index, cacheConfig);

    assertSoftly(softly -> {
      softly.assertThat(indices).hasSize(1);
      softly.assertThat(indices.get(0).getName()).isEqualTo("index3");
      softly.assertThat(indices.get(0).getRegionName()).isEqualTo("region1");
    });
  }

  @Test
  public void add_regionNotFound() {
    setupCacheConfig();
    index.setName("index3");
    index.setIndexType(IndexType.RANGE);
    index.setRegionPath("region3");

    assertThatThrownBy(() -> manager.add(index, cacheConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Region provided does not exist: region3");
  }

  @Test
  public void update_notImplemented() {
    setupCacheConfig();
    index.setName("index3");
    index.setIndexType(IndexType.RANGE);
    index.setRegionPath("region3");

    assertThatThrownBy(() -> manager.update(index, cacheConfig))
        .isInstanceOf(NotImplementedException.class)
        .hasMessageContaining("Not implemented yet");
  }

  @Test
  public void delete_regionNotFound() {
    setupCacheConfig();
    index.setName("index3");
    index.setIndexType(IndexType.RANGE);
    index.setRegionPath("region3");

    assertThatThrownBy(() -> manager.delete(index, cacheConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Region provided does not exist: region3");
  }

  @Test
  public void delete_indexNotFound() {
    setupCacheConfig();
    index.setName("index3");
    index.setIndexType(IndexType.RANGE);
    index.setRegionPath("region2");

    assertThatThrownBy(() -> manager.delete(index, cacheConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Index provided does not exist on Region: region2, index3");
  }

  @Test
  public void delete_indexDeleted() {
    setupCacheConfig();
    index.setName("index2");
    index.setIndexType(IndexType.RANGE);
    index.setRegionPath("region2");

    assertSoftly(softly -> {
      softly.assertThat(manager.list(index, cacheConfig)).hasSize(1);
      manager.delete(index, cacheConfig);
      softly.assertThat(manager.list(index, cacheConfig)).hasSize(0);
    });
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
