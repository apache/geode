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

package org.apache.geode.management.internal.configuration.validators;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;
import org.apache.geode.management.internal.CacheElementOperation;

public class IndexValidatorTest {
  private IndexValidator indexValidator;
  private Index index;
  private ConfigurationPersistenceService configurationPersistenceService;
  private CacheConfig cacheConfig;
  private RegionConfig regionConfig;

  @Before
  public void init() {
    configurationPersistenceService = mock(ConfigurationPersistenceService.class);
    cacheConfig = mock(CacheConfig.class);
    regionConfig = mock(RegionConfig.class);
    indexValidator = new IndexValidator(configurationPersistenceService);
    index = new Index();
    index.setName("testIndex");
    index.setExpression("testExpression");
    index.setRegionPath("testRegion");
  }

  @Test
  public void validate_happy() {
    when(configurationPersistenceService.getCacheConfig(any(), anyBoolean()))
        .thenReturn(cacheConfig);
    when(cacheConfig.findRegionConfiguration(anyString())).thenReturn(regionConfig);
    indexValidator.validate(CacheElementOperation.CREATE, index);
  }

  @Test
  public void validate_regionMissing() {
    when(configurationPersistenceService.getCacheConfig(any(), anyBoolean()))
        .thenReturn(cacheConfig);
    when(cacheConfig.findRegionConfiguration(anyString())).thenReturn(null);

    assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.CREATE, index))
        .isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("Region provided does not exist: testRegion.");
  }

  @Test
  public void validate_missingExpression() {
    index.setExpression(null);

    assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.CREATE, index))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expression is required");
  }

  @Test
  public void validate_hasRegionPath() {
    index.setRegionPath(null);

    assertSoftly(softly -> {
      softly.assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.CREATE, index))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("RegionPath is required");
      softly.assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.DELETE, index))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("RegionPath is required");
    });

  }

  @Test
  public void validate_HashTypeNotAllowed() {
    index.setIndexType(IndexType.HASH_DEPRECATED);

    assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.CREATE, index))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("IndexType HASH is not allowed");
  }

  @Test
  public void validate_IndexHasName() {
    index.setName(null);

    assertSoftly(softly -> {
      softly.assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.CREATE, index))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Name is required");
      softly.assertThatThrownBy(() -> indexValidator.validate(CacheElementOperation.DELETE, index))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Name is required");
    });

  }
}
