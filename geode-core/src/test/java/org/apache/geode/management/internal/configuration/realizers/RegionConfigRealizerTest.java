/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.configuration.realizers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;

public class RegionConfigRealizerTest {
  InternalCache cache;
  RegionFactory regionFactory;
  RegionConfigRealizer realizer;
  RegionConfigValidator validator;
  Region config;
  org.apache.geode.cache.Region region;

  @Before
  public void setup() {
    cache = mock(InternalCache.class);
    validator = new RegionConfigValidator(cache);
    regionFactory = mock(RegionFactory.class);
    when(cache.createRegionFactory()).thenReturn(regionFactory);
    realizer = new RegionConfigRealizer();
    config = new Region();
    config.setName("test");
    region = mock(org.apache.geode.cache.Region.class);
  }

  @Test
  public void createsPartitionedInCache() {
    config.setType(RegionType.PARTITION);
    validator.validate(CacheElementOperation.CREATE, config);
    realizer.create(config, cache);
    verify(regionFactory).create("test");
    verify(regionFactory).setDataPolicy(DataPolicy.PARTITION);
  }

  @Test
  public void createsReplicateInCache() {
    config.setType(RegionType.REPLICATE);
    validator.validate(CacheElementOperation.CREATE, config);
    realizer.create(config, cache);
    verify(regionFactory).create("test");
    verify(regionFactory).setDataPolicy(DataPolicy.REPLICATE);
  }

  @Test
  public void getRegionFactory() throws Exception {
    config.setType(RegionType.REPLICATE);
    config.setDiskStoreName("diskstore");
    config.setKeyConstraint("java.lang.String");
    config.setValueConstraint("java.lang.Boolean");

    realizer.create(config, cache);
    verify(regionFactory).setKeyConstraint(String.class);
    verify(regionFactory).setValueConstraint(Boolean.class);
    verify(regionFactory).setDiskStoreName("diskstore");
    verify(regionFactory).setDataPolicy(DataPolicy.REPLICATE);
  }


  @Test
  public void createPartitionRegion() throws Exception {
    config.setType(RegionType.PARTITION);
    config.setRedundantCopies(2);

    realizer.create(config, cache);
    verify(regionFactory).setDataPolicy(DataPolicy.PARTITION);
    ArgumentCaptor<PartitionAttributes> argumentCaptor =
        ArgumentCaptor.forClass(PartitionAttributes.class);
    verify(regionFactory).setPartitionAttributes(argumentCaptor.capture());
    assertThat(argumentCaptor.getValue().getRedundantCopies()).isEqualTo(2);
  }

  @Test
  public void getRegionFactoryWhenValueNotSet() throws Exception {
    config.setType(RegionType.REPLICATE);
    config.setDiskStoreName(null);
    config.setKeyConstraint(null);
    config.setValueConstraint(null);

    realizer.create(config, cache);
    verify(regionFactory, never()).setKeyConstraint(any());
    verify(regionFactory, never()).setValueConstraint(any());
    verify(regionFactory, never()).setDiskStoreName(any());
    verify(regionFactory).setDataPolicy(DataPolicy.REPLICATE);
  }

  @Test
  public void regionDoesNotExistIfNotInCache() {
    config.setName("test");
    when(cache.getRegion("/test")).thenReturn(null);
    assertThat(realizer.exists(config, cache)).isFalse();
  }


  @Test
  public void regionDoesNotExistIfDestroyed() {
    when(cache.getRegion("/test")).thenReturn(region);
    when(region.isDestroyed()).thenReturn(true);
    assertThat(realizer.exists(config, cache)).isFalse();
  }

  @Test
  public void regionExistsDoesNotGetRuntimeInfo() {
    config.setName("test");
    when(cache.getRegion("/test")).thenReturn(region);
    when(region.isDestroyed()).thenReturn(false);
    boolean exists = realizer.exists(config, cache);
    assertThat(exists).isTrue();
    verify(region, never()).size();
  }
}
