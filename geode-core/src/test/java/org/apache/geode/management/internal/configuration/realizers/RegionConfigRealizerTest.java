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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;

public class RegionConfigRealizerTest {
  InternalCache cache;
  RegionFactory regionFactory;
  RegionConfigRealizer realizer;
  RegionConfigValidator validator;

  @Before
  public void setup() {
    cache = mock(InternalCache.class);
    validator = new RegionConfigValidator(cache);
    regionFactory = mock(RegionFactory.class);
    when(cache.createRegionFactory()).thenReturn(regionFactory);
    realizer = new RegionConfigRealizer();
  }

  @Test
  public void createsPartitionedInCache() {
    Region config = new Region();
    config.setName("regionName");
    config.setType(RegionType.PARTITION);
    validator.validate(CacheElementOperation.CREATE, config);
    realizer.create(config, cache);

    ArgumentCaptor<DataPolicy> dataPolicyArgumentCaptor = ArgumentCaptor.forClass(DataPolicy.class);
    verify(regionFactory).setDataPolicy(dataPolicyArgumentCaptor.capture());
    assertThat(dataPolicyArgumentCaptor.getValue()).isEqualTo(DataPolicy.PARTITION);

    verify(regionFactory).create("regionName");
  }

  @Test
  public void createsReplicateInCache() {
    Region config = new Region();
    config.setName("regionName");
    config.setType(RegionType.REPLICATE);
    validator.validate(CacheElementOperation.CREATE, config);
    realizer.create(config, cache);

    ArgumentCaptor<DataPolicy> dataPolicyArgumentCaptor = ArgumentCaptor.forClass(DataPolicy.class);
    verify(regionFactory).setDataPolicy(dataPolicyArgumentCaptor.capture());
    assertThat(dataPolicyArgumentCaptor.getValue()).isEqualTo(DataPolicy.REPLICATE);

    verify(regionFactory).create("regionName");
  }
}
