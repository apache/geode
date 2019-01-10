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
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.configuration.domain.ClusterRegionConfig;

public class ClusterRegionConfigRealizerTest {
  InternalCache cache;
  RegionFactory regionFactory;

  @Before
  public void setup() {
    cache = mock(InternalCache.class);
    regionFactory = mock(RegionFactory.class);
    when(cache.createRegionFactory()).thenReturn(regionFactory);
  }

  @Test
  public void createsPartitionedInCache() {
    ClusterRegionConfig config = new ClusterRegionConfig();
    config.setName("regionName");
    config.setRefid("PARTITION");

    ClusterRegionConfigRealizer subject = new ClusterRegionConfigRealizer(config);
    subject.createIn(cache);

    ArgumentCaptor<DataPolicy> dataPolicyArgumentCaptor = ArgumentCaptor.forClass(DataPolicy.class);
    verify(regionFactory).setDataPolicy(dataPolicyArgumentCaptor.capture());
    assertThat(dataPolicyArgumentCaptor.getValue()).isEqualTo(DataPolicy.PARTITION);

    ArgumentCaptor<PartitionAttributes> partitionAttributesArgumentCaptor =
        ArgumentCaptor.forClass(PartitionAttributes.class);
    verify(regionFactory).setPartitionAttributes(partitionAttributesArgumentCaptor.capture());

    PartitionAttributes partitionAttributes = partitionAttributesArgumentCaptor.getValue();
    assertThat(partitionAttributes).isNotNull();
    assertThat(partitionAttributes.getRedundantCopies()).isEqualTo(1);

    verify(regionFactory).create("regionName");
  }

  @Test
  public void createsReplicateInCache() {
    ClusterRegionConfig config = new ClusterRegionConfig();
    config.setName("regionName");
    config.setRefid("REPLICATE");

    ClusterRegionConfigRealizer subject = new ClusterRegionConfigRealizer(config);
    subject.createIn(cache);

    ArgumentCaptor<DataPolicy> dataPolicyArgumentCaptor = ArgumentCaptor.forClass(DataPolicy.class);
    verify(regionFactory).setDataPolicy(dataPolicyArgumentCaptor.capture());
    assertThat(dataPolicyArgumentCaptor.getValue()).isEqualTo(DataPolicy.REPLICATE);

    verify(regionFactory).create("regionName");
  }
}
