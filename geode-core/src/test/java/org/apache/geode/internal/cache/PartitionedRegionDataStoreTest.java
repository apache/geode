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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;


public class PartitionedRegionDataStoreTest {
  @Mock
  private PartitionedRegion partitionedRegion;
  @Mock
  private PartitionedRegion childRegion1;
  @Mock
  private PartitionedRegion childRegion2;
  @Mock
  private PartitionedRegion grandChildRegion1_1;
  @Mock
  private PartitionedRegion grandChildRegion1_2;
  @Mock
  private PartitionedRegion grandChildRegion2_1;
  @Mock
  private PartitionedRegion grandChildRegion2_2;
  @Mock
  private PartitionedRegion grandChildRegion2_3;
  @Mock
  private PartitionedRegionDataStore colocatedRegionDateStore;
  @Mock
  private PartitionedRegionDataStore grandChildRegionDateStore2_3;
  private final int bucketId = 29;

  @Before
  public void setup() {
    initMocks(this);

    when(partitionedRegion.isInitialized()).thenReturn(true);
    when(childRegion1.isInitialized()).thenReturn(true);
    when(childRegion2.isInitialized()).thenReturn(true);
    when(grandChildRegion1_1.isInitialized()).thenReturn(true);
    when(grandChildRegion1_2.isInitialized()).thenReturn(true);
    when(grandChildRegion2_1.isInitialized()).thenReturn(true);
    when(grandChildRegion2_2.isInitialized()).thenReturn(true);
    when(grandChildRegion2_3.isInitialized()).thenReturn(true);

    when(childRegion1.getDataStore()).thenReturn(colocatedRegionDateStore);
    when(childRegion2.getDataStore()).thenReturn(colocatedRegionDateStore);
    when(grandChildRegion1_1.getDataStore()).thenReturn(colocatedRegionDateStore);
    when(grandChildRegion1_2.getDataStore()).thenReturn(colocatedRegionDateStore);
    when(grandChildRegion2_1.getDataStore()).thenReturn(colocatedRegionDateStore);
    when(grandChildRegion2_2.getDataStore()).thenReturn(colocatedRegionDateStore);
    when(grandChildRegion2_3.getDataStore()).thenReturn(grandChildRegionDateStore2_3);

    when(colocatedRegionDateStore.isColocationComplete(bucketId)).thenReturn(true);
    when(grandChildRegionDateStore2_3.isColocationComplete(bucketId)).thenReturn(true);
  }

  @Test
  public void initializedPartitionedRegionWithoutColocationReturnsRegionReady() {
    PartitionedRegionDataStore partitionedRegionDataStore = spy(new PartitionedRegionDataStore());
    List<PartitionedRegion> colocatedChildRegions = new ArrayList<>();

    doReturn(colocatedChildRegions).when(partitionedRegionDataStore)
        .getColocatedChildRegions(partitionedRegion);

    assertThat(partitionedRegionDataStore.isPartitionedRegionReady(partitionedRegion, bucketId))
        .isTrue();
  }

  @Test
  public void notInitializedPartitionedRegionWithoutColocationReturnsRegionNotReady() {
    PartitionedRegionDataStore partitionedRegionDataStore = spy(new PartitionedRegionDataStore());
    List<PartitionedRegion> colocatedChildRegions = new ArrayList<>();

    doReturn(colocatedChildRegions).when(partitionedRegionDataStore)
        .getColocatedChildRegions(partitionedRegion);
    when(partitionedRegion.isInitialized()).thenReturn(false);

    assertThat(partitionedRegionDataStore.isPartitionedRegionReady(partitionedRegion, bucketId))
        .isFalse();
  }

  @Test
  public void returnRegionReadyIfAllColocatedRegionsAreReady() {
    PartitionedRegionDataStore partitionedRegionDataStore = spy(new PartitionedRegionDataStore());

    setupColocatedRegions(partitionedRegionDataStore);

    assertThat(partitionedRegionDataStore.isPartitionedRegionReady(partitionedRegion, bucketId))
        .isTrue();
  }

  private void setupColocatedRegions(PartitionedRegionDataStore partitionedRegionDataStore) {
    List<PartitionedRegion> colocatedChildRegions = new ArrayList<>();
    colocatedChildRegions.add(childRegion1);
    colocatedChildRegions.add(childRegion2);
    doReturn(colocatedChildRegions).when(partitionedRegionDataStore)
        .getColocatedChildRegions(partitionedRegion);

    List<PartitionedRegion> childRegion1ColocatedRegions = new ArrayList<>();
    colocatedChildRegions.add(grandChildRegion1_1);
    colocatedChildRegions.add(grandChildRegion1_2);
    doReturn(childRegion1ColocatedRegions).when(partitionedRegionDataStore)
        .getColocatedChildRegions(childRegion1);

    List<PartitionedRegion> childRegion2ColocatedRegions = new ArrayList<>();
    colocatedChildRegions.add(grandChildRegion2_1);
    colocatedChildRegions.add(grandChildRegion2_2);
    colocatedChildRegions.add(grandChildRegion2_3);
    doReturn(childRegion2ColocatedRegions).when(partitionedRegionDataStore)
        .getColocatedChildRegions(childRegion2);

    List<PartitionedRegion> emptyList = new ArrayList<>();
    doReturn(emptyList).when(partitionedRegionDataStore)
        .getColocatedChildRegions(grandChildRegion1_1);
    doReturn(emptyList).when(partitionedRegionDataStore)
        .getColocatedChildRegions(grandChildRegion1_2);
    doReturn(emptyList).when(partitionedRegionDataStore)
        .getColocatedChildRegions(grandChildRegion2_1);
    doReturn(emptyList).when(partitionedRegionDataStore)
        .getColocatedChildRegions(grandChildRegion2_2);
    doReturn(emptyList).when(partitionedRegionDataStore)
        .getColocatedChildRegions(grandChildRegion2_3);
  }

  @Test
  public void returnRegionNotReadyIfColocationNotCompletedForAColocatedRegion() {
    PartitionedRegionDataStore partitionedRegionDataStore = spy(new PartitionedRegionDataStore());

    setupColocatedRegions(partitionedRegionDataStore);
    when(grandChildRegionDateStore2_3.isColocationComplete(bucketId)).thenReturn(false);

    assertThat(partitionedRegionDataStore.isPartitionedRegionReady(partitionedRegion, bucketId))
        .isFalse();
  }

  @Test
  public void returnRegionNotReadyIfAColocatedRegionIsNotInitialized() {
    PartitionedRegionDataStore partitionedRegionDataStore = spy(new PartitionedRegionDataStore());

    setupColocatedRegions(partitionedRegionDataStore);
    when(grandChildRegion2_2.isInitialized()).thenReturn(false);

    assertThat(partitionedRegionDataStore.isPartitionedRegionReady(partitionedRegion, bucketId))
        .isFalse();
  }
}
