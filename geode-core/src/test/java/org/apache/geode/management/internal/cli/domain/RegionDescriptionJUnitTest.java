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

package org.apache.geode.management.internal.cli.domain;

import static org.apache.geode.cache.DataPolicy.NORMAL;
import static org.apache.geode.cache.Scope.DISTRIBUTED_ACK;
import static org.apache.geode.cache.Scope.LOCAL;
import static org.apache.geode.management.internal.cli.domain.RegionDescription.findCommon;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionDescriptionJUnitTest {
  private static final String evictionKeyShared = "sharedEvictionKey";
  private static final String partKeyShared = "sharedPartitionKey";
  private static final String regKeyShared = "sharedRegionKey";
  private static final String evictionValueShared = "sharedEvictionValue";
  private static final String partValueShared = "sharedPartitionValue";
  private static final String regValueShared = "sharedRegionValue";

  private static final String evictionKeyA = "uniqueEvictionKey_A";
  private static final String partKeyA = "uniquePartitionKey_A";
  private static final String regKeyA = "uniqueRegionKey_A";
  private static final String evictionValueA = "uniqueEvictionValue_A";
  private static final String partValueA = "uniquePartitionValue_A";
  private static final String regValueA = "uniqueRegionValue_A";

  private static final String evictionKeyB = "uniqueEvictionKey_B";
  private static final String partKeyB = "uniquePartitionKey_B";
  private static final String regKeyB = "uniqueRegionKey_B";
  private static final String evictionValueB = "uniqueEvictionValue_B";
  private static final String partValueB = "uniquePartitionValue_B";
  private static final String regValueB = "uniqueRegionValue_B";

  public static final String regionName = "mockRegion1";

  @Test
  public void findCommonRemovesUnsharedKeys() {
    Map<String, String> commonMap = new HashMap<>();
    commonMap.put(evictionKeyShared, evictionValueShared);
    commonMap.put(partKeyShared, partValueShared);
    commonMap.put(regKeyShared, regValueShared);
    commonMap.put(evictionKeyA, evictionValueA);
    commonMap.put(partKeyA, partValueA);

    Map<String, String> comparisonMap = new HashMap<>();
    comparisonMap.put(evictionKeyShared, evictionValueShared);
    comparisonMap.put(partKeyShared, partValueShared);
    comparisonMap.put(regKeyShared, regValueShared);
    comparisonMap.put(evictionKeyB, evictionValueB);
    comparisonMap.put(regKeyB, regValueB);

    findCommon(commonMap, comparisonMap);

    assertThat(commonMap).containsOnlyKeys(evictionKeyShared, partKeyShared, regKeyShared);
  }

  @Test
  public void findCommonRemovesKeysWithDisagreeingValues() {
    Map<String, String> commonMap = new HashMap<>();
    commonMap.put(evictionKeyShared, evictionValueShared);
    commonMap.put(partKeyShared, partValueA);
    commonMap.put(regKeyShared, regValueA);

    Map<String, String> comparisonMap = new HashMap<>();
    comparisonMap.put(evictionKeyShared, evictionValueShared);
    comparisonMap.put(partKeyShared, partValueB);
    comparisonMap.put(regKeyShared, regValueB);

    findCommon(commonMap, comparisonMap);

    assertThat(commonMap).containsOnlyKeys(evictionKeyShared);
  }

  @Test
  public void findCommonRemovesDisagreeingKeysInvolvingNull() {
    Map<String, String> commonMap = new HashMap<>();
    commonMap.put(evictionKeyShared, evictionValueShared);
    commonMap.put(partKeyShared, partValueA);
    commonMap.put(regKeyShared, null);

    Map<String, String> comparisonMap = new HashMap<>();
    comparisonMap.put(evictionKeyShared, evictionValueShared);
    comparisonMap.put(partKeyShared, null);
    comparisonMap.put(regKeyShared, regValueB);

    findCommon(commonMap, comparisonMap);

    assertThat(commonMap).containsOnlyKeys(evictionKeyShared);
  }


  @Test
  public void singleAddDefinesDescription() {
    RegionDescriptionPerMember mockA = getMockRegionDescriptionPerMember_A();
    RegionDescription description = new RegionDescription();
    description.add(mockA);

    assertThat(description.getCndEvictionAttributes())
        .isEqualTo(mockA.getNonDefaultEvictionAttributes());
    assertThat(description.getCndPartitionAttributes())
        .isEqualTo(mockA.getNonDefaultPartitionAttributes());
    assertThat(description.getCndRegionAttributes())
        .isEqualTo(mockA.getNonDefaultRegionAttributes());
  }

  @Test
  public void multipleAddsMergeAsExpected() {
    RegionDescriptionPerMember mockA = getMockRegionDescriptionPerMember_A();
    RegionDescriptionPerMember mockB = getMockRegionDescriptionPerMember_B();
    RegionDescription description = new RegionDescription();
    description.add(mockA);
    description.add(mockB);

    Map<String, String> sharedEviction = new HashMap<>();
    sharedEviction.put(evictionKeyShared, evictionValueShared);
    Map<String, String> sharedRegion = new HashMap<>();
    sharedRegion.put(regKeyShared, regValueShared);
    Map<String, String> sharedPartition = new HashMap<>();
    sharedPartition.put(partKeyShared, partValueShared);

    assertThat(description.getCndEvictionAttributes()).isEqualTo(sharedEviction);
    assertThat(description.getCndPartitionAttributes()).isEqualTo(sharedPartition);
    assertThat(description.getCndRegionAttributes()).isEqualTo(sharedRegion);

    assertThat(description.getRegionDescriptionPerMemberMap())
        .containsOnlyKeys(mockA.getHostingMember(), mockB.getHostingMember())
        .containsEntry(mockA.getHostingMember(), mockA)
        .containsEntry(mockB.getHostingMember(), mockB);
  }

  @Test
  public void outOfScopeAddGetsIgnored() {
    RegionDescriptionPerMember mockA = getMockRegionDescriptionPerMember_A();
    RegionDescriptionPerMember mockB = getMockRegionDescriptionPerMember_OutOfScope();
    RegionDescription description = new RegionDescription();
    description.add(mockA);
    description.add(mockB);

    assertThat(description.getCndEvictionAttributes())
        .isEqualTo(mockA.getNonDefaultEvictionAttributes());
    assertThat(description.getCndPartitionAttributes())
        .isEqualTo(mockA.getNonDefaultPartitionAttributes());
    assertThat(description.getCndRegionAttributes())
        .isEqualTo(mockA.getNonDefaultRegionAttributes());
  }

  private RegionDescriptionPerMember getMockRegionDescriptionPerMember_A() {
    Map<String, String> mockNonDefaultEvictionAttributes = new HashMap<>();
    mockNonDefaultEvictionAttributes.put(evictionKeyShared, evictionValueShared);
    mockNonDefaultEvictionAttributes.put(evictionKeyA, evictionValueA);

    Map<String, String> mockNonDefaultPartitionAttributes = new HashMap<>();
    mockNonDefaultPartitionAttributes.put(partKeyShared, partValueShared);
    mockNonDefaultPartitionAttributes.put(partKeyA, partValueA);

    Map<String, String> mockNonDefaultRegionAttributes = new HashMap<>();
    mockNonDefaultRegionAttributes.put(regKeyShared, regValueShared);
    mockNonDefaultRegionAttributes.put(regKeyA, regValueA);

    RegionDescriptionPerMember mockDescPerMember = mock(RegionDescriptionPerMember.class);

    when(mockDescPerMember.getNonDefaultEvictionAttributes())
        .thenReturn(mockNonDefaultEvictionAttributes);
    when(mockDescPerMember.getNonDefaultPartitionAttributes())
        .thenReturn(mockNonDefaultPartitionAttributes);
    when(mockDescPerMember.getNonDefaultRegionAttributes())
        .thenReturn(mockNonDefaultRegionAttributes);
    when(mockDescPerMember.getHostingMember()).thenReturn("mockMemberA");
    when(mockDescPerMember.getScope()).thenReturn(DISTRIBUTED_ACK);
    when(mockDescPerMember.getDataPolicy()).thenReturn(NORMAL);
    when(mockDescPerMember.getName()).thenReturn(regionName);

    return mockDescPerMember;
  }

  private RegionDescriptionPerMember getMockRegionDescriptionPerMember_B() {
    Map<String, String> mockNonDefaultEvictionAttributes = new HashMap<>();
    mockNonDefaultEvictionAttributes.put(evictionKeyShared, evictionValueShared);
    mockNonDefaultEvictionAttributes.put(evictionKeyB, evictionValueB);

    Map<String, String> mockNonDefaultPartitionAttributes = new HashMap<>();
    mockNonDefaultPartitionAttributes.put(partKeyShared, partValueShared);
    mockNonDefaultPartitionAttributes.put(partKeyB, partValueB);

    Map<String, String> mockNonDefaultRegionAttributes = new HashMap<>();
    mockNonDefaultRegionAttributes.put(regKeyShared, regValueShared);
    mockNonDefaultRegionAttributes.put(regKeyB, regValueB);

    RegionDescriptionPerMember mockDescPerMember = mock(RegionDescriptionPerMember.class);

    when(mockDescPerMember.getNonDefaultEvictionAttributes())
        .thenReturn(mockNonDefaultEvictionAttributes);
    when(mockDescPerMember.getNonDefaultPartitionAttributes())
        .thenReturn(mockNonDefaultPartitionAttributes);
    when(mockDescPerMember.getNonDefaultRegionAttributes())
        .thenReturn(mockNonDefaultRegionAttributes);
    when(mockDescPerMember.getHostingMember()).thenReturn("mockMemberB");
    when(mockDescPerMember.getScope()).thenReturn(DISTRIBUTED_ACK);
    when(mockDescPerMember.getDataPolicy()).thenReturn(NORMAL);
    when(mockDescPerMember.getName()).thenReturn(regionName);

    return mockDescPerMember;
  }

  private RegionDescriptionPerMember getMockRegionDescriptionPerMember_OutOfScope() {
    Map<String, String> mockNonDefaultEvictionAttributes = new HashMap<>();
    mockNonDefaultEvictionAttributes.put(evictionKeyShared, evictionValueShared);

    Map<String, String> mockNonDefaultPartitionAttributes = new HashMap<>();
    mockNonDefaultPartitionAttributes.put(partKeyShared, partValueShared);

    Map<String, String> mockNonDefaultRegionAttributes = new HashMap<>();
    mockNonDefaultRegionAttributes.put(regKeyShared, regValueShared);

    RegionDescriptionPerMember mockDescPerMember = mock(RegionDescriptionPerMember.class);

    when(mockDescPerMember.getNonDefaultEvictionAttributes())
        .thenReturn(mockNonDefaultEvictionAttributes);
    when(mockDescPerMember.getNonDefaultPartitionAttributes())
        .thenReturn(mockNonDefaultPartitionAttributes);
    when(mockDescPerMember.getNonDefaultRegionAttributes())
        .thenReturn(mockNonDefaultRegionAttributes);
    when(mockDescPerMember.getHostingMember()).thenReturn("mockMemberC");
    when(mockDescPerMember.getScope()).thenReturn(LOCAL);
    when(mockDescPerMember.getDataPolicy()).thenReturn(NORMAL);
    when(mockDescPerMember.getName()).thenReturn(regionName);

    return mockDescPerMember;
  }


}
