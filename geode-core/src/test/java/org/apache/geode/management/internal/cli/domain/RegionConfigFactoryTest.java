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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;

public class RegionConfigFactoryTest {

  RegionConfigFactory subject;
  RegionConfig config;

  String regionPath;
  String keyConstraint;
  String valueConstraint;
  Boolean statisticsEnabled;
  Integer entryExpirationIdleTime;
  ExpirationAction entryExpirationIdleAction;
  Integer entryExpirationTTL;
  ExpirationAction entryExpirationTTLAction;
  ClassName<CustomExpiry> entryIdleTimeCustomExpiry;
  ClassName<CustomExpiry> entryTTLCustomExpiry;

  Integer regionExpirationIdleTime;
  ExpirationAction regionExpirationIdleAction;
  Integer regionExpirationTTL;
  ExpirationAction regionExpirationTTLAction;

  String evictionAction;
  Integer evictionMaxMemory;
  Integer evictionEntryCount;
  String evictionObjectSizer;

  String diskStore;
  Boolean diskSynchronous;
  Boolean enableAsyncConflation;
  Boolean enableSubscriptionConflation;
  Set<ClassName<CacheListener>> cacheListeners;
  ClassName<CacheLoader> cacheLoader;
  ClassName<CacheWriter> cacheWriter;
  Set<String> asyncEventQueueIds;
  Set<String> gatewaySenderIds;
  Boolean concurrencyChecksEnabled;
  Boolean cloningEnabled;
  Boolean mcastEnabled;
  Integer concurrencyLevel;
  PartitionArgs partitionArgs;
  String compressor;
  Boolean offHeap;
  RegionAttributes<?, ?> regionAttributes;

  @Before
  public void setup() {
    subject = new RegionConfigFactory();
    regionPath = "region-name";
    keyConstraint = null;
    valueConstraint = null;
    statisticsEnabled = null;
    entryExpirationIdleTime = null;
    entryExpirationIdleAction = null;
    entryExpirationTTL = null;
    entryExpirationTTLAction = null;
    entryIdleTimeCustomExpiry = null;
    entryTTLCustomExpiry = null;
    regionExpirationIdleTime = null;
    regionExpirationIdleAction = null;
    regionExpirationTTL = null;
    regionExpirationTTLAction = null;
    evictionAction = null;
    evictionMaxMemory = null;
    evictionEntryCount = null;
    evictionObjectSizer = null;
    diskStore = null;
    diskSynchronous = null;
    enableAsyncConflation = null;
    enableSubscriptionConflation = null;
    cacheListeners = null;
    cacheLoader = null;
    cacheWriter = null;
    asyncEventQueueIds = null;
    gatewaySenderIds = null;
    concurrencyChecksEnabled = null;
    cloningEnabled = null;
    mcastEnabled = null;
    concurrencyLevel = null;
    partitionArgs = null;
    compressor = null;
    offHeap = null;
    regionAttributes = null;
  }

  private void generate() {
    config = subject.generate(regionPath, keyConstraint, valueConstraint, statisticsEnabled,
        entryExpirationIdleTime, entryExpirationIdleAction, entryExpirationTTL,
        entryExpirationTTLAction,
        entryIdleTimeCustomExpiry, entryTTLCustomExpiry, regionExpirationIdleTime,
        regionExpirationIdleAction,
        regionExpirationTTL, regionExpirationTTLAction, evictionAction, evictionMaxMemory,
        evictionEntryCount,
        evictionObjectSizer, diskStore, diskSynchronous, enableAsyncConflation,
        enableSubscriptionConflation,
        cacheListeners, cacheLoader,
        cacheWriter,
        asyncEventQueueIds, gatewaySenderIds, concurrencyChecksEnabled, cloningEnabled,
        mcastEnabled,
        concurrencyLevel, partitionArgs, compressor, offHeap, regionAttributes);
  }

  @Test
  public void generatesConfigForRegion() {
    generate();
    assertThat(config.getName()).isEqualTo("region-name");
  }

  @Test
  public void generatesConfigForSubRegion() {
    regionPath = "region-name/subregion";

    generate();
    assertThat(config.getName()).isEqualTo("subregion");
  }

  @Test
  public void generatesWithConstraintAttributes() {
    keyConstraint = "key-const";
    valueConstraint = "value-const";

    generate();
    assertThat(config.getRegionAttributes().getKeyConstraint()).isEqualTo("key-const");
    assertThat(config.getRegionAttributes().getValueConstraint())
        .isEqualTo("value-const");
  }

  @Test
  public void generatesWithExpirationIdleTimeAttributes() {
    regionExpirationTTL = 10;
    regionExpirationTTLAction = ExpirationAction.DESTROY;

    regionExpirationIdleTime = 3;
    regionExpirationIdleAction = ExpirationAction.INVALIDATE;

    entryExpirationTTL = 1;
    entryExpirationTTLAction = ExpirationAction.LOCAL_DESTROY;

    entryExpirationIdleTime = 12;
    entryExpirationIdleAction = ExpirationAction.LOCAL_DESTROY;
    entryIdleTimeCustomExpiry = new ClassName<>("java.lang.String");

    generate();
    RegionAttributesType.ExpirationAttributesType regionTimeToLive =
        config.getRegionAttributes().getRegionTimeToLive();
    assertThat(regionTimeToLive.getTimeout()).isEqualTo("10");

    RegionAttributesType.ExpirationAttributesType entryTimeToLive =
        config.getRegionAttributes().getEntryTimeToLive();
    assertThat(entryTimeToLive.getAction())
        .isEqualTo(ExpirationAction.LOCAL_DESTROY.toXmlString());

    RegionAttributesType.ExpirationAttributesType entryIdleTime =
        config.getRegionAttributes().getEntryIdleTime();
    DeclarableType customExpiry = entryIdleTime.getCustomExpiry();
    assertThat(customExpiry.getClassName()).isEqualTo("java.lang.String");
    assertThat(entryIdleTime.getAction())
        .isEqualTo(ExpirationAction.LOCAL_DESTROY.toXmlString());
    assertThat(entryIdleTime.getTimeout())
        .isEqualTo("12");
  }

  @Test
  public void generatesWithDiskAttributes() {
    diskStore = "disk-store";
    diskSynchronous = false;

    generate();
    assertThat(config.getRegionAttributes().getDiskStoreName()).isEqualTo("disk-store");
    assertThat(config.getRegionAttributes().isDiskSynchronous()).isEqualTo(false);
  }

  @Test
  public void generatesWithPrAttributes() {
    partitionArgs = new PartitionArgs("colo-with", 100,
        100L, 100, 100L,
        100L, 100, "java.lang.String");

    generate();
    RegionAttributesType.PartitionAttributes partitionAttributes =
        config.getRegionAttributes().getPartitionAttributes();
    assertThat(partitionAttributes).isNotNull();
    assertThat(partitionAttributes.getColocatedWith()).isEqualTo("colo-with");
    assertThat(partitionAttributes.getLocalMaxMemory()).isEqualTo("100");
    assertThat(partitionAttributes.getRecoveryDelay()).isEqualTo("100");
    assertThat(partitionAttributes.getRedundantCopies()).isEqualTo("100");
    assertThat(partitionAttributes.getStartupRecoveryDelay()).isEqualTo("100");
    assertThat(partitionAttributes.getTotalMaxMemory()).isEqualTo("100");
    assertThat(partitionAttributes.getTotalNumBuckets()).isEqualTo("100");

    DeclarableType partitionResolverType = partitionAttributes.getPartitionResolver();
    assertThat(partitionResolverType.getClassName()).isEqualTo("java.lang.String");
  }

  @Test
  public void generatesWithMiscBooleanFlags() {
    statisticsEnabled = false;
    enableAsyncConflation = false;
    concurrencyChecksEnabled = true;
    enableSubscriptionConflation = true;
    mcastEnabled = false;
    cloningEnabled = false;
    offHeap = true;
    generate();

    assertThat(config.getRegionAttributes().isStatisticsEnabled()).isEqualTo(false);
    assertThat(config.getRegionAttributes().isEnableSubscriptionConflation())
        .isEqualTo(true);
    assertThat(config.getRegionAttributes().isConcurrencyChecksEnabled())
        .isEqualTo(true);
    assertThat(config.getRegionAttributes().isEnableSubscriptionConflation())
        .isEqualTo(true);
    assertThat(config.getRegionAttributes().isMulticastEnabled())
        .isEqualTo(false);
    assertThat(config.getRegionAttributes().isCloningEnabled()).isEqualTo(false);
    assertThat(config.getRegionAttributes().isOffHeap()).isEqualTo(true);
  }

  @Test
  public void generatesWithGatewayFlags() {
    gatewaySenderIds =
        Arrays.stream(new String[] {"some-id", "some-other-id"}).collect(Collectors.toSet());
    generate();

    assertThat(config.getRegionAttributes().getGatewaySenderIds())
        .contains("some-id");
    assertThat(config.getRegionAttributes().getGatewaySenderIds())
        .contains("some-other-id");
  }

  @Test
  public void generatesWithEvictionHeapPercentageFlags() {
    evictionAction = EvictionAction.LOCAL_DESTROY.toString();
    evictionObjectSizer = "java.lang.String";
    generate();

    RegionAttributesType.EvictionAttributes evictionAttributes =
        config.getRegionAttributes().getEvictionAttributes();
    assertThat(evictionAttributes).isNotNull();
    assertThat(evictionAttributes.getLruHeapPercentage().getAction())
        .isSameAs(EnumActionDestroyOverflow.LOCAL_DESTROY);
    assertThat(evictionAttributes.getLruHeapPercentage().getClassName())
        .isEqualTo("java.lang.String");
  }

  @Test
  public void generatesWithEvictionMaxMemory() {
    evictionAction = EvictionAction.LOCAL_DESTROY.toString();
    evictionMaxMemory = 100;
    generate();

    RegionAttributesType.EvictionAttributes evictionAttributes =
        config.getRegionAttributes().getEvictionAttributes();
    assertThat(evictionAttributes).isNotNull();
    assertThat(evictionAttributes.getLruMemorySize().getAction())
        .isSameAs(EnumActionDestroyOverflow.LOCAL_DESTROY);
    assertThat(evictionAttributes.getLruMemorySize().getMaximum()).isEqualTo("100");
  }

  @Test
  public void generatesWithEvictionMaxEntry() {
    evictionAction = EvictionAction.OVERFLOW_TO_DISK.toString();
    evictionEntryCount = 1;
    generate();
    RegionAttributesType.EvictionAttributes evictionAttributes =
        config.getRegionAttributes().getEvictionAttributes();
    assertThat(evictionAttributes).isNotNull();
    assertThat(evictionAttributes.getLruEntryCount().getAction())
        .isSameAs(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
    assertThat(evictionAttributes.getLruEntryCount().getMaximum()).isEqualTo("1");
  }

  @Test
  public void generatesWithAsyncEventQueueIds() {
    asyncEventQueueIds = Arrays.stream(new String[] {"id-1", "id-2"}).collect(Collectors.toSet());
    generate();

    assertThat(config.getRegionAttributes().getAsyncEventQueueIds())
        .contains("id-1");
    assertThat(config.getRegionAttributes().getAsyncEventQueueIds())
        .contains("id-2");
  }

  @Test
  public void generatesWithCacheClasses() {
    cacheListeners = new HashSet<>();
    cacheListeners.add(new ClassName<>("java.lang.String"));
    cacheLoader = new ClassName("java.lang.String");
    cacheWriter = new ClassName("java.lang.String");
    generate();

    List<DeclarableType> cacheListeners = config.getRegionAttributes().getCacheListeners();

    assertThat(cacheListeners).isNotNull();
    assertThat(cacheListeners.get(0).getClassName()).isEqualTo("java.lang.String");
    assertThat(
        config.getRegionAttributes().getCacheLoader().getClassName())
            .isEqualTo("java.lang.String");
    assertThat(
        config.getRegionAttributes().getCacheWriter().getClassName())
            .isEqualTo("java.lang.String");
  }

  @Test
  public void generatesWithOtherMiscSimpleFlags() {
    compressor = "java.lang.String";
    concurrencyLevel = 1;
    generate();

    assertThat(
        config.getRegionAttributes().getCompressor().getClassName())
            .isEqualTo("java.lang.String");
    assertThat(config.getRegionAttributes().getConcurrencyLevel()).isEqualTo("1");
  }

}
