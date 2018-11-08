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

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.configuration.ClassNameType;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs;

public class RegionConfigFactoryTest {

  RegionConfigFactory subject;
  RegionFunctionArgs args;

  @Before
  public void setup() {
    subject = new RegionConfigFactory();
    args = new RegionFunctionArgs();
    args.setRegionPath("region-name");
  }

  @Test
  public void generatesConfigForRegion() {
    RegionConfig config = subject.generate(args);
    assertThat(config.getName()).isEqualTo("region-name");
  }

  @Test
  public void generatesConfigForSubRegion() {
    args.setRegionPath("region-name/subregion");

    RegionConfig config = subject.generate(args);
    assertThat(config.getName()).isEqualTo("subregion");
  }

  @Test
  public void generatesWithNoAttributes() {
    RegionConfig config = subject.generate(args);
    assertThat(config.getRegionAttributes()).isEmpty();
  }

  @Test
  public void generatesWithConstraintAttributes() {
    args.setKeyConstraint("key-const");
    args.setValueConstraint("value-const");

    RegionConfig config = subject.generate(args);
    assertThat(getRegionAttributeValue(config, t -> t.getKeyConstraint())).isEqualTo("key-const");
    assertThat(getRegionAttributeValue(config, t -> t.getValueConstraint()))
        .isEqualTo("value-const");
  }

  @Test
  public void generatesWithExpirationIdleTimeAttributes() {
    args.setRegionExpirationTTL(10, ExpirationAction.DESTROY);
    args.setRegionExpirationIdleTime(3, ExpirationAction.INVALIDATE);
    args.setEntryExpirationTTL(1, ExpirationAction.LOCAL_DESTROY);
    args.setEntryExpirationIdleTime(12, ExpirationAction.LOCAL_DESTROY);
    args.setEntryIdleTimeCustomExpiry(new ClassName<>("java.lang.String"));

    RegionConfig config = subject.generate(args);
    RegionAttributesType.RegionTimeToLive regionTimeToLive =
        (RegionAttributesType.RegionTimeToLive) getRegionAttributeValue(config,
            t -> t.getRegionTimeToLive());
    assertThat(regionTimeToLive.getExpirationAttributes().getTimeout()).isEqualTo("10");

    RegionAttributesType.EntryTimeToLive entryTimeToLive =
        (RegionAttributesType.EntryTimeToLive) getRegionAttributeValue(config,
            t -> t.getEntryTimeToLive());
    assertThat(entryTimeToLive.getExpirationAttributes().getAction())
        .isEqualTo(ExpirationAction.LOCAL_DESTROY.toXmlString());

    RegionAttributesType.EntryIdleTime entryIdleTime =
        (RegionAttributesType.EntryIdleTime) getRegionAttributeValue(config,
            t -> t.getEntryIdleTime());
    DeclarableType customExpiry = entryIdleTime.getExpirationAttributes().getCustomExpiry();
    assertThat(customExpiry.getClassName()).isEqualTo("java.lang.String");
    assertThat(entryIdleTime.getExpirationAttributes().getAction())
        .isEqualTo(ExpirationAction.LOCAL_DESTROY.toXmlString());
    assertThat(entryIdleTime.getExpirationAttributes().getTimeout())
        .isEqualTo("12");
  }

  @Test
  public void generatesWithDiskAttributes() {
    args.setDiskStore("disk-store");
    args.setDiskSynchronous(false);

    RegionConfig config = subject.generate(args);
    assertThat(getRegionAttributeValue(config, t -> t.getDiskStoreName())).isEqualTo("disk-store");
    assertThat(getRegionAttributeValue(config, t -> t.isDiskSynchronous())).isEqualTo(false);
  }

  @Test
  public void generatesWithPrAttributes() {
    args.setPartitionArgs("colo-with", 100,
        100L, 100, 100L,
        100L, 100, "java.lang.String");

    RegionConfig config = subject.generate(args);
    RegionAttributesType.PartitionAttributes partitionAttributes =
        (RegionAttributesType.PartitionAttributes) getRegionAttributeValue(config,
            t -> t.getPartitionAttributes());
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
    args.setStatisticsEnabled(false);
    args.setEnableAsyncConflation(false);
    args.setConcurrencyChecksEnabled(true);
    args.setEnableSubscriptionConflation(true);
    args.setMcastEnabled(false);
    args.setCloningEnabled(false);
    args.setOffHeap(true);
    RegionConfig config = subject.generate(args);

    assertThat(getRegionAttributeValue(config, t -> t.isStatisticsEnabled())).isEqualTo(false);
    assertThat(getRegionAttributeValue(config, t -> t.isEnableSubscriptionConflation()))
        .isEqualTo(true);
    assertThat(getRegionAttributeValue(config, t -> t.isConcurrencyChecksEnabled()))
        .isEqualTo(true);
    assertThat(getRegionAttributeValue(config, t -> t.isEnableSubscriptionConflation()))
        .isEqualTo(true);
    assertThat(getRegionAttributeValue(config, t -> t.isMulticastEnabled()))
        .isEqualTo(false);
    assertThat(getRegionAttributeValue(config, t -> t.isCloningEnabled())).isEqualTo(false);
    assertThat(getRegionAttributeValue(config, t -> t.isOffHeap())).isEqualTo(true);
  }

  @Test
  public void generatesWithGatewayFlags() {
    args.setGatewaySenderIds(new String[] {"some-id", "some-other-id"});
    RegionConfig config = subject.generate(args);

    assertThat((String) getRegionAttributeValue(config, t -> t.getGatewaySenderIds()))
        .contains("some-id");
    assertThat((String) getRegionAttributeValue(config, t -> t.getGatewaySenderIds()))
        .contains("some-other-id");
  }

  @Test
  public void generatesWithEvictionHeapPercentageFlags() {
    args.setEvictionAttributes(EvictionAction.LOCAL_DESTROY.toString(), null, null,
        "java.lang.String");
    RegionConfig config = subject.generate(args);

    RegionAttributesType.EvictionAttributes evictionAttributes =
        (RegionAttributesType.EvictionAttributes) getRegionAttributeValue(config,
            t -> t.getEvictionAttributes());
    assertThat(evictionAttributes).isNotNull();
    assertThat(evictionAttributes.getLruHeapPercentage().getAction())
        .isSameAs(EnumActionDestroyOverflow.LOCAL_DESTROY);
    assertThat(evictionAttributes.getLruHeapPercentage().getClassName())
        .isEqualTo("java.lang.String");
  }

  @Test
  public void generatesWithEvictionMaxMemory() {
    args.setEvictionAttributes(EvictionAction.LOCAL_DESTROY.toString(), 100, null,
        null);
    RegionConfig config = subject.generate(args);

    RegionAttributesType.EvictionAttributes evictionAttributes =
        (RegionAttributesType.EvictionAttributes) getRegionAttributeValue(config,
            t -> t.getEvictionAttributes());
    assertThat(evictionAttributes).isNotNull();
    assertThat(evictionAttributes.getLruMemorySize().getAction())
        .isSameAs(EnumActionDestroyOverflow.LOCAL_DESTROY);
    assertThat(evictionAttributes.getLruMemorySize().getMaximum()).isEqualTo("100");
  }

  @Test
  public void generatesWithEvictionMaxEntry() {
    args.setEvictionAttributes(EvictionAction.OVERFLOW_TO_DISK.toString(), null, 1,
        null);
    RegionConfig config = subject.generate(args);
    RegionAttributesType.EvictionAttributes evictionAttributes =
        (RegionAttributesType.EvictionAttributes) getRegionAttributeValue(config,
            t -> t.getEvictionAttributes());
    assertThat(evictionAttributes).isNotNull();
    assertThat(evictionAttributes.getLruEntryCount().getAction())
        .isSameAs(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
    assertThat(evictionAttributes.getLruEntryCount().getMaximum()).isEqualTo("1");
  }

  @Test
  public void generatesWithAsyncEventQueueIds() {
    args.setAsyncEventQueueIds(new String[] {"id-1", "id-2"});
    RegionConfig config = subject.generate(args);

    assertThat((String) getRegionAttributeValue(config, t -> t.getAsyncEventQueueIds()))
        .contains("id-1");
    assertThat((String) getRegionAttributeValue(config, t -> t.getAsyncEventQueueIds()))
        .contains("id-2");
  }

  @Test
  public void generatesWithCacheClasses() {
    args.setCacheListeners(new ClassName[] {new ClassName("java.lang.String")});
    args.setCacheLoader(new ClassName("java.lang.String"));
    args.setCacheWriter(new ClassName("java.lang.String"));
    RegionConfig config = subject.generate(args);

    List<DeclarableType> cacheListeners = config.getRegionAttributes().stream()
        .filter(a -> !a.getCacheListeners().isEmpty())
        .findFirst()
        .map(a -> a.getCacheListeners())
        .orElse(null);

    assertThat(cacheListeners).isNotNull();
    assertThat(cacheListeners.get(0).getClassName()).isEqualTo("java.lang.String");
    assertThat(
        ((DeclarableType) getRegionAttributeValue(config, t -> t.getCacheLoader())).getClassName())
            .isEqualTo("java.lang.String");
    assertThat(
        ((DeclarableType) getRegionAttributeValue(config, t -> t.getCacheWriter())).getClassName())
            .isEqualTo("java.lang.String");
  }

  @Test
  public void generatesWithOtherMiscSimpleFlags() {
    args.setCompressor("java.lang.String");
    args.setConcurrencyLevel(1);

    RegionConfig config = subject.generate(args);

    assertThat(
        ((ClassNameType) getRegionAttributeValue(config, t -> t.getCompressor())).getClassName())
            .isEqualTo("java.lang.String");
    assertThat(getRegionAttributeValue(config, t -> t.getConcurrencyLevel())).isEqualTo("1");
  }

  private Object getRegionAttributeValue(RegionConfig config, RegionAttributeGetFunction function) {
    return config.getRegionAttributes().stream()
        .findFirst()
        .map(a -> function.getValue(a))
        .orElse(null);
  }
}
