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

package org.apache.geode.management.internal.configuration.converters;

import static org.apache.geode.lang.Identifiable.find;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.ParameterType;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;

public class RegionConverterTest {
  private RegionConverter converter;
  private RegionConfig config;
  private Region region;

  @Before
  public void before() throws Exception {
    converter = new RegionConverter();
    config = new RegionConfig();
    region = new Region();
  }

  @Test
  public void fromXmlWithNameType() {
    config.setName("test");
    config.setRegionAttributes(converter.createRegionAttributesByType("REPLICATE"));

    Region region = converter.fromXmlObject(config);
    assertThat(region.getName()).isEqualTo("test");
    assertThat(region.getType()).isEqualTo(RegionType.REPLICATE);
  }

  @Test
  public void fromXmlWithAll() {
    config.setName("test");
    config.setRegionAttributes(converter.createRegionAttributesByType("PARTITION"));

    RegionAttributesType attributesType = config.getRegionAttributes();
    attributesType.setValueConstraint("foo");
    attributesType.setKeyConstraint("bar");
    attributesType.setDiskStoreName("diskstore");
    config.setRegionAttributes(attributesType);

    RegionAttributesType.PartitionAttributes partitionAttributes =
        new RegionAttributesType.PartitionAttributes();
    partitionAttributes.setRedundantCopies("2");
    attributesType.setPartitionAttributes(partitionAttributes);

    Region region = converter.fromXmlObject(config);
    assertThat(region.getName()).isEqualTo("test");
    assertThat(region.getType()).isEqualTo(RegionType.PARTITION);
    assertThat(region.getValueConstraint()).isEqualTo("foo");
    assertThat(region.getKeyConstraint()).isEqualTo("bar");
    assertThat(region.getDiskStoreName()).isEqualTo("diskstore");
    assertThat(region.getRedundantCopies()).isEqualTo(2);
    assertThat(region.getExpirations()).isNull();
  }

  @Test
  public void fromXmlWithLocalType() {
    config.setName("test");
    config.setRegionAttributes(converter.createRegionAttributesByType("LOCAL"));
    assertThat(converter.fromXmlObject(config).getType()).isEqualTo(RegionType.LEGACY);
  }

  @Test
  public void fromXmlWithNullType() {
    config.setName("test");
    config.setType((String) null);
    assertThat(converter.fromXmlObject(config).getType()).isEqualTo(RegionType.LEGACY);
  }

  @Test
  public void fromConfig() {
    region.setName("test");
    region.setType(RegionType.PARTITION);
    region.setValueConstraint("foo");
    region.setKeyConstraint("bar");
    region.setDiskStoreName("diskstore");
    region.setRedundantCopies(2);
    RegionConfig config = converter.fromConfigObject(region);
    assertThat(config.getName()).isEqualTo("test");
    assertThat(config.getType()).isEqualTo("PARTITION");
    RegionAttributesType regionAttributes = config.getRegionAttributes();
    assertThat(regionAttributes.getDataPolicy()).isEqualTo(RegionAttributesDataPolicy.PARTITION);
    assertThat(regionAttributes.getKeyConstraint()).isEqualTo("bar");
    assertThat(regionAttributes.getValueConstraint()).isEqualTo("foo");
    assertThat(regionAttributes.getDiskStoreName()).isEqualTo("diskstore");
    assertThat(regionAttributes.getPartitionAttributes().getRedundantCopies()).isEqualTo("2");
    assertThat(regionAttributes.isStatisticsEnabled()).isTrue();
    assertThat(regionAttributes.getEntryIdleTime()).isNull();
    assertThat(regionAttributes.getEntryTimeToLive()).isNull();
  }

  @Test
  public void checkDefaultRegionAttributesForShortcuts() throws Exception {
    URL xmlResource = RegionConverterTest.class.getResource("RegionConverterTest.xml");
    assertThat(xmlResource).isNotNull();
    CacheConfig master =
        new JAXBService(CacheConfig.class)
            .unMarshall(FileUtils.readFileToString(new File(xmlResource.getFile()), "UTF-8"));
    RegionShortcut[] shortcuts = RegionShortcut.values();
    for (RegionShortcut shortcut : shortcuts) {
      RegionConfig config = new RegionConfig();
      config.setType(shortcut.name());
      config.setName(shortcut.name());
      config.setRegionAttributes(converter.createRegionAttributesByType(shortcut.name()));
      RegionConfig masterRegion = find(master.getRegions(), shortcut.name());
      assertThat(config).isEqualToComparingFieldByFieldRecursively(masterRegion);
    }
  }

  @Test
  public void getRegionType() {
    assertThat(converter.getRegionType("ABC", null))
        .isEqualTo(RegionType.LEGACY);

    assertThat(converter.getRegionType(null, null)).isEqualTo(RegionType.LEGACY);

    RegionAttributesType regionAttributes = new RegionAttributesType();
    assertThat(converter.getRegionType(null, regionAttributes)).isEqualTo(RegionType.LEGACY);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
    assertThat(converter.getRegionType(null, regionAttributes)).isEqualTo(RegionType.PARTITION);

    RegionAttributesType.PartitionAttributes pAttributes =
        new RegionAttributesType.PartitionAttributes();
    pAttributes.setLocalMaxMemory("20000");
    regionAttributes.setPartitionAttributes(pAttributes);
    assertThat(converter.getRegionType(null, regionAttributes)).isEqualTo(RegionType.PARTITION);
    assertThat(converter.getRegionType("PARTITION_REDUNDANT", regionAttributes))
        .isEqualTo(RegionType.PARTITION);

    pAttributes.setLocalMaxMemory("0");
    assertThat(converter.getRegionType(null, regionAttributes))
        .isEqualTo(RegionType.PARTITION_PROXY);
    assertThat(converter.getRegionType("PARTITION_PROXY_REDUNDANT", regionAttributes))
        .isEqualTo(RegionType.PARTITION_PROXY);
    assertThat(converter.getRegionType("PARTITION_PROXY", regionAttributes))
        .isEqualTo(RegionType.PARTITION_PROXY);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
    assertThat(converter.getRegionType(null, regionAttributes))
        .isEqualTo(RegionType.PARTITION_PERSISTENT);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
    assertThat(converter.getRegionType(null, regionAttributes)).isEqualTo(RegionType.REPLICATE);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
    assertThat(converter.getRegionType(null, regionAttributes))
        .isEqualTo(RegionType.REPLICATE_PERSISTENT);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.EMPTY);
    assertThat(converter.getRegionType(null, regionAttributes))
        .isEqualTo(RegionType.REPLICATE_PROXY);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.NORMAL);
    assertThat(converter.getRegionType(null, regionAttributes)).isEqualTo(RegionType.LEGACY);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PRELOADED);
    assertThat(converter.getRegionType(null, regionAttributes)).isEqualTo(RegionType.LEGACY);
  }

  @Test
  public void fromXmlWithPartitionRedundantType() {
    config.setName("test");
    config.setType("PARTITION_REDUNDANT");
    RegionAttributesType attributesType = new RegionAttributesType();
    attributesType.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
    attributesType.setRedundantCopy("3");
    config.setRegionAttributes(attributesType);
    region = converter.fromXmlObject(config);
    assertThat(region.getType()).isEqualTo(RegionType.PARTITION);
    assertThat(region.getRedundantCopies()).isEqualTo(3);
  }

  @Test
  public void fromXmlWithPartitionRedundantPersistentType() {
    config.setName("test");
    config.setType("PARTITION_REDUNDANT_PERSISTENT");
    RegionAttributesType attributesType = new RegionAttributesType();
    attributesType.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
    attributesType.setRedundantCopy("3");
    config.setRegionAttributes(attributesType);
    region = converter.fromXmlObject(config);
    assertThat(region.getType()).isEqualTo(RegionType.PARTITION_PERSISTENT);
    assertThat(region.getRedundantCopies()).isEqualTo(3);
  }

  @Test
  public void fromXmlWithPartitionProxyRedundantType() {
    config.setName("test");
    config.setType("PARTITION_PROXY_REDUNDANT");
    RegionAttributesType attributesType = new RegionAttributesType();
    config.setRegionAttributes(attributesType);
    attributesType.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
    attributesType.setRedundantCopy("3");
    attributesType.setLocalMaxMemory("0");
    region = converter.fromXmlObject(config);
    assertThat(region.getType()).isEqualTo(RegionType.PARTITION_PROXY);
    assertThat(region.getRedundantCopies()).isEqualTo(3);
  }

  @Test
  public void createRegionAttributesByInvalidType() {
    assertThatThrownBy(() -> converter.createRegionAttributesByType("abc"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void convertRegionExpirationFromXml() {
    config.setType("REPLICATE");
    config.setName("test");
    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setEntryTimeToLive(
        new RegionAttributesType.ExpirationAttributesType(10, "destroy", null, null));
    attributes.setEntryIdleTime(
        new RegionAttributesType.ExpirationAttributesType(100, "local-destroy", null, null));
    attributes.setRegionIdleTime(
        new RegionAttributesType.ExpirationAttributesType(101, "invalidate", null, null));
    attributes.setRegionTimeToLive(
        new RegionAttributesType.ExpirationAttributesType(102, "local-invalidate", null, null));

    config.setRegionAttributes(attributes);

    Region region = converter.fromXmlObject(config);
    List<Region.Expiration> expirations = region.getExpirations();
    assertThat(expirations).hasSize(4);
    assertThat(expirations.get(0).getTimeInSeconds()).isEqualTo(100);
    assertThat(expirations.get(0).getAction()).isEqualTo(Region.ExpirationAction.LEGACY);
    assertThat(expirations.get(0).getType()).isEqualTo(Region.ExpirationType.ENTRY_IDLE_TIME);
    assertThat(expirations.get(1).getTimeInSeconds()).isEqualTo(10);
    assertThat(expirations.get(1).getAction()).isEqualTo(Region.ExpirationAction.DESTROY);
    assertThat(expirations.get(1).getType()).isEqualTo(Region.ExpirationType.ENTRY_TIME_TO_LIVE);
    assertThat(expirations.get(2).getTimeInSeconds()).isEqualTo(101);
    assertThat(expirations.get(2).getAction()).isEqualTo(Region.ExpirationAction.INVALIDATE);
    assertThat(expirations.get(2).getType()).isEqualTo(Region.ExpirationType.LEGACY);
    assertThat(expirations.get(3).getTimeInSeconds()).isEqualTo(102);
    assertThat(expirations.get(3).getAction()).isEqualTo(Region.ExpirationAction.LEGACY);
    assertThat(expirations.get(3).getType()).isEqualTo(Region.ExpirationType.LEGACY);
  }

  @Test
  public void convertRegionExpirationFromConfig() {
    region.setName("test");
    region.setType(RegionType.REPLICATE);
    region.addExpiry(Region.ExpirationType.ENTRY_IDLE_TIME, 100, null);
    region.addExpiry(Region.ExpirationType.ENTRY_TIME_TO_LIVE, 101,
        Region.ExpirationAction.INVALIDATE);

    RegionConfig regionConfig = converter.fromConfigObject(region);
    RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
    assertThat(regionAttributes.getEntryIdleTime().getTimeout()).isEqualTo("100");
    assertThat(regionAttributes.getEntryIdleTime().getAction()).isEqualTo("destroy");
    assertThat(regionAttributes.getEntryIdleTime().getCustomExpiry()).isNull();
    assertThat(regionAttributes.getEntryTimeToLive().getTimeout()).isEqualTo("101");
    assertThat(regionAttributes.getEntryTimeToLive().getAction()).isEqualTo("invalidate");
    assertThat(regionAttributes.getEntryTimeToLive().getCustomExpiry()).isNull();
  }

  @Test
  public void convertExpirationFromConfig() {
    Region.Expiration expiration = new Region.Expiration();
    expiration.setTimeInSeconds(2);
    RegionAttributesType.ExpirationAttributesType expirationAttributes =
        converter.convertFrom(expiration);
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
    assertThat(expirationAttributes.getAction()).isEqualTo("destroy");
    assertThat(expirationAttributes.getTimeout()).isEqualTo("2");

    expiration.setTimeInSeconds(20);
    expiration.setAction(Region.ExpirationAction.INVALIDATE);
    expirationAttributes =
        converter.convertFrom(expiration);
    assertThat(expirationAttributes.getCustomExpiry()).isNull();
    assertThat(expirationAttributes.getAction()).isEqualTo("invalidate");
    assertThat(expirationAttributes.getTimeout()).isEqualTo("20");
  }

  @Test
  public void convertExpirationFromXml() {
    RegionAttributesType.ExpirationAttributesType xmlConfig =
        new RegionAttributesType.ExpirationAttributesType();
    Region.Expiration expiration =
        converter.convertFrom(Region.ExpirationType.ENTRY_IDLE_TIME, xmlConfig);
    assertThat(expiration.getType()).isEqualTo(Region.ExpirationType.ENTRY_IDLE_TIME);
    assertThat(expiration.getAction()).isEqualTo(Region.ExpirationAction.INVALIDATE);
    assertThat(expiration.getTimeInSeconds()).isEqualTo(0);

    xmlConfig.setTimeout("1000");
    xmlConfig.setAction("destroy");
    expiration =
        converter.convertFrom(Region.ExpirationType.ENTRY_IDLE_TIME, xmlConfig);
    assertThat(expiration.getType()).isEqualTo(Region.ExpirationType.ENTRY_IDLE_TIME);
    assertThat(expiration.getAction()).isEqualTo(Region.ExpirationAction.DESTROY);
    assertThat(expiration.getTimeInSeconds()).isEqualTo(1000);

    xmlConfig.setAction("local-destroy");
    expiration =
        converter.convertFrom(Region.ExpirationType.ENTRY_IDLE_TIME, xmlConfig);
    assertThat(expiration.getType()).isEqualTo(Region.ExpirationType.ENTRY_IDLE_TIME);
    assertThat(expiration.getAction()).isEqualTo(Region.ExpirationAction.LEGACY);
    assertThat(expiration.getTimeInSeconds()).isEqualTo(1000);
  }

  @Test
  public void convertRegionEvictionFromConfigDefaultHeap() {
    region.setName("test");
    region.setType(RegionType.REPLICATE);
    Region.Eviction eviction = new Region.Eviction();
    region.setEviction(eviction);

    RegionConfig regionConfig = converter.fromConfigObject(region);
    RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
    assertThat(regionAttributes.getEvictionAttributes()).isNotNull();
    assertThat(regionAttributes.getEvictionAttributes().getLruHeapPercentage()).isNotNull();
    assertThat(regionAttributes.getEvictionAttributes().getLruHeapPercentage().getAction())
        .isEqualTo(
            EnumActionDestroyOverflow.LOCAL_DESTROY);
    assertThat(regionAttributes.getEvictionAttributes().getLruHeapPercentage().getClassName())
        .isNull();
  }

  @Test
  public void convertRegionEvictionFromConfigMemorySize() {
    region.setName("test");
    region.setType(RegionType.REPLICATE);
    Region.Eviction eviction = new Region.Eviction();
    eviction.setMemorySizeMb(10);
    eviction.setAction(Region.EvictionAction.OVERFLOW_TO_DISK);
    Properties properties = new Properties();
    properties.setProperty("key", "value");
    eviction.setObjectSizer(new ClassName("ObjectSizer", properties));
    region.setEviction(eviction);

    RegionConfig regionConfig = converter.fromConfigObject(region);
    RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
    assertThat(regionAttributes.getEvictionAttributes()).isNotNull();
    assertThat(regionAttributes.getEvictionAttributes().getLruMemorySize()).isNotNull();
    assertThat(regionAttributes.getEvictionAttributes().getLruMemorySize().getAction()).isEqualTo(
        EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
    assertThat(regionAttributes.getEvictionAttributes().getLruMemorySize().getMaximum())
        .isEqualTo("10");
    assertThat(regionAttributes.getEvictionAttributes().getLruMemorySize().getClassName())
        .isEqualTo("ObjectSizer");
    assertThat(regionAttributes.getEvictionAttributes().getLruMemorySize().getParameters())
        .containsExactly(new ParameterType("key", "value"));

  }

  @Test
  public void convertRegionEvictionFromConfigEntryCount() {
    region.setName("test");
    region.setType(RegionType.REPLICATE);
    Region.Eviction eviction = new Region.Eviction();
    eviction.setEntryCount(10);
    eviction.setAction(Region.EvictionAction.LOCAL_DESTROY);
    region.setEviction(eviction);

    RegionConfig regionConfig = converter.fromConfigObject(region);
    RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
    assertThat(regionAttributes.getEvictionAttributes()).isNotNull();
    assertThat(regionAttributes.getEvictionAttributes().getLruEntryCount()).isNotNull();
    assertThat(regionAttributes.getEvictionAttributes().getLruEntryCount().getAction()).isEqualTo(
        EnumActionDestroyOverflow.LOCAL_DESTROY);
    assertThat(regionAttributes.getEvictionAttributes().getLruEntryCount().getMaximum())
        .isEqualTo("10");
  }

  @Test
  public void convertRegionEvictionFromXMLMemorySize() {
    config.setType("REPLICATE");
    config.setName("test");
    RegionAttributesType attributes = new RegionAttributesType();

    RegionAttributesType.EvictionAttributes.LruMemorySize evictionXmlConfig =
        new RegionAttributesType.EvictionAttributes.LruMemorySize();
    evictionXmlConfig.setMaximum("100");
    evictionXmlConfig.setAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
    evictionXmlConfig.setClassName("ObjectSizer");
    Properties properties = new Properties();
    properties.setProperty("key", "value");
    evictionXmlConfig.setParameters(properties);

    RegionAttributesType.EvictionAttributes evictionAttributes =
        new RegionAttributesType.EvictionAttributes();
    evictionAttributes.setLruMemorySize(evictionXmlConfig);
    attributes.setEvictionAttributes(evictionAttributes);
    config.setRegionAttributes(attributes);

    Region region = converter.fromXmlObject(config);
    Region.Eviction eviction = region.getEviction();
    assertThat(eviction.getType()).isEqualTo(Region.EvictionType.MEMORY_SIZE);
    assertThat(eviction.getAction()).isEqualTo(Region.EvictionAction.OVERFLOW_TO_DISK);
    assertThat(eviction.getMemorySizeMb()).isEqualTo(100);
    assertThat(eviction.getObjectSizer().getClassName()).isEqualTo("ObjectSizer");
    assertThat(eviction.getObjectSizer().getInitProperties()).containsEntry("key", "value");
    assertThat(eviction.getEntryCount()).isNull();

    evictionXmlConfig.setAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
    eviction = converter.convertFrom(evictionXmlConfig);
    assertThat(eviction.getAction()).isEqualTo(Region.EvictionAction.LOCAL_DESTROY);
  }

  @Test
  public void convertRegionEvicionFromXMLEntrySize() {
    config.setType("REPLICATE");
    config.setName("test");
    RegionAttributesType attributes = new RegionAttributesType();

    RegionAttributesType.EvictionAttributes.LruEntryCount evictionXmlConfig =
        new RegionAttributesType.EvictionAttributes.LruEntryCount();
    evictionXmlConfig.setMaximum("100");
    evictionXmlConfig.setAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);

    RegionAttributesType.EvictionAttributes evictionAttributes =
        new RegionAttributesType.EvictionAttributes();
    evictionAttributes.setLruEntryCount(evictionXmlConfig);
    attributes.setEvictionAttributes(evictionAttributes);
    config.setRegionAttributes(attributes);

    Region region = converter.fromXmlObject(config);
    Region.Eviction eviction = region.getEviction();
    assertThat(eviction.getType()).isEqualTo(Region.EvictionType.ENTRY_COUNT);
    assertThat(eviction.getAction()).isEqualTo(Region.EvictionAction.OVERFLOW_TO_DISK);
    assertThat(eviction.getEntryCount()).isEqualTo(100);
    assertThat(eviction.getMemorySizeMb()).isNull();

    evictionXmlConfig.setAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
    eviction = converter.convertFrom(evictionXmlConfig);
    assertThat(eviction.getAction()).isEqualTo(Region.EvictionAction.LOCAL_DESTROY);
  }

  @Test
  public void convertRegionEvictionFromXMLHeapSize() {
    config.setType("REPLICATE");
    config.setName("test");
    RegionAttributesType attributes = new RegionAttributesType();

    RegionAttributesType.EvictionAttributes.LruHeapPercentage evictionXmlConfig =
        new RegionAttributesType.EvictionAttributes.LruHeapPercentage();
    evictionXmlConfig.setAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);

    RegionAttributesType.EvictionAttributes evictionAttributes =
        new RegionAttributesType.EvictionAttributes();
    evictionAttributes.setLruHeapPercentage(evictionXmlConfig);
    attributes.setEvictionAttributes(evictionAttributes);
    config.setRegionAttributes(attributes);

    Region region = converter.fromXmlObject(config);
    Region.Eviction eviction = region.getEviction();
    assertThat(eviction.getType()).isEqualTo(Region.EvictionType.HEAP_PERCENTAGE);
    assertThat(eviction.getAction()).isEqualTo(Region.EvictionAction.OVERFLOW_TO_DISK);
    assertThat(eviction.getMemorySizeMb()).isNull();
    assertThat(eviction.getEntryCount()).isNull();

    evictionXmlConfig.setAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
    eviction = converter.convertFrom(evictionXmlConfig);
    assertThat(eviction.getAction()).isEqualTo(Region.EvictionAction.LOCAL_DESTROY);
  }

}
