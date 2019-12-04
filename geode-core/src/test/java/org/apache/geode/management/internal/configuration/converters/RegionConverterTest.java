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

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.config.JAXBService;
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
  public void fromXmlWithNameType() throws Exception {
    config.setName("test");
    config.setRegionAttributes(converter.createRegionAttributesByType("REPLICATE"));

    Region region = converter.fromXmlObject(config);
    assertThat(region.getName()).isEqualTo("test");
    assertThat(region.getType()).isEqualTo(RegionType.REPLICATE);
  }

  @Test
  public void fromXmlWithAll() throws Exception {
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
  public void fromXmlWithLocalType() throws Exception {
    config.setName("test");
    config.setRegionAttributes(converter.createRegionAttributesByType("LOCAL"));
    assertThat(converter.fromXmlObject(config).getType()).isEqualTo(RegionType.LEGACY);
  }

  @Test
  public void fromXmlWithNullType() throws Exception {
    config.setName("test");
    config.setType((String) null);
    assertThat(converter.fromXmlObject(config).getType()).isEqualTo(RegionType.LEGACY);
  }

  @Test
  public void fromConfig() throws Exception {
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
  public void getRegionType() throws Exception {
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
  public void fromXmlWithPartitionRedundantType() throws Exception {
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
  public void fromXmlWithPartitionRedundantPersistentType() throws Exception {
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
  public void fromXmlWithPartitionProxyRedundantType() throws Exception {
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
  public void createRegionAttributesByInvalidType() throws Exception {
    assertThatThrownBy(() -> converter.createRegionAttributesByType("abc"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void convertRegionExpirationFromXml() throws Exception {
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
  public void convertRegionExpirationFromConfig() throws Exception {
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
  public void convertExpirationFromConfig() throws Exception {
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
  public void convertExpirationFromXml() throws Exception {
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
}
