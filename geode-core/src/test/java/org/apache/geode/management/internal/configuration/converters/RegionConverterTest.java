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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.management.configuration.Region;

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
    config.setRegionAttributes(converter.createRegionAttributesByType("REPLICATE"));

    RegionAttributesType attributesType = config.getRegionAttributes();
    attributesType.setValueConstraint("foo");
    attributesType.setKeyConstraint("bar");
    attributesType.setDiskStoreName("diskstore");
    config.setRegionAttributes(attributesType);

    Region region = converter.fromXmlObject(config);
    assertThat(region.getName()).isEqualTo("test");
    assertThat(region.getType()).isEqualTo(RegionType.REPLICATE);
    assertThat(region.getValueConstraint()).isEqualTo("foo");
    assertThat(region.getKeyConstraint()).isEqualTo("bar");
    assertThat(region.getDiskStoreName()).isEqualTo("diskstore");
  }

  @Test
  public void fromXmlWithLocalType() throws Exception {
    config.setName("test");
    config.setRegionAttributes(converter.createRegionAttributesByType("LOCAL"));
    assertThat(converter.fromXmlObject(config).getType()).isEqualTo(RegionType.UNSUPPORTED);
  }

  @Test
  public void fromXmlWithNullType() throws Exception {
    config.setName("test");
    config.setType((String) null);
    assertThat(converter.fromXmlObject(config).getType()).isEqualTo(RegionType.UNSUPPORTED);
  }

  @Test
  public void fromConfig() throws Exception {
    region.setName("test");
    region.setType(RegionType.REPLICATE);
    region.setValueConstraint("foo");
    region.setKeyConstraint("bar");
    region.setDiskStoreName("diskstore");
    RegionConfig config = converter.fromConfigObject(region);
    assertThat(config.getName()).isEqualTo("test");
    assertThat(config.getType()).isEqualTo("REPLICATE");
    RegionAttributesType regionAttributes = config.getRegionAttributes();
    assertThat(regionAttributes.getDataPolicy()).isEqualTo(RegionAttributesDataPolicy.REPLICATE);
    assertThat(regionAttributes.getKeyConstraint()).isEqualTo("bar");
    assertThat(regionAttributes.getValueConstraint()).isEqualTo("foo");
    assertThat(regionAttributes.getDiskStoreName()).isEqualTo("diskstore");
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
      RegionConfig masterRegion = CacheElement.findElement(master.getRegions(), shortcut.name());
      assertThat(config).isEqualToComparingFieldByFieldRecursively(masterRegion);
    }
  }

  @Test
  public void getRegionType() throws Exception {
    assertThat(converter.getRegionType(null)).isEqualTo(RegionType.UNSUPPORTED);

    RegionAttributesType regionAttributes = new RegionAttributesType();
    assertThat(converter.getRegionType(regionAttributes)).isEqualTo(RegionType.UNSUPPORTED);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
    assertThat(converter.getRegionType(regionAttributes)).isEqualTo(RegionType.PARTITION);

    RegionAttributesType.PartitionAttributes pAttributes =
        new RegionAttributesType.PartitionAttributes();
    pAttributes.setLocalMaxMemory("20000");
    regionAttributes.setPartitionAttributes(pAttributes);
    assertThat(converter.getRegionType(regionAttributes)).isEqualTo(RegionType.PARTITION);

    pAttributes.setLocalMaxMemory("0");
    assertThat(converter.getRegionType(regionAttributes)).isEqualTo(RegionType.PARTITION_PROXY);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
    assertThat(converter.getRegionType(regionAttributes))
        .isEqualTo(RegionType.PARTITION_PERSISTENT);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
    assertThat(converter.getRegionType(regionAttributes)).isEqualTo(RegionType.REPLICATE);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
    assertThat(converter.getRegionType(regionAttributes))
        .isEqualTo(RegionType.REPLICATE_PERSISTENT);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.EMPTY);
    assertThat(converter.getRegionType(regionAttributes)).isEqualTo(RegionType.REPLICATE_PROXY);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.NORMAL);
    assertThat(converter.getRegionType(regionAttributes)).isEqualTo(RegionType.UNSUPPORTED);

    regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PRELOADED);
    assertThat(converter.getRegionType(regionAttributes)).isEqualTo(RegionType.UNSUPPORTED);
  }
}
