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
package org.apache.geode.cache.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.io.File;
import java.net.URL;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RegionConfigTest {

  private JAXBService service;
  private CacheConfig master;
  private RegionConfig regionConfig;
  private URL xmlResource;

  @Before
  public void before() throws Exception {
    service = new JAXBService(CacheConfig.class);
    regionConfig = new RegionConfig();
    xmlResource = RegionConfigTest.class.getResource("RegionConfigTest.xml");
    assertThat(xmlResource).isNotNull();
    master =
        service.unMarshall(FileUtils.readFileToString(new File(xmlResource.getFile()), "UTF-8"));
  }

  @Test
  public void regionNameSwallowsSlash() {
    regionConfig.setName("/regionA");
    assertThat(regionConfig.getName()).isEqualTo("regionA");
  }

  @Test
  public void subRegionsUnsupported() {
    regionConfig = new RegionConfig();
    assertThatThrownBy(() -> regionConfig.setName("/Parent/Child"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> regionConfig.setName("Parent/Child"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void checkDefaultRegionAttributesForShortcuts() {
    RegionShortcut[] shortcuts = RegionShortcut.values();
    for (RegionShortcut shortcut : shortcuts) {
      RegionConfig config = new RegionConfig();
      config.setType(shortcut.name());
      config.setName(shortcut.name());
      RegionConfig masterRegion = CacheElement.findElement(master.getRegions(), shortcut.name());
      assertThat(config).isEqualToComparingFieldByFieldRecursively(masterRegion);
    }
  }


  @Test
  public void correctJsonAndXml() throws Exception {
    String json = "{\"name\":\"test\", \"type\":\"REPLICATE\"}";
    ObjectMapper mapper = GeodeJsonMapper.getMapper();
    regionConfig = mapper.readValue(json, RegionConfig.class);
    assertThat(regionConfig.getName()).isEqualTo("test");
    assertThat(regionConfig.getType()).isEqualTo("REPLICATE");

    String json2 = mapper.writeValueAsString(regionConfig);
    assertThat(json2).contains("\"type\":\"REPLICATE\"");
    assertThat(json2).contains("\"name\":\"test\"");

    CacheConfig cacheConfig = new CacheConfig();
    cacheConfig.getRegions().add(regionConfig);
    String xml = service.marshall(cacheConfig);
    assertThat(xml).contains("<region name=\"test\" refid=\"REPLICATE\"");
  }
}
