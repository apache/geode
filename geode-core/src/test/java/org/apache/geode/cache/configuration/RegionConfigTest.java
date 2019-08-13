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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RegionConfigTest {

  private JAXBService service;

  @Before
  public void before() throws Exception {
    service = new JAXBService(CacheConfig.class);
  }


  @Test
  public void indexType() throws Exception {
    RegionConfig.Index index = new RegionConfig.Index();
    assertThat(index.isKeyIndex()).isNull();
    assertThat(index.getType()).isEqualTo("range");

    index.setKeyIndex(true);
    assertThat(index.isKeyIndex()).isTrue();
    assertThat(index.getType()).isEqualTo("key");

    index.setKeyIndex(false);
    assertThat(index.isKeyIndex()).isFalse();
    assertThat(index.getType()).isEqualTo("range");

    index.setType("hash");
    assertThat(index.isKeyIndex()).isFalse();
    assertThat(index.getType()).isEqualTo("hash");

    index.setType("key");
    assertThat(index.isKeyIndex()).isTrue();
    assertThat(index.getType()).isEqualTo("key");
  }

  @Test
  public void index() throws Exception {
    String xml = "<region name=\"region1\" refid=\"REPLICATE\">\n"
        + "<region-attributes data-policy=\"replicate\" scope=\"distributed-ack\" concurrency-checks-enabled=\"true\"/>\n"
        + "<index name=\"index1\" expression=\"id\" from-clause=\"/region1\" key-index=\"true\"/>\n"
        + "</region>";

    RegionConfig regionConfig = service.unMarshall(xml, RegionConfig.class);

    RegionConfig.Index index = regionConfig.getIndexes().get(0);
    assertThat(index.isKeyIndex()).isTrue();
    assertThat(index.getType()).isEqualTo("key");

    String json = GeodeJsonMapper.getMapper().writeValueAsString(index);
    System.out.println(json);
    RegionConfig.Index newIndex =
        GeodeJsonMapper.getMapper().readValue(json, RegionConfig.Index.class);
    assertThat(newIndex.isKeyIndex()).isTrue();
    assertThat(newIndex.getType()).isEqualTo("key");

    CacheConfig cacheConfig = new CacheConfig();
    regionConfig.getIndexes().clear();
    regionConfig.getIndexes().add(newIndex);
    cacheConfig.getRegions().add(regionConfig);

    // the end xml should not have "type" attribute in index definition
    String newXml = service.marshall(cacheConfig);
    System.out.println(newXml);
    assertThat(newXml).doesNotContain("type=");
  }

  @Test
  public void diskDirTypeInXml() throws Exception {
    CacheConfig cacheConfig = new CacheConfig();
    DiskStoreType diskStore = new DiskStoreType();
    diskStore.setName("diskStore");
    DiskDirType dir1 = new DiskDirType();
    DiskDirType dir2 = new DiskDirType();
    dir1.setContent("./data/persist");
    dir2.setContent("/data/persist");

    diskStore.getDiskDirs().add(dir1);
    diskStore.getDiskDirs().add(dir2);

    cacheConfig.getDiskStores().add(diskStore);

    String xml = service.marshall(cacheConfig);
    System.out.println(xml);

    String diskStoreXml = "<disk-store name=\"diskStore\">\n"
        + "        <disk-dirs>\n"
        + "            <disk-dir>./data/persist</disk-dir>\n"
        + "            <disk-dir>/data/persist</disk-dir>\n"
        + "        </disk-dirs>\n"
        + "    </disk-store>";
    assertThat(xml).contains(diskStoreXml);

    DiskStoreType parsedDiskStore = service.unMarshall(diskStoreXml, DiskStoreType.class);
    assertThat(parsedDiskStore.getDiskDirs()).hasSize(2);

    cacheConfig.getDiskStores().clear();
    cacheConfig.getDiskStores().add(parsedDiskStore);

    String xml2 = service.marshall(cacheConfig);
    System.out.println(xml2);
    assertThat(xml.replace('\\', '/')).contains(diskStoreXml);
  }

  @Test
  public void diskDirTypeInJson() throws Exception {
    DiskStoreType diskStore = new DiskStoreType();
    diskStore.setName("diskStore");
    DiskDirType dir1 = new DiskDirType();
    DiskDirType dir2 = new DiskDirType();
    dir1.setContent("./data/persist");
    dir2.setContent("/data/persist");

    diskStore.getDiskDirs().add(dir1);
    diskStore.getDiskDirs().add(dir2);

    ObjectMapper mapper = GeodeJsonMapper.getMapper();
    String json = mapper.writeValueAsString(diskStore);
    System.out.println(json);

    String goldenJson =
        "\"diskDirs\":[{\"content\":\"./data/persist\"},{\"content\":\"/data/persist\"}]";
    assertThat(json.replace('\\', '/')).contains(goldenJson);

    DiskStoreType newDiskStore = mapper.readValue(json, DiskStoreType.class);
    assertThat(newDiskStore.getDiskDirs()).hasSize(2);

  }
}
