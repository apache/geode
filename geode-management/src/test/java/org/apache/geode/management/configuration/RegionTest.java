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

package org.apache.geode.management.configuration;

import static org.apache.geode.management.configuration.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;

public class RegionTest {

  private Region regionConfig;
  private static final ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Before
  public void before() {
    regionConfig = new Region();
  }

  @Test
  public void regionNameSwallowsSeparator() {
    regionConfig.setName(SEPARATOR + "regionA");
    assertThat(regionConfig.getName()).isEqualTo("regionA");
  }

  @Test
  public void subRegionsUnsupported() {
    regionConfig = new Region();
    assertThatThrownBy(() -> regionConfig.setName(SEPARATOR + "Parent" + SEPARATOR + "Child"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> regionConfig.setName("Parent" + SEPARATOR + "Child"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void correctJson() throws Exception {
    String json = "{\"name\":\"test\", \"type\":\"REPLICATE\"}";
    regionConfig = mapper.readValue(json, Region.class);
    assertThat(regionConfig.getName()).isEqualTo("test");
    assertThat(regionConfig.getType()).isEqualTo(RegionType.REPLICATE);

    String json2 = mapper.writeValueAsString(regionConfig);
    System.out.println(json2);
    assertThat(json2).contains("\"type\":\"REPLICATE\"");
    assertThat(json2).contains("\"name\":\"test\"");

    mapper.readValue(json2, Region.class);
  }

  @Test
  public void correctJsonWithExpiriations() throws Exception {
    String expireJson = "{\n"
        + "  \"name\": \"region1\",\n"
        + "  \"type\": \"PARTITION\",\n"
        + "  \"expirations\": [\n"
        + "    {\n"
        + "      \"type\": \"ENTRY_IDLE_TIME\",\n"
        + "      \"timeInSeconds\": 3600,\n"
        + "      \"action\": \"DESTROY\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";
    regionConfig = mapper.readValue(expireJson, Region.class);
    assertThat(regionConfig.getName()).isEqualTo("region1");
    assertThat(regionConfig.getType()).isEqualTo(RegionType.PARTITION);
    assertThat(regionConfig.getExpirations()).isNotNull();
    assertThat(regionConfig.getExpirations().size()).isEqualTo(1);

    Region.Expiration expiration = regionConfig.getExpirations().get(0);
    assertThat(expiration.getType()).isEqualTo(Region.ExpirationType.ENTRY_IDLE_TIME);
    assertThat(expiration.getTimeInSeconds()).isEqualTo(3600);
    assertThat(expiration.getAction()).isEqualTo(Region.ExpirationAction.DESTROY);

    String json2 = mapper.writeValueAsString(regionConfig);
    System.out.println(json2);
    assertThat(json2).contains("\"type\":\"PARTITION\"");
    assertThat(json2).contains("\"name\":\"region1\"");
    assertThat(json2).contains("\"type\":\"ENTRY_IDLE_TIME\"");
    assertThat(json2).contains("\"timeInSeconds\":3600");
    assertThat(json2).contains("\"action\":\"DESTROY\"");

    mapper.readValue(json2, Region.class);
  }

  @Test
  public void correctJsonWithEviction() throws Exception {
    String evictJson = "{\n"
        + "  \"name\": \"region1\",\n"
        + "  \"type\": \"PARTITION\",\n"
        + "  \"eviction\": {\n"
        + "    \"entryCount\": 100,\n"
        + "    \"action\": \"OVERFLOW_TO_DISK\"\n"
        + "  }\n"
        + "}\n";
    regionConfig = mapper.readValue(evictJson, Region.class);
    assertThat(regionConfig.getName()).isEqualTo("region1");
    assertThat(regionConfig.getType()).isEqualTo(RegionType.PARTITION);
    assertThat(regionConfig.getEviction()).isNotNull();
    assertThat(regionConfig.getEviction().getType()).isEqualTo(Region.EvictionType.ENTRY_COUNT);
    assertThat(regionConfig.getEviction().getAction())
        .isEqualTo(Region.EvictionAction.OVERFLOW_TO_DISK);
    assertThat(regionConfig.getEviction().getEntryCount()).isEqualTo(100);

    String json2 = mapper.writeValueAsString(regionConfig);
    System.out.println(json2);
    assertThat(json2).contains("\"type\":\"PARTITION\"");
    assertThat(json2).contains("\"name\":\"region1\"");
    assertThat(json2).contains("\"type\":\"ENTRY_COUNT\"");
    assertThat(json2).contains("\"entryCount\":100");
    assertThat(json2).doesNotContain("limit");
    assertThat(json2).contains("\"action\":\"OVERFLOW_TO_DISK\"");
    mapper.readValue(json2, Region.class);
  }

  @Test
  public void readEviction() throws Exception {
    String json = "{\"action\":\"OVERFLOW_TO_DISK\",\"entryCount\":100}";
    Region.Eviction eviction = mapper.readValue(json, Region.Eviction.class);
    assertThat(eviction.getEntryCount()).isEqualTo(100);
    assertThat(eviction.getAction()).isEqualTo(Region.EvictionAction.OVERFLOW_TO_DISK);

    String json2 = "{\"action\":\"OVERFLOW_TO_DISK\",\"entryCount\":100,\"memorySizeMb\":200}";
    assertThatThrownBy(() -> mapper.readValue(json2, Region.Eviction.class))
        .hasMessageContaining("Type conflict");

    String json3 = "{\"entryCount\":100,\"type\":\"HEAP_PERCENTAGE\"}";
    assertThatThrownBy(() -> mapper.readValue(json3, Region.Eviction.class))
        .hasMessageContaining("Type conflict");

    String json4 = "{\"entryCount\":100,\"entryCount\":\"200\"}";
    eviction = mapper.readValue(json4, Region.Eviction.class);
    assertThat(eviction.getEntryCount()).isEqualTo(200);
  }

  @Test
  public void heapIgnoreLimit() throws Exception {
    Region.Eviction eviction = new Region.Eviction();
    eviction.setType(Region.EvictionType.HEAP_PERCENTAGE);
    assertThat(eviction.getType()).isEqualTo(Region.EvictionType.HEAP_PERCENTAGE);
    assertThat(eviction.getEntryCount()).isNull();
    assertThat(eviction.getMemorySizeMb()).isNull();
  }

  @Test
  public void getUri() {
    regionConfig.setName("regionA");
    assertThat(regionConfig.getLinks().getList()).isEqualTo("/regions");

    assertThat(regionConfig.getLinks().getSelf())
        .isEqualTo("/regions/regionA");
  }

  @Test
  public void equality() {
    Region region1 = new Region();
    Region region2 = new Region();

    assertThat(region1).as("initial state").isEqualTo(region2);

    region1.setName("region");
    region2.setName("different-region");
    assertThat(region1).as("with different names").isNotEqualTo(region2);

    region1.setName("region");
    region2.setName("region");
    assertThat(region1).as("with same name and no group").isEqualTo(region2);

    region1.setName("region");
    region2.setName("region");
    region1.setGroup("group");
    region2.setGroup("group");
    assertThat(region1).as("with same name and same group").isEqualTo(region2);


    region1.setName("region");
    region2.setName("region");
    region1.setGroup("group");
    region2.setGroup("different-group");
    assertThat(region1).as("with same name and different group").isNotEqualTo(region2);
  }
}
