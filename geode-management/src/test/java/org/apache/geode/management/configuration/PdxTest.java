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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;

public class PdxTest {

  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Test
  public void getUri() {
    Pdx config = new Pdx();
    assertThat(config.getLinks().getList())
        .isEqualTo("/configurations/pdx");
    assertThat(config.getLinks().getSelf())
        .isEqualTo("/configurations/pdx");
  }

  @Test
  public void autoSerializer() throws Exception {
    Pdx config = new Pdx();
    config.setAutoSerializer(true, ".*");
    String json = mapper.writeValueAsString(config);
    System.out.println(json);
    assertThat(json).contains("org.apache.geode.pdx.ReflectionBasedAutoSerializer")
        .contains("initProperties");

    Pdx pdx = mapper.readValue(json, Pdx.class);
    ClassName pdxSerializer = pdx.getPdxSerializer();
    assertThat(pdxSerializer.getClassName())
        .isEqualTo("org.apache.geode.pdx.ReflectionBasedAutoSerializer");
    assertThat(pdxSerializer.getInitProperties()).containsEntry("check-portability", "true")
        .containsEntry("classes", ".*")
        .hasSize(2);
  }

  @Test
  public void deserialize() throws Exception {
    String json =
        "{\"pdxSerializer\":{\"className\":\"org.apache.geode.pdx.ReflectionBasedAutoSerializer\",\"initProperties\":{\"classes\":\".*\",\"check-portability\":\"true\"}}}";
    String json2 = "{\"autoSerializer\":{\"pattern\":\".*\",\"portable\":\"true\"}}";
    Pdx pdx = mapper.readValue(json2, Pdx.class);
    System.out.println();
  }
}
