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

package org.apache.geode.management.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.configuration.Links;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class LinksTest {

  private static final ObjectMapper mapper = GeodeJsonMapper.getMapper();
  private Links links;

  @Before
  public void before() throws Exception {
    links = new Links();
    links = new Links("regionA", "/regions");
    links.addLink("index", "/indexes/index1");
  }

  @Test
  public void parse() throws Exception {
    String json = mapper.writeValueAsString(links);
    System.out.println(json);
    assertThat(json).doesNotContain("others")
        .contains("\"list\":\"#HREF/management/v1/regions\"");
    // this is to make sure nothing get de-serialized.
    Links links2 = mapper.readValue(json, Links.class);
    assertThat(links2.getSelf()).isNull();
    assertThat(links2.getList()).isNull();
    assertThat(links2.getLinks()).isEmpty();
  }

  @Test
  public void getUri() throws Exception {
    assertThat(links.getSelf()).isEqualTo("/regions/regionA");
    assertThat(links.getList()).isEqualTo("/regions");
    assertThat(links.getLinks().get("list")).isEqualTo("#HREF/management/v1/regions");
    assertThat(links.getLinks().get("self"))
        .isEqualTo("#HREF/management/v1/regions/regionA");
  }
}
