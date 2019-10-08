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

package org.apache.geode.management.internal;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.geode.management.configuration.Region;

public class LinksTest {

  @Test
  public void singleItemOk() {
    Region region = new Region();
    region.setName("testRegion");
    String expectedLink = "/management/experimental/regions/testRegion";

    Map<String, String> links = Links.singleItem(region);
    assertThat(links).hasSize(1);
    assertThat(links.get("self")).isEqualTo(expectedLink);
  }

  @Test
  public void singleItemNull() {
    Region region = new Region();

    Map<String, String> links = Links.singleItem(region);
    assertThat(links).isEmpty();
  }

  @Test
  public void addApiRootOk() {
    Map<String, String> links = new HashMap<>();
    String expectedLink = "/management/experimental/";

    Links.addApiRoot(links);
    assertThat(links).hasSize(1);
    assertThat(links.get("api root")).isEqualTo(expectedLink);
  }

  @Test(expected = NullPointerException.class)
  public void addApiRootToNull() {
    Links.addApiRoot(null);
  }

  @Test
  public void rootLinksOk() {
    Map<String, String> links = Links.rootLinks();
    String swaggerLink = "/management/swagger-ui.html";
    String docsLink = "https://geode.apache.org/docs";
    String wikiLink = "https://cwiki.apache.org/confluence/display/GEODE/Management+REST+API";

    assertThat(links).hasSize(3);
    assertThat(links.get(Links.SWAGGER)).isEqualTo(swaggerLink);
    assertThat(links.get(Links.DOCS)).isEqualTo(docsLink);
    assertThat(links.get(Links.WIKI)).isEqualTo(wikiLink);
  }
}
