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
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import org.junit.Before;
import org.junit.Test;


public class IndexTest {
  private Index index;

  @Before
  public void before() {
    index = new Index();
  }

  @Test
  public void getRegionName() {
    assertSoftly(softly -> {
      index.setRegionPath(null);
      softly.assertThat(index.getRegionName()).isNull();

      index.setRegionPath("regionA");
      softly.assertThat(index.getRegionName()).isEqualTo("regionA");

      index.setRegionPath("   regionA   ");
      softly.assertThat(index.getRegionName()).isEqualTo("regionA");

      index.setRegionPath(SEPARATOR + "regionA");
      softly.assertThat(index.getRegionName()).isEqualTo("regionA");

      index.setRegionPath(SEPARATOR + "regionA.method()");
      softly.assertThat(index.getRegionName()).isEqualTo("regionA");

      index.setRegionPath(SEPARATOR + "regionA.method() a");
      softly.assertThat(index.getRegionName()).isEqualTo("regionA");

      index.setRegionPath(SEPARATOR + "regionA.fieled.method() a");
      softly.assertThat(index.getRegionName()).isEqualTo("regionA");

      index.setRegionPath(SEPARATOR + "regionA a");
      softly.assertThat(index.getRegionName()).isEqualTo("regionA");

      index.setRegionPath(SEPARATOR + "regionA a, a.foo");
      softly.assertThat(index.getRegionName()).isEqualTo("regionA");
    });
  }

  @Test
  public void getEndPoint() {
    assertThat(index.getLinks().getList()).isEqualTo("/indexes");

    index.setName("testSelf");
    assertThat(index.getLinks().getSelf()).as("only name defined - self")
        .isNull();
    assertThat(index.getLinks().getList()).as("only name defined - list")
        .isEqualTo("/indexes");

    index.setRegionPath(SEPARATOR + "regionPath");
    assertThat(index.getLinks().getSelf()).as("region and name defined - self")
        .isEqualTo("/regions/regionPath/indexes/testSelf");
    assertThat(index.getLinks().getList()).as("region and name defined - list")
        .isEqualTo("/regions/regionPath/indexes");

  }

  @Test
  public void getEndPoint_self_isNull() {
    assertSoftly(softly -> {
      softly.assertThat(index.getLinks().getSelf()).as("empty index config").isNull();
    });
  }
}
