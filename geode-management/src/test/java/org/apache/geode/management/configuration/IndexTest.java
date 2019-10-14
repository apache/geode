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

import org.junit.Before;
import org.junit.Test;


public class IndexTest {
  private Index index;

  @Before
  public void before() throws Exception {
    index = new Index();
  }

  @Test
  public void getRegionName() throws Exception {
    index.setRegionPath(null);
    assertThat(index.getRegionName()).isNull();

    index.setRegionPath("regionA");
    assertThat(index.getRegionName()).isEqualTo("regionA");

    index.setRegionPath("   regionA   ");
    assertThat(index.getRegionName()).isEqualTo("regionA");

    index.setRegionPath("/regionA");
    assertThat(index.getRegionName()).isEqualTo("regionA");

    index.setRegionPath("/regionA.method()");
    assertThat(index.getRegionName()).isEqualTo("regionA");

    index.setRegionPath("/regionA.method() a");
    assertThat(index.getRegionName()).isEqualTo("regionA");

    index.setRegionPath("/regionA.fieled.method() a");
    assertThat(index.getRegionName()).isEqualTo("regionA");

    index.setRegionPath("/regionA a");
    assertThat(index.getRegionName()).isEqualTo("regionA");

    index.setRegionPath("/regionA a, a.foo");
    assertThat(index.getRegionName()).isEqualTo("regionA");
  }

  @Test
  public void getEndPoint() throws Exception {
    assertThat(index.getLinks().getList()).isEqualTo("/indexes");

    index.setRegionPath("/regionA");
    assertThat(index.getLinks().getList()).isEqualTo("/regions/regionA/indexes");
  }
}
