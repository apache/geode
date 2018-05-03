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

package org.apache.geode.management.internal.cli.result;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class TabularResultDataTest {

  private TabularResultData data;

  @Before
  public void before() throws Exception {
    data = new TabularResultData();
  }

  @Test
  public void emptyTabularResultData() {
    assertThat(data.getGfJsonObject().getString("content")).isEqualTo("{}");
    assertThat(data.getType()).isEqualTo(ResultData.TYPE_TABULAR);
  }

  @Test
  public void canAccumulate() {
    data.accumulate("col1", "value1");
    assertThat(data.getGfJsonObject().getJSONObject("content").getString("col1"))
        .isEqualTo("[\"value1\"]");

    data.accumulate("col1", "value2");
    assertThat(data.getGfJsonObject().getJSONObject("content").getString("col1"))
        .isEqualTo("[\"value1\",\"value2\"]");

    data.accumulate("col2", "value3");
    data.accumulate("col2", "value4");
    assertThat(data.getGfJsonObject().getJSONObject("content").getString("col1"))
        .isEqualTo("[\"value1\",\"value2\"]");
    assertThat(data.getGfJsonObject().getJSONObject("content").getString("col2"))
        .isEqualTo("[\"value3\",\"value4\"]");
  }

  @Test
  public void canRetrieveAllValues() {
    data.accumulate("col1", "value1");
    data.accumulate("col1", "value2");
    data.accumulate("col1", "value3");

    assertThat(data.retrieveAllValues("col1")).containsExactly("value1", "value2", "value3");

  }

  @Test
  public void hasHeaderAndFooter() {
    String headerFooter = "header and footer";
    data.setHeader(headerFooter);
    data.setFooter(headerFooter);

    assertThat(data.getGfJsonObject().getString("header")).isEqualTo(headerFooter);
    assertThat(data.getGfJsonObject().getString("footer")).isEqualTo(headerFooter);
  }

  @Test
  public void rowColumnSize() {
    data.accumulate("key", "value1");
    assertThat(data.rowSize("key")).isEqualTo(1);
    assertThat(data.columnSize()).isEqualTo(1);

    data.accumulate("key", "value2");
    assertThat(data.rowSize("key")).isEqualTo(2);
    assertThat(data.columnSize()).isEqualTo(1);

    data.accumulate("key1", "value1");
    assertThat(data.rowSize("key1")).isEqualTo(1);
    assertThat(data.columnSize()).isEqualTo(2);
  }
}
