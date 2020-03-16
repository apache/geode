/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.cli.result.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.test.junit.assertions.TabularResultModelAssert;


public class TabularResultModelTest {
  private TabularResultModel table1;

  @Before
  public void setUp() throws Exception {
    table1 = new TabularResultModel();
  }

  @Test
  public void accumulateAndAddRowHasTheSameEffect() {
    table1.accumulate("c1", "v1");
    table1.accumulate("c1", "v4");
    table1.accumulate("c2", "v2");
    table1.accumulate("c2", "v5");
    table1.accumulate("c3", "v3");
    table1.accumulate("c3", "v6");

    assertThat(table1.getColumnSize()).isEqualTo(3);

    TabularResultModel table2 = new TabularResultModel();
    table2.setColumnHeader("c1", "c2", "c3");
    table2.addRow("v1", "v2", "v3");
    table2.addRow("v4", "v5", "v6");

    assertThat(table2.getColumnSize()).isEqualTo(3);

    assertThat(table1.getContent()).usingRecursiveComparison().isEqualTo(table2.getContent());
  }

  @Test
  public void fromJson() {
    ResultModel result = new ResultModel();
    table1 = result.addTable("table1");
    // create table that has 2 columns and 1 rows
    table1.accumulate("c1", "v1");
    table1.accumulate("c2", "v2");
    assertThat(table1.getColumnSize()).isEqualTo(2);
    assertThat(table1.getRowSize()).isEqualTo(1);

    String json = result.toJson();
    assertThat(json).contains(
        "{\"table1\":{\"modelClass\":\"org.apache.geode.management.internal.cli.result.model.TabularResultModel\",\"header\":\"\",\"footer\":\"\",\"content\":{\"c1\":[\"v1\"],\"c2\":[\"v2\"]}}}");
    // these make sure we don't serialize extra info
    assertThat(json).doesNotContain("headers");
    assertThat(json).doesNotContain("values");
    assertThat(json).doesNotContain("columnSize");
    assertThat(json).doesNotContain("rowSize");
    assertThat(json).doesNotContain("valuesInColumn");
    assertThat(json).doesNotContain("valuesInRow");

    // convert the json back to the ResultModel object
    ResultModel result2 = ResultModel.fromJson(json);
    TabularResultModel table2 = result2.getTableSection("table1");
    assertThat(table2.getColumnSize()).isEqualTo(2);
    assertThat(table2.getHeaders()).containsExactly("c1", "c2");
    assertThat(table2.getValuesInRow(0)).containsExactly("v1", "v2");
    assertThat(table2.getRowSize()).isEqualTo(1);
  }

  @Test
  public void cannotAddRowWithDifferentSize() {
    table1.setColumnHeader("c1", "c2", "c3");
    assertThatThrownBy(() -> table1.addRow("v1", "v2")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getTableSizes() {
    table1.setColumnHeader("header1");
    assertThat(table1.getRowSize()).isZero();
    table1.addRow("v1");
    assertThat(table1.getRowSize()).isEqualTo(1);
    table1.addRow("v2");
    assertThat(table1.getRowSize()).isEqualTo(2);

    assertThat(table1.getColumnSize()).isEqualTo(1);
  }

  @Test
  public void getValuesInRow() {
    table1.setColumnHeader("c1", "c2", "c3");
    table1.addRow("v1", "k1", "t1");
    table1.addRow("v2", "k2", "t2");
    table1.addRow("v3", "k3", "t3");
    assertThat(table1.getValuesInRow(0)).containsExactly("v1", "k1", "t1");
    assertThat(table1.getValuesInRow(1)).containsExactly("v2", "k2", "t2");
    assertThat(table1.getValuesInRow(2)).containsExactly("v3", "k3", "t3");

    new TabularResultModelAssert(table1).hasAnyRow().contains("v1", "k1", "t1")
        .hasAnyRow().contains("v3", "k3", "t3");
  }
}
