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

package org.apache.geode.test.junit.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

public class TabularResultModelAssert
    extends AbstractResultModelAssert<TabularResultModelAssert, TabularResultModel> {

  public TabularResultModelAssert(TabularResultModel resultModel) {
    super(resultModel, TabularResultModelAssert.class);
  }

  /**
   * verifies that the table has the expected number of rows
   */
  public TabularResultModelAssert hasRowSize(int expectedSize) {
    assertThat(actual.getRowSize()).isEqualTo(expectedSize);
    return this;
  }

  /**
   * verifies that the table has the expected number of columns
   */
  public TabularResultModelAssert hasColumnSize(int expectedSize) {
    assertThat(actual.getColumnSize()).isEqualTo(expectedSize);
    return this;
  }

  /**
   * verifies that the table is empty
   */
  public void isEmpty() {
    hasRowSize(0);
  }

  /**
   * return a ListAssert-like handle to assert on the header row
   */
  public TabularResultModelRowAssert<String> hasColumns() {
    return new TabularResultModelRowAssert<>(this, actual.getHeaders());
  }

  /**
   * return a ListAssert-like handle to assert on the values of a named column
   */
  public TabularResultModelColumnAssert<String> hasColumn(String header) {
    List<String> values = actual.getValuesInColumn(header);
    assertThat(values).withFailMessage("Column not found: %s", header).isNotNull();
    return new TabularResultModelColumnAssert<>(this, values);
  }

  /**
   * return a ListAssert-like handle to assert on a specific row
   */
  public TabularResultModelRowAssert<String> hasRow(int rowIndex) {
    return new TabularResultModelRowAssert<>(this, actual.getValuesInRow(rowIndex));
  }

  /**
   * return a ListAssert-like handle to assert on any row
   */
  public TabularResultModelAnyRowAssert<String> hasAnyRow() {
    return new TabularResultModelAnyRowAssert<>(this);
  }
}
