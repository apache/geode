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

import org.assertj.core.api.ListAssert;

import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

public class TabularResultModelAssert
    extends AbstractResultModelAssert<TabularResultModelAssert, TabularResultModel> {

  public TabularResultModelAssert(TabularResultModel resultModel) {
    super(resultModel, TabularResultModelAssert.class);
  }

  public TabularResultModelAssert hasRowSize(int expectedSize) {
    assertThat(actual.getRowSize()).isEqualTo(expectedSize);
    return this;
  }


  public TabularResultModelAssert hasColumnSize(int expectedSize) {
    assertThat(actual.getColumnSize()).isEqualTo(expectedSize);
    return this;
  }

  /**
   * return a ListAssert for a column of values
   */
  public ListAssert<String> hasColumn(String header) {
    return assertThat(actual.getValuesInColumn(header));
  }

  /**
   * return a ListAssert for a row of values
   */
  public ListAssert<String> hasRow(int rowIndex) {
    return assertThat(actual.getValuesInRow(rowIndex));
  }
}
