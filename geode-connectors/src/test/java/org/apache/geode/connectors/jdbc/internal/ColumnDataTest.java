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
package org.apache.geode.connectors.jdbc.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.JDBCType;

import org.junit.Before;
import org.junit.Test;


public class ColumnDataTest {

  private static final String COLUMN_NAME = "columnName";
  private static final Object VALUE = new Object();
  private static final JDBCType DATA_TYPE = JDBCType.VARCHAR;

  private ColumnData value;

  @Before
  public void setup() {
    value = new ColumnData(COLUMN_NAME, VALUE, DATA_TYPE);
  }

  @Test
  public void hasCorrectColumnName() {
    assertThat(value.getColumnName()).isEqualTo(COLUMN_NAME);
  }

  @Test
  public void hasCorrectValue() {
    assertThat(value.getValue()).isSameAs(VALUE);
  }

  @Test
  public void hasCorrectDataType() {
    assertThat(value.getDataType()).isSameAs(DATA_TYPE);
  }

  @Test
  public void toStringContainsAllVariables() {
    assertThat(value.toString()).contains(COLUMN_NAME).contains(VALUE.toString())
        .contains(DATA_TYPE.toString());
  }
}
