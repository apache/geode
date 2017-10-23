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

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ColumnValueTest {
  private static final String COLUMN_NAME = "columnName";
  private static final Object VALUE = new Object();

  private ColumnValue value;

  @Before
  public void setup() {
    value = new ColumnValue(true, COLUMN_NAME, VALUE);
  }

  @Test
  public void isKeyReturnsCorrectValue() {
    assertThat(value.isKey()).isTrue();
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
  public void toStringContainsAllVariables() {
    assertThat(value.toString()).contains(Boolean.toString(true)).contains(COLUMN_NAME)
        .contains(VALUE.toString());
  }
}
