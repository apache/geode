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

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SqlStatementFactoryTest {
  private static final String TABLE_NAME = "testTable";
  private static final String KEY_COLUMN_NAME = "column1";

  private List<ColumnValue> columnValues = new ArrayList<>();
  private SqlStatementFactory factory = new SqlStatementFactory();

  @Before
  public void setup() {
    columnValues.add(new ColumnValue(false, "column0", null));
    columnValues.add(new ColumnValue(true, KEY_COLUMN_NAME, null));
    columnValues.add(new ColumnValue(false, "column2", null));
  }

  @Test
  public void getSelectQueryString() throws Exception {
    List<ColumnValue> keyColumn = new ArrayList<>();
    keyColumn.add(new ColumnValue(true, KEY_COLUMN_NAME, null));
    String statement = factory.createSelectQueryString(TABLE_NAME, keyColumn);
    String expectedStatement =
        String.format("SELECT * FROM %s WHERE %s = ?", TABLE_NAME, KEY_COLUMN_NAME);
    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getDestroySqlString() throws Exception {
    List<ColumnValue> keyColumn = new ArrayList<>();
    keyColumn.add(new ColumnValue(true, KEY_COLUMN_NAME, null));
    String statement = factory.createDestroySqlString(TABLE_NAME, keyColumn);
    String expectedStatement =
        String.format("DELETE FROM %s WHERE %s = ?", TABLE_NAME, KEY_COLUMN_NAME);
    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getUpdateSqlString() throws Exception {
    String statement = factory.createUpdateSqlString(TABLE_NAME, columnValues);
    String expectedStatement = String.format("UPDATE %s SET %s = ?, %s = ? WHERE %s = ?",
        TABLE_NAME, columnValues.get(0).getColumnName(), columnValues.get(2).getColumnName(),
        KEY_COLUMN_NAME);
    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getInsertSqlString() throws Exception {
    String statement = factory.createInsertSqlString(TABLE_NAME, columnValues);
    String expectedStatement = String.format("INSERT INTO %s (%s, %s, %s) VALUES (?,?,?)",
        TABLE_NAME, columnValues.get(0).getColumnName(), columnValues.get(1).getColumnName(),
        columnValues.get(2).getColumnName());
    assertThat(statement).isEqualTo(expectedStatement);
  }

}
