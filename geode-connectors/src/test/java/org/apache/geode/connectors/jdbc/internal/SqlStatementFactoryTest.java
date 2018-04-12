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
  private static final String KEY_COLUMN_NAME = "keyColumn";
  private static final String VALUE_COLUMN_1_NAME = "valueColumn1";
  private static final String VALUE_COLUMN_2_NAME = "valueColumn2";

  private EntryColumnData entryColumnData;
  private SqlStatementFactory factory = new SqlStatementFactory("");

  @Before
  public void setup() {
    List<ColumnData> columnData = new ArrayList<>();
    columnData.add(new ColumnData(VALUE_COLUMN_1_NAME, null, 0));
    columnData.add(new ColumnData(VALUE_COLUMN_2_NAME, null, 0));
    ColumnData keyColumnData = new ColumnData(KEY_COLUMN_NAME, null, 0);
    entryColumnData = new EntryColumnData(keyColumnData, columnData);
  }

  @Test
  public void getSelectQueryString() throws Exception {
    String expectedStatement =
        String.format("SELECT * FROM %s WHERE %s = ?", TABLE_NAME, KEY_COLUMN_NAME);

    String statement = factory.createSelectQueryString(TABLE_NAME, entryColumnData);

    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getDestroySqlString() throws Exception {
    String expectedStatement =
        String.format("DELETE FROM %s WHERE %s = ?", TABLE_NAME, KEY_COLUMN_NAME);

    String statement = factory.createDestroySqlString(TABLE_NAME, entryColumnData);

    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getUpdateSqlString() throws Exception {
    String expectedStatement = String.format("UPDATE %s SET %s = ?, %s = ? WHERE %s = ?",
        TABLE_NAME, VALUE_COLUMN_1_NAME, VALUE_COLUMN_2_NAME, KEY_COLUMN_NAME);

    String statement = factory.createUpdateSqlString(TABLE_NAME, entryColumnData);

    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getInsertSqlString() throws Exception {
    String expectedStatement = String.format("INSERT INTO %s (%s, %s, %s) VALUES (?,?,?)",
        TABLE_NAME, VALUE_COLUMN_1_NAME, VALUE_COLUMN_2_NAME, KEY_COLUMN_NAME);

    String statement = factory.createInsertSqlString(TABLE_NAME, entryColumnData);

    assertThat(statement).isEqualTo(expectedStatement);
  }

}
