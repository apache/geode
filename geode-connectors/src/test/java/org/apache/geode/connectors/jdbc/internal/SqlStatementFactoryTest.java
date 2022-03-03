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
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;


public class SqlStatementFactoryTest {

  private static final String QUOTE = "@@";
  private static final String QUOTED_TABLE_PATH = QUOTE + "testTable" + QUOTE;
  private static final String KEY_COLUMN_1_NAME = "keyColumn1";
  private static final String KEY_COLUMN_2_NAME = "keyColumn2";
  private static final String VALUE_COLUMN_1_NAME = "valueColumn1";
  private static final String VALUE_COLUMN_2_NAME = "valueColumn2";
  private final List<ColumnData> keyColumnData = new ArrayList<>();
  private final List<ColumnData> valueColumnData = new ArrayList<>();

  private EntryColumnData entryColumnData;
  private final SqlStatementFactory factory = new SqlStatementFactory(QUOTE);

  private String quoted(String id) {
    return QUOTE + id + QUOTE;
  }

  @Before
  public void setup() {
    valueColumnData.add(new ColumnData(VALUE_COLUMN_1_NAME, null, JDBCType.NULL));
    valueColumnData.add(new ColumnData(VALUE_COLUMN_2_NAME, null, JDBCType.NULL));
    keyColumnData.add(new ColumnData(KEY_COLUMN_1_NAME, null, JDBCType.NULL));
    entryColumnData = new EntryColumnData(keyColumnData, valueColumnData);
  }

  @Test
  public void getSelectQueryString() throws Exception {
    String expectedStatement =
        String.format("SELECT * FROM %s WHERE %s = ?", QUOTED_TABLE_PATH,
            quoted(KEY_COLUMN_1_NAME));

    String statement = factory.createSelectQueryString(QUOTED_TABLE_PATH, entryColumnData);

    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getDestroySqlString() throws Exception {
    String expectedStatement =
        String.format("DELETE FROM %s WHERE %s = ?", QUOTED_TABLE_PATH, quoted(KEY_COLUMN_1_NAME));

    String statement = factory.createDestroySqlString(QUOTED_TABLE_PATH, entryColumnData);

    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getUpdateSqlString() throws Exception {
    String expectedStatement = String.format("UPDATE %s SET %s = ?, %s = ? WHERE %s = ?",
        QUOTED_TABLE_PATH, quoted(VALUE_COLUMN_1_NAME), quoted(VALUE_COLUMN_2_NAME),
        quoted(KEY_COLUMN_1_NAME));

    String statement = factory.createUpdateSqlString(QUOTED_TABLE_PATH, entryColumnData);

    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getInsertSqlString() throws Exception {
    String expectedStatement = String.format("INSERT INTO %s (%s,%s,%s) VALUES (?,?,?)",
        QUOTED_TABLE_PATH, quoted(VALUE_COLUMN_1_NAME), quoted(VALUE_COLUMN_2_NAME),
        quoted(KEY_COLUMN_1_NAME));

    String statement = factory.createInsertSqlString(QUOTED_TABLE_PATH, entryColumnData);

    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getInsertSqlStringGivenNoColumns() throws Exception {
    valueColumnData.clear();
    keyColumnData.clear();

    String statement = factory.createInsertSqlString(QUOTED_TABLE_PATH, entryColumnData);

    String expectedStatement = String.format("INSERT INTO %s () VALUES ()", QUOTED_TABLE_PATH);
    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getInsertSqlStringGivenMultipleKeys() throws Exception {
    keyColumnData.add(new ColumnData(KEY_COLUMN_2_NAME, null, JDBCType.NULL));

    String statement = factory.createInsertSqlString(QUOTED_TABLE_PATH, entryColumnData);

    String expectedStatement = String.format("INSERT INTO %s (%s,%s,%s,%s) VALUES (?,?,?,?)",
        QUOTED_TABLE_PATH, quoted(VALUE_COLUMN_1_NAME), quoted(VALUE_COLUMN_2_NAME),
        quoted(KEY_COLUMN_1_NAME), quoted(KEY_COLUMN_2_NAME));
    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getUpdateSqlStringGivenMultipleKeys() throws Exception {
    keyColumnData.add(new ColumnData(KEY_COLUMN_2_NAME, null, JDBCType.NULL));

    String statement = factory.createUpdateSqlString(QUOTED_TABLE_PATH, entryColumnData);

    String expectedStatement = String.format("UPDATE %s SET %s = ?, %s = ? WHERE %s = ? AND %s = ?",
        QUOTED_TABLE_PATH, quoted(VALUE_COLUMN_1_NAME), quoted(VALUE_COLUMN_2_NAME),
        quoted(KEY_COLUMN_1_NAME), quoted(KEY_COLUMN_2_NAME));
    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getSelectQueryStringGivenMultipleKeys() throws Exception {
    keyColumnData.add(new ColumnData(KEY_COLUMN_2_NAME, null, JDBCType.NULL));

    String statement = factory.createSelectQueryString(QUOTED_TABLE_PATH, entryColumnData);

    String expectedStatement =
        String.format("SELECT * FROM %s WHERE %s = ? AND %s = ?", QUOTED_TABLE_PATH,
            quoted(KEY_COLUMN_1_NAME),
            quoted(KEY_COLUMN_2_NAME));
    assertThat(statement).isEqualTo(expectedStatement);
  }

  @Test
  public void getDestroySqlStringGivenMultipleKeys() throws Exception {
    keyColumnData.add(new ColumnData(KEY_COLUMN_2_NAME, null, JDBCType.NULL));

    String statement = factory.createDestroySqlString(QUOTED_TABLE_PATH, entryColumnData);

    String expectedStatement =
        String.format("DELETE FROM %s WHERE %s = ? AND %s = ?", QUOTED_TABLE_PATH,
            quoted(KEY_COLUMN_1_NAME),
            quoted(KEY_COLUMN_2_NAME));
    assertThat(statement).isEqualTo(expectedStatement);
  }

}
