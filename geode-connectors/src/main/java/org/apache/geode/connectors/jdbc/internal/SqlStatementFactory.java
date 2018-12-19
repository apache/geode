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

import java.util.Iterator;
import java.util.stream.Stream;

class SqlStatementFactory {
  private final String quote;

  public SqlStatementFactory(String identifierQuoteString) {
    this.quote = identifierQuoteString;
  }

  String createSelectQueryString(String tableName, EntryColumnData entryColumnData) {
    return addKeyColumnsToQuery(entryColumnData,
        new StringBuilder("SELECT * FROM " + quoteIdentifier(tableName)));
  }

  String createDestroySqlString(String tableName, EntryColumnData entryColumnData) {
    return addKeyColumnsToQuery(entryColumnData,
        new StringBuilder("DELETE FROM " + quoteIdentifier(tableName)));
  }

  private String addKeyColumnsToQuery(EntryColumnData entryColumnData, StringBuilder queryBuilder) {
    queryBuilder.append(" WHERE ");
    Iterator<ColumnData> iterator = entryColumnData.getEntryKeyColumnData().iterator();
    while (iterator.hasNext()) {
      ColumnData keyColumn = iterator.next();
      boolean onLastColumn = !iterator.hasNext();
      queryBuilder.append(quoteIdentifier(keyColumn.getColumnName())).append(" = ?");
      if (!onLastColumn) {
        queryBuilder.append(" AND ");
      }
    }
    return queryBuilder.toString();
  }

  String createUpdateSqlString(String tableName, EntryColumnData entryColumnData) {
    StringBuilder query =
        new StringBuilder("UPDATE ").append(quoteIdentifier(tableName)).append(" SET ");
    int idx = 0;
    for (ColumnData column : entryColumnData.getEntryValueColumnData()) {
      idx++;
      if (idx > 1) {
        query.append(", ");
      }
      query.append(quoteIdentifier(column.getColumnName()));
      query.append(" = ?");
    }
    return addKeyColumnsToQuery(entryColumnData, query);
  }

  String createInsertSqlString(String tableName, EntryColumnData entryColumnData) {
    StringBuilder columnNames =
        new StringBuilder("INSERT INTO ").append(quoteIdentifier(tableName)).append(" (");
    StringBuilder columnValues = new StringBuilder(" VALUES (");
    addColumnDataToSqlString(entryColumnData, columnNames, columnValues);
    columnNames.append(')');
    columnValues.append(')');
    return columnNames.append(columnValues).toString();
  }

  private void addColumnDataToSqlString(EntryColumnData entryColumnData, StringBuilder columnNames,
      StringBuilder columnValues) {
    Stream<ColumnData> values = entryColumnData.getEntryValueColumnData().stream();
    Stream<ColumnData> keys = entryColumnData.getEntryKeyColumnData().stream();
    Stream<ColumnData> columnDataStream = Stream.concat(values, keys);
    final boolean[] firstTime = new boolean[] {true};
    columnDataStream.forEachOrdered(column -> {
      if (!firstTime[0]) {
        columnNames.append(',');
        columnValues.append(',');
      } else {
        firstTime[0] = false;
      }
      columnNames.append(quoteIdentifier(column.getColumnName()));
      columnValues.append('?');
    });
  }

  private String quoteIdentifier(String identifier) {
    return quote + identifier + quote;
  }
}
