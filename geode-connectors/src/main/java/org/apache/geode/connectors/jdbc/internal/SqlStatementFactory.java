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

class SqlStatementFactory {
  private final String quote;

  public SqlStatementFactory(String identifierQuoteString) {
    this.quote = identifierQuoteString;
  }

  String createSelectQueryString(String tableName, EntryColumnData entryColumnData) {
    ColumnData keyCV = entryColumnData.getEntryKeyColumnData();
    return "SELECT * FROM " + quoteIdentifier(tableName) + " WHERE "
        + quoteIdentifier(keyCV.getColumnName()) + " = ?";
  }

  String createDestroySqlString(String tableName, EntryColumnData entryColumnData) {
    ColumnData keyCV = entryColumnData.getEntryKeyColumnData();
    return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE "
        + quoteIdentifier(keyCV.getColumnName()) + " = ?";
  }

  String createUpdateSqlString(String tableName, EntryColumnData entryColumnData) {
    StringBuilder query = new StringBuilder("UPDATE " + quoteIdentifier(tableName) + " SET ");
    int idx = 0;
    for (ColumnData column : entryColumnData.getEntryValueColumnData()) {
      idx++;
      if (idx > 1) {
        query.append(", ");
      }
      query.append(quoteIdentifier(column.getColumnName()));
      query.append(" = ?");
    }

    ColumnData keyColumnData = entryColumnData.getEntryKeyColumnData();
    query.append(" WHERE ");
    query.append(quoteIdentifier(keyColumnData.getColumnName()));
    query.append(" = ?");

    return query.toString();
  }

  String createInsertSqlString(String tableName, EntryColumnData entryColumnData) {
    StringBuilder columnNames =
        new StringBuilder("INSERT INTO " + quoteIdentifier(tableName) + " (");
    StringBuilder columnValues = new StringBuilder(" VALUES (");

    for (ColumnData column : entryColumnData.getEntryValueColumnData()) {
      columnNames.append(quoteIdentifier(column.getColumnName())).append(", ");
      columnValues.append("?,");
    }

    ColumnData keyColumnData = entryColumnData.getEntryKeyColumnData();
    columnNames.append(quoteIdentifier(keyColumnData.getColumnName())).append(")");
    columnValues.append("?)");
    return columnNames.append(columnValues).toString();
  }

  private String quoteIdentifier(String identifier) {
    return quote + identifier + quote;
  }
}
