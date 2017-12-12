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

import java.util.List;

class SqlStatementFactory {

  String createSelectQueryString(String tableName, List<ColumnValue> columnList) {
    assert columnList.size() == 1;
    ColumnValue keyCV = columnList.get(0);
    assert keyCV.isKey();
    return "SELECT * FROM " + tableName + " WHERE " + keyCV.getColumnName() + " = ?";
  }

  String createDestroySqlString(String tableName, List<ColumnValue> columnList) {
    assert columnList.size() == 1;
    ColumnValue keyCV = columnList.get(0);
    assert keyCV.isKey();
    return "DELETE FROM " + tableName + " WHERE " + keyCV.getColumnName() + " = ?";
  }

  String createUpdateSqlString(String tableName, List<ColumnValue> columnList) {
    StringBuilder query = new StringBuilder("UPDATE " + tableName + " SET ");
    int idx = 0;
    for (ColumnValue column : columnList) {
      if (!column.isKey()) {
        idx++;
        if (idx > 1) {
          query.append(", ");
        }
        query.append(column.getColumnName());
        query.append(" = ?");
      }
    }
    for (ColumnValue column : columnList) {
      if (column.isKey()) {
        query.append(" WHERE ");
        query.append(column.getColumnName());
        query.append(" = ?");
        // currently only support simple primary key with one column
        break;
      }
    }
    return query.toString();
  }

  String createInsertSqlString(String tableName, List<ColumnValue> columnList) {
    StringBuilder columnNames = new StringBuilder("INSERT INTO " + tableName + " (");
    StringBuilder columnValues = new StringBuilder(" VALUES (");
    int columnCount = columnList.size();
    int idx = 0;
    for (ColumnValue column : columnList) {
      idx++;
      columnNames.append(column.getColumnName());
      columnValues.append('?');
      if (idx != columnCount) {
        columnNames.append(", ");
        columnValues.append(",");
      }
    }
    columnNames.append(")");
    columnValues.append(")");
    return columnNames.append(columnValues).toString();
  }
}
