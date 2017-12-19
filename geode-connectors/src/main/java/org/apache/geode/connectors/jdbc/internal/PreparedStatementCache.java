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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Operation;

class PreparedStatementCache {

  private SqlStatementFactory statementFactory = new SqlStatementFactory();
  // TODO: if connection lost, we will still keep the statement. Make LRU?
  private Map<StatementKey, PreparedStatement> statements = new HashMap<>();

  PreparedStatement getPreparedStatement(Connection connection, List<ColumnValue> columnList,
      String tableName, Operation operation, int pdxTypeId) {
    StatementKey key = new StatementKey(pdxTypeId, operation, tableName);
    return statements.computeIfAbsent(key, k -> {
      String sqlStr = getSqlString(tableName, columnList, operation);
      PreparedStatement statement = null;
      try {
        statement = connection.prepareStatement(sqlStr);
      } catch (SQLException e) {
        handleSQLException(e);
      }
      return statement;
    });
  }

  private String getSqlString(String tableName, List<ColumnValue> columnList, Operation operation) {
    if (operation.isCreate()) {
      return statementFactory.createInsertSqlString(tableName, columnList);
    } else if (operation.isUpdate()) {
      return statementFactory.createUpdateSqlString(tableName, columnList);
    } else if (operation.isDestroy()) {
      return statementFactory.createDestroySqlString(tableName, columnList);
    } else if (operation.isGet()) {
      return statementFactory.createSelectQueryString(tableName, columnList);
    } else {
      throw new IllegalArgumentException("unsupported operation " + operation);
    }
  }

  private void handleSQLException(SQLException e) {
    throw new IllegalStateException("NYI: handleSQLException", e);
  }

  private static class StatementKey {
    private final int pdxTypeId;
    private final Operation operation;
    private final String tableName;

    StatementKey(int pdxTypeId, Operation operation, String tableName) {
      this.pdxTypeId = pdxTypeId;
      this.operation = operation;
      this.tableName = tableName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StatementKey that = (StatementKey) o;

      if (pdxTypeId != that.pdxTypeId) {
        return false;
      }
      if (operation != null ? !operation.equals(that.operation) : that.operation != null) {
        return false;
      }
      return tableName != null ? tableName.equals(that.tableName) : that.tableName == null;
    }

    @Override
    public int hashCode() {
      int result = pdxTypeId;
      result = 31 * result + (operation != null ? operation.hashCode() : 0);
      result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
      return result;
    }
  }
}
