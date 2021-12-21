/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.connectors.jdbc.internal;

import java.sql.JDBCType;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableMetaData implements TableMetaDataView {

  private final String quotedTablePath;
  private final List<String> keyColumnNames;
  private final Map<String, ColumnMetaData> columnMetaDataMap;
  private final String identifierQuoteString;

  public TableMetaData(String catalogName, String schemaName, String tableName,
      List<String> keyColumnNames, String quoteString,
      Map<String, ColumnMetaData> columnMetaDataMap) {
    if (quoteString == null) {
      quoteString = "";
    }
    quotedTablePath = createQuotedTablePath(catalogName, schemaName, tableName, quoteString);
    this.keyColumnNames = keyColumnNames;
    this.columnMetaDataMap = columnMetaDataMap;
    identifierQuoteString = quoteString;
  }

  private static String createQuotedTablePath(String catalogName, String schemaName,
      String tableName, String quote) {
    StringBuilder builder = new StringBuilder();
    appendPrefix(builder, catalogName, quote);
    appendPrefix(builder, schemaName, quote);
    builder.append(quote).append(tableName).append(quote);
    return builder.toString();
  }

  private static void appendPrefix(StringBuilder builder, String prefix, String quote) {
    if (prefix != null && !prefix.isEmpty()) {
      builder.append(quote).append(prefix).append(quote).append('.');
    }
  }

  @Override
  public String getQuotedTablePath() {
    return quotedTablePath;
  }

  @Override
  public List<String> getKeyColumnNames() {
    return keyColumnNames;
  }

  @Override
  public JDBCType getColumnDataType(String columnName) {
    ColumnMetaData columnMetaData = columnMetaDataMap.get(columnName);
    if (columnMetaData == null) {
      return JDBCType.NULL;
    }
    return columnMetaData.getType();
  }

  @Override
  public boolean isColumnNullable(String columnName) {
    ColumnMetaData columnMetaData = columnMetaDataMap.get(columnName);
    if (columnMetaData == null) {
      return true;
    }
    return columnMetaData.isNullable();
  }

  @Override
  public Set<String> getColumnNames() {
    return columnMetaDataMap.keySet();
  }

  @Override
  public String getIdentifierQuoteString() {
    return identifierQuoteString;
  }

  public static class ColumnMetaData {
    private final JDBCType type;
    private final boolean nullable;

    public ColumnMetaData(JDBCType type, boolean nullable) {
      this.type = type;
      this.nullable = nullable;
    }

    public JDBCType getType() {
      return type;
    }

    public boolean isNullable() {
      return nullable;
    }
  }
}
