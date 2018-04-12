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

import java.util.HashMap;
import java.util.Set;

public class TableMetaData implements TableMetaDataView {

  private final String tableName;
  private final String keyColumnName;
  private final HashMap<String, Integer> columnNameToTypeMap;
  private final String identifierQuoteString;

  public TableMetaData(String tableName, String keyColumnName, String quoteString) {
    this.tableName = tableName;
    this.keyColumnName = keyColumnName;
    this.columnNameToTypeMap = new HashMap<>();
    this.identifierQuoteString = quoteString;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public String getKeyColumnName() {
    return this.keyColumnName;
  }

  @Override
  public int getColumnDataType(String columnName) {
    Integer dataType = this.columnNameToTypeMap.get(columnName);
    if (dataType == null) {
      return 0;
    }
    return dataType;
  }

  @Override
  public Set<String> getColumnNames() {
    return columnNameToTypeMap.keySet();
  }

  public void addDataType(String columnName, int dataType) {
    this.columnNameToTypeMap.put(columnName, dataType);
  }

  @Override
  public String getIdentifierQuoteString() {
    return this.identifierQuoteString;
  }
}
