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

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableMetaData implements TableMetaDataView {

  private final String quotedTablePath;
  private final List<String> keyColumnNames;
  private final Map<String, Integer> columnNameToTypeMap;
  private final String identifierQuoteString;

  public TableMetaData(String tableName, List<String> keyColumnNames, String quoteString,
      Map<String, Integer> dataTypes) {
    this.quotedTablePath = quoteString + tableName + quoteString;
    this.keyColumnNames = keyColumnNames;
    this.columnNameToTypeMap = dataTypes;
    this.identifierQuoteString = quoteString;
  }

  @Override
  public String getQuotedTablePath() {
    return quotedTablePath;
  }

  @Override
  public List<String> getKeyColumnNames() {
    return this.keyColumnNames;
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

  @Override
  public String getIdentifierQuoteString() {
    return this.identifierQuoteString;
  }
}
