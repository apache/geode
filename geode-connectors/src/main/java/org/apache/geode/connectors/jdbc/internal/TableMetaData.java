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

import org.apache.geode.connectors.jdbc.JdbcConnectorException;

public class TableMetaData implements TableMetaDataView {

  private final String keyColumnName;
  private final HashMap<String, Integer> columnNameToTypeMap = new HashMap<>();

  public TableMetaData(String keyColumnName) {
    this.keyColumnName = keyColumnName;
  }

  @Override
  public String getKeyColumnName() {
    return this.keyColumnName;
  }

  @Override
  public int getColumnDataType(String columnName) {
    Integer dataType = this.columnNameToTypeMap.get(columnName.toLowerCase());
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
    Integer previousDataType = this.columnNameToTypeMap.put(columnName.toLowerCase(), dataType);
    if (previousDataType != null) {
      throw new JdbcConnectorException(
          "Column names must be different in case. Two columns both have the name "
              + columnName.toLowerCase());
    }
  }
}
