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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.annotations.Experimental;

@Experimental
public class RegionMapping implements Serializable {
  private final String regionName;
  private final String pdxClassName;
  private final String tableName;
  private final String connectionConfigName;
  private final Boolean primaryKeyInValue;
  private final Map<String, String> fieldToColumnMap;
  private final Map<String, String> columnToFieldMap;

  public RegionMapping(String regionName, String pdxClassName, String tableName,
      String connectionConfigName, Boolean primaryKeyInValue,
      Map<String, String> fieldToColumnMap) {
    this.regionName = regionName;
    this.pdxClassName = pdxClassName;
    this.tableName = tableName;
    this.connectionConfigName = connectionConfigName;
    this.primaryKeyInValue = primaryKeyInValue;
    this.fieldToColumnMap =
        fieldToColumnMap == null ? null : Collections.unmodifiableMap(fieldToColumnMap);
    this.columnToFieldMap = createReverseMap(fieldToColumnMap);
  }

  private static Map<String, String> createReverseMap(Map<String, String> fieldToColumnMap) {
    if (fieldToColumnMap == null) {
      return null;
    }
    Map<String, String> reverseMap = new HashMap<>();
    for (Map.Entry<String, String> entry : fieldToColumnMap.entrySet()) {
      String reverseMapKey = entry.getValue().toLowerCase();
      String reverseMapValue = entry.getKey();
      if (reverseMap.containsKey(reverseMapKey)) {
        throw new IllegalArgumentException(
            "The field " + reverseMapValue + " can not be mapped to more than one column.");
      }
      reverseMap.put(reverseMapKey, reverseMapValue);
    }
    return Collections.unmodifiableMap(reverseMap);
  }

  public String getConnectionConfigName() {
    return connectionConfigName;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getPdxClassName() {
    return pdxClassName;
  }

  public String getTableName() {
    return tableName;
  }

  public Boolean isPrimaryKeyInValue() {
    return primaryKeyInValue;
  }

  public String getRegionToTableName() {
    if (tableName == null) {
      return regionName;
    }
    return tableName;
  }

  public String getColumnNameForField(String fieldName) {
    String columnName = null;
    if (fieldToColumnMap != null) {
      columnName = fieldToColumnMap.get(fieldName);
    }
    return columnName != null ? columnName : fieldName;
  }

  public String getFieldNameForColumn(String columnName) {
    String canonicalColumnName = columnName.toLowerCase();
    String fieldName = null;
    if (this.columnToFieldMap != null) {
      fieldName = columnToFieldMap.get(canonicalColumnName);
    }
    return fieldName != null ? fieldName : canonicalColumnName;
  }

  public Map<String, String> getFieldToColumnMap() {
    return fieldToColumnMap;
  }

  /**
   * For unit tests
   */
  Map<String, String> getColumnToFieldMap() {
    return this.columnToFieldMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RegionMapping that = (RegionMapping) o;

    if (primaryKeyInValue != that.primaryKeyInValue) {
      return false;
    }
    if (regionName != null ? !regionName.equals(that.regionName) : that.regionName != null) {
      return false;
    }
    if (pdxClassName != null ? !pdxClassName.equals(that.pdxClassName)
        : that.pdxClassName != null) {
      return false;
    }
    if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) {
      return false;
    }
    if (connectionConfigName != null ? !connectionConfigName.equals(that.connectionConfigName)
        : that.connectionConfigName != null) {
      return false;
    }
    return fieldToColumnMap != null ? fieldToColumnMap.equals(that.fieldToColumnMap)
        : that.fieldToColumnMap == null;
  }

  @Override
  public int hashCode() {
    int result = regionName != null ? regionName.hashCode() : 0;
    result = 31 * result + (pdxClassName != null ? pdxClassName.hashCode() : 0);
    result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
    result = 31 * result + (connectionConfigName != null ? connectionConfigName.hashCode() : 0);
    result = 31 * result + (primaryKeyInValue ? 1 : 0);
    result = 31 * result + (fieldToColumnMap != null ? fieldToColumnMap.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RegionMapping{" + "regionName='" + regionName + '\'' + ", pdxClassName='" + pdxClassName
        + '\'' + ", tableName='" + tableName + '\'' + ", connectionConfigName='"
        + connectionConfigName + '\'' + ", primaryKeyInValue=" + primaryKeyInValue
        + ", fieldToColumnMap=" + fieldToColumnMap + '}';
  }
}
