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

import java.util.HashMap;
import java.util.Map;

public class RegionMappingBuilder {

  private static final String MAPPINGS_DELIMITER = ":";
  private String regionName;
  private String pdxClassName;
  private String tableName;
  private String connectionConfigName;
  private boolean primaryKeyInValue;
  private final Map<String, String> fieldToColumnMap = new HashMap<>();

  public RegionMappingBuilder withRegionName(String regionName) {
    this.regionName = regionName;
    return this;
  }

  public RegionMappingBuilder withPdxClassName(String pdxClassName) {
    this.pdxClassName = pdxClassName;
    return this;
  }

  public RegionMappingBuilder withTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public RegionMappingBuilder withConnectionConfigName(String connectionConfigName) {
    this.connectionConfigName = connectionConfigName;
    return this;
  }

  // TODO: delete withPrimaryKeyInValue(String)
  public RegionMappingBuilder withPrimaryKeyInValue(String primaryKeyInValue) {
    this.primaryKeyInValue = Boolean.parseBoolean(primaryKeyInValue);
    return this;
  }

  public RegionMappingBuilder withPrimaryKeyInValue(boolean primaryKeyInValue) {
    this.primaryKeyInValue = primaryKeyInValue;
    return this;
  }

  public RegionMappingBuilder withFieldToColumnMapping(String fieldName, String columnMapping) {
    this.fieldToColumnMap.put(fieldName, columnMapping);
    return this;
  }

  public RegionMappingBuilder withFieldToColumnMappings(String[] mappings) {
    for (String mapping : mappings) {
      String[] keyValuePair = mapping.split(MAPPINGS_DELIMITER);
      if (keyValuePair.length == 2) {
        fieldToColumnMap.put(keyValuePair[0], keyValuePair[1]);
      }
    }
    return this;
  }

  public RegionMapping build() {
    return new RegionMapping(regionName, pdxClassName, tableName, connectionConfigName,
        primaryKeyInValue, fieldToColumnMap);
  }
}
