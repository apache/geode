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

import org.apache.geode.annotations.Experimental;

@Experimental
public class RegionMappingBuilder {

  private static final String MAPPINGS_DELIMITER = ":";
  private String regionName;
  private String pdxClassName;
  private String tableName;
  private String connectionConfigName;
  private Boolean primaryKeyInValue;
  private Map<String, String> fieldToColumnMap = new HashMap<>();

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

  public RegionMappingBuilder withPrimaryKeyInValue(String v) {
    if (v != null) {
      withPrimaryKeyInValue(Boolean.parseBoolean(v));
    }
    return this;
  }

  public RegionMappingBuilder withPrimaryKeyInValue(Boolean v) {
    this.primaryKeyInValue = v;
    return this;
  }

  public RegionMappingBuilder withFieldToColumnMapping(String fieldName, String columnMapping) {
    this.fieldToColumnMap.put(fieldName, columnMapping);
    return this;
  }

  public RegionMappingBuilder withFieldToColumnMappings(String[] mappings) {
    if (mappings == null) {
      fieldToColumnMap = null;
    } else {
      for (String mapping : mappings) {
        if (mapping.isEmpty()) {
          continue;
        }
        String[] keyValuePair = mapping.split(MAPPINGS_DELIMITER);
        validateParam(keyValuePair, mapping);
        fieldToColumnMap.put(keyValuePair[0], keyValuePair[1]);
      }
    }
    return this;
  }

  private void validateParam(String[] paramKeyValue, String mapping) {
    // paramKeyValue is produced by split which will never give us
    // an empty second element
    if (paramKeyValue.length != 2 || paramKeyValue[0].isEmpty()) {
      throw new IllegalArgumentException("Field to column mapping '" + mapping
          + "' is not of the form 'Field" + MAPPINGS_DELIMITER + "Column'");
    }
  }

  public RegionMapping build() {
    return new RegionMapping(regionName, pdxClassName, tableName, connectionConfigName,
        primaryKeyInValue, fieldToColumnMap);
  }
}
