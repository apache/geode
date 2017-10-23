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
package org.apache.geode.connectors.jdbc.internal.xml;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionMappingBuilderTest {

  @Test
  public void createsMappingWithDefaultValuesIfNonAreSet() {
    RegionMapping regionMapping = new RegionMappingBuilder().build();

    assertThat(regionMapping.getRegionName()).isNull();
    assertThat(regionMapping.getTableName()).isNull();
    assertThat(regionMapping.getPdxClassName()).isNull();
    assertThat(regionMapping.getConnectionConfigName()).isNull();
    assertThat(regionMapping.isPrimaryKeyInValue()).isFalse();
  }

  @Test
  public void createsMappingWithSpecifiedValues() {
    RegionMappingBuilder builder = new RegionMappingBuilder();
    RegionMapping regionMapping = builder.withTableName("tableName").withRegionName("regionName")
        .withPrimaryKeyInValue("true").withPdxClassName("pdxClassName")
        .withConnectionConfigName("configName").withFieldToColumnMapping("fieldName", "columnName")
        .build();

    assertThat(regionMapping.getRegionName()).isEqualTo("regionName");
    assertThat(regionMapping.getTableName()).isEqualTo("tableName");
    assertThat(regionMapping.getPdxClassName()).isEqualTo("pdxClassName");
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo("configName");
    assertThat(regionMapping.isPrimaryKeyInValue()).isTrue();
    assertThat(regionMapping.getColumnNameForField("fieldName")).isEqualTo("columnName");
  }
}
