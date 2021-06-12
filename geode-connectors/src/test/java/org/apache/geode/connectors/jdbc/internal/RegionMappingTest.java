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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;

public class RegionMappingTest {

  private String name;
  private String fieldName1;

  private RegionMapping mapping;

  @Before
  public void setUp() {
    name = "name";
    fieldName1 = "myField1";
  }

  @Test
  public void initiatedWithNullValues() {
    mapping = new RegionMapping(null, "pdxClassName", null, null, null, null, null);

    assertThat(mapping.getTableName()).isNull();
    assertThat(mapping.getRegionName()).isNull();
    assertThat(mapping.getDataSourceName()).isNull();
    assertThat(mapping.getPdxName()).isEqualTo("pdxClassName");
    assertThat(mapping.getIds()).isNull();
    assertThat(mapping.getCatalog()).isNull();
    assertThat(mapping.getSchema()).isNull();
  }

  @Test
  public void hasCorrectTableName() {
    mapping = new RegionMapping(null, null, name, null, null, null, null);

    assertThat(mapping.getTableName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectRegionName() {
    mapping = new RegionMapping(name, null, null, null, null, null, null);

    assertThat(mapping.getRegionName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectConfigName() {
    mapping = new RegionMapping(null, null, null, name, null, null, null);

    assertThat(mapping.getDataSourceName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectPdxClassName() {
    mapping = new RegionMapping(null, name, null, null, null, null, null);

    assertThat(mapping.getPdxName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectIds() {
    String ids = "ids";
    mapping = new RegionMapping(null, null, null, null, ids, null, null);

    assertThat(mapping.getIds()).isEqualTo(ids);
  }

  @Test
  public void hasCorrectCatalog() {
    String catalog = "catalog";
    mapping = new RegionMapping(null, null, null, null, null, catalog, null);

    assertThat(mapping.getCatalog()).isEqualTo(catalog);
  }

  @Test
  public void hasCorrectSchema() {
    String schema = "schema";
    mapping = new RegionMapping(null, null, null, null, null, null, schema);

    assertThat(mapping.getSchema()).isEqualTo(schema);
  }

  @Test
  public void verifyTwoDefaultInstancesAreEqual() {
    RegionMapping rm1 =
        new RegionMapping("regionName", "pdxClassName", null, "dataSourceName", null, null, null);
    RegionMapping rm2 =
        new RegionMapping("regionName", "pdxClassName", null, "dataSourceName", null, null, null);
    assertThat(rm1).isEqualTo(rm2);
  }

  @Test
  public void verifyTwoInstancesThatAreEqualHaveSameHashCode() {
    RegionMapping rm1 = new RegionMapping("regionName",
        "pdxClassName", "tableName", "dataSourceName", "ids", "catalog", "schema");

    RegionMapping rm2 = new RegionMapping("regionName",
        "pdxClassName", "tableName", "dataSourceName", "ids", "catalog", "schema");

    assertThat(rm1.hashCode()).isEqualTo(rm2.hashCode());
  }

  @Test
  public void verifyToStringGivenAllAttributes() {
    RegionMapping rm = new RegionMapping("regionName", "pdxClassName", "tableName",
        "dataSourceName", "ids", "catalog", "schema");
    rm.addFieldMapping(new FieldMapping("pdxName", "pdxType", "jdbcName", "jdbcType", true));

    String result = rm.toString();

    assertThat(result).isEqualTo(
        "RegionMapping{regionName='regionName', pdxName='pdxClassName', tableName='tableName', dataSourceName='dataSourceName', ids='ids', specifiedIds='true', catalog='catalog', schema='schema', fieldMapping='[FieldMapping [pdxName=pdxName, pdxType=pdxType, jdbcName=jdbcName, jdbcType=jdbcType, jdbcNullable=true]]'}");
  }

  @Test
  public void verifyToStringGivenRequiredAttributes() {
    RegionMapping rm =
        new RegionMapping("regionName", "pdxClassName", null, "dataSourceName", null, null, null);

    String result = rm.toString();

    assertThat(result).isEqualTo(
        "RegionMapping{regionName='regionName', pdxName='pdxClassName', tableName='null', dataSourceName='dataSourceName', ids='null', specifiedIds='false', catalog='null', schema='null', fieldMapping='[]'}");
  }

  @Test
  public void verifyThatMappingIsEqualToItself() {
    mapping = new RegionMapping(null, "pdxClassName", null, null, null, null, null);
    boolean result = mapping.equals(mapping);
    assertThat(mapping.hashCode()).isEqualTo(mapping.hashCode());
    assertThat(result).isTrue();
  }

  @Test
  public void verifyThatNullIsNotEqual() {
    mapping = new RegionMapping(null, null, null, null, null, null, null);
    boolean result = mapping.equals(null);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyOtherClassIsNotEqual() {
    mapping = new RegionMapping(null, null, null, null, null, null, null);
    boolean result = mapping.equals("not equal");
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentRegionNamesAreNotEqual() {
    RegionMapping rm1 =
        new RegionMapping(null, null, null, null, null, null, null);
    RegionMapping rm2 =
        new RegionMapping("name", null, null, null, null, null, null);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentPdxClassNameAreNotEqual() {
    RegionMapping rm1 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, null);
    RegionMapping rm2 =
        new RegionMapping(null, "pdxClass", null, null, null, null, null);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentTablesAreNotEqual() {
    RegionMapping rm1 =
        new RegionMapping(null, "pdxClassName", "table1", null, null, null, null);
    RegionMapping rm2 =
        new RegionMapping(null, "pdxClassName", "table2", null, null, null, null);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentDataSourcesAreNotEqual() {
    RegionMapping rm1 =
        new RegionMapping(null, "pdxClassName", null, "datasource1", null, null, null);
    RegionMapping rm2 =
        new RegionMapping(null, "pdxClassName", null, "datasource2", null, null, null);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentIdsAreNotEqual() {
    RegionMapping rm1 =
        new RegionMapping(null, "pdxClassName", null, null, "ids1", null, null);
    RegionMapping rm2 =
        new RegionMapping(null, "pdxClassName", null, null, "ids2", null, null);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentCatalogsAreNotEqual() {
    RegionMapping rm1 =
        new RegionMapping(null, "pdxClassName", null, null, null, "catalog1", null);
    RegionMapping rm2 =
        new RegionMapping(null, "pdxClassName", null, null, null, "catalog2", null);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentSchemasAreNotEqual() {
    RegionMapping rm1 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, "schema1");
    RegionMapping rm2 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, "schema2");
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentFieldMappingsAreNotEqual() {
    RegionMapping rm1 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, null);
    rm1.addFieldMapping(
        new FieldMapping("myPdxName", "myPdxType", "myJdbcName", "myJdbcType", false));
    RegionMapping rm2 =
        new RegionMapping(null, "pdxClassName", null, null, null, null, null);
    rm2.addFieldMapping(
        new FieldMapping("myPdxName", "myPdxType", "myJdbcName", "myOtherJdbcType", false));
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

}
