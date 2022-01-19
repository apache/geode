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
package org.apache.geode.connectors.jdbc.internal.configuration;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.XSDRootElement;

/**
 * Java class for xsd mapping element.
 */
@Experimental
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {"fieldMappings"})
@XmlRootElement(name = "mapping", namespace = "http://geode.apache.org/schema/jdbc")
@XSDRootElement(namespace = "http://geode.apache.org/schema/jdbc",
    schemaLocation = "http://geode.apache.org/schema/jdbc/jdbc-1.0.xsd")
public class RegionMapping extends CacheElement {

  @XmlElement(name = "field-mapping", namespace = "http://geode.apache.org/schema/jdbc")
  protected final List<FieldMapping> fieldMappings = new ArrayList<>();
  @XmlAttribute(name = "data-source")
  protected String dataSourceName;
  @XmlAttribute(name = "table")
  protected String tableName;
  @XmlAttribute(name = "pdx-name")
  protected String pdxName;
  @XmlAttribute(name = "ids")
  protected String ids;
  @XmlAttribute(name = "specified-ids")
  protected boolean specifiedIds;
  @XmlAttribute(name = "catalog")
  protected String catalog;
  @XmlAttribute(name = "schema")
  protected String schema;

  @XmlTransient
  protected String regionName;

  public static final String ELEMENT_ID = "jdbc-mapping";

  public RegionMapping() {}

  public RegionMapping(String regionName, String pdxName, String tableName,
      String dataSourceName, String ids, String catalog, String schema) {
    this.regionName = regionName;
    this.pdxName = pdxName;
    this.tableName = tableName;
    this.dataSourceName = dataSourceName;
    this.ids = ids;
    specifiedIds = !StringUtils.isEmpty(ids);
    this.catalog = catalog;
    this.schema = schema;
  }

  public void setDataSourceName(String dataSourceName) {
    this.dataSourceName = dataSourceName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setPdxName(String pdxName) {
    this.pdxName = pdxName;
  }

  public void setIds(String ids) {
    this.ids = ids;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public void setSpecifiedIds(boolean specifiedIds) {
    this.specifiedIds = specifiedIds;
  }

  public String getDataSourceName() {
    return dataSourceName;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getPdxName() {
    return pdxName;
  }

  public String getIds() {
    return ids;
  }

  public boolean getSpecifiedIds() {
    return specifiedIds;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }

  public String getTableName() {
    return tableName;
  }

  public List<FieldMapping> getFieldMappings() {
    return fieldMappings;
  }

  public void addFieldMapping(FieldMapping value) {
    fieldMappings.add(value);
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

    return isEqual(regionName, that.regionName)
        && isEqual(pdxName, that.pdxName)
        && isEqual(tableName, that.tableName)
        && isEqual(dataSourceName, that.dataSourceName)
        && isEqual(ids, that.ids)
        && specifiedIds == that.specifiedIds
        && isEqual(catalog, that.catalog)
        && isEqual(schema, that.schema)
        && isEqual(fieldMappings, that.fieldMappings);
  }

  private static boolean isEqual(Object o1, Object o2) {
    return o1 != null ? o1.equals(o2) : o2 == null;
  }

  @Override
  public int hashCode() {
    int result = regionName != null ? regionName.hashCode() : 0;
    result = 31 * result + pdxName.hashCode();
    result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
    result = 31 * result + (dataSourceName != null ? dataSourceName.hashCode() : 0);
    result = 31 * result + (ids != null ? ids.hashCode() : 0);
    result = 31 * result + (catalog != null ? catalog.hashCode() : 0);
    result = 31 * result + (schema != null ? schema.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RegionMapping{"
        + "regionName='" + regionName + '\''
        + ", pdxName='" + pdxName + '\''
        + ", tableName='" + tableName + '\''
        + ", dataSourceName='" + dataSourceName + '\''
        + ", ids='" + ids + '\''
        + ", specifiedIds='" + specifiedIds + '\''
        + ", catalog='" + catalog + '\''
        + ", schema='" + schema + '\''
        + ", fieldMapping='" + fieldMappings + '\''
        + '}';
  }

  @Override
  public String getId() {
    return ELEMENT_ID;
  }
}
