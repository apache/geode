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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.XSDRootElement;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * <p>
 * Java class for anonymous complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;attribute name="data-source" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="table" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="pdx-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="ids" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="catalog" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@Experimental
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "")
@XmlRootElement(name = "mapping", namespace = "http://geode.apache.org/schema/jdbc")
@XSDRootElement(namespace = "http://geode.apache.org/schema/jdbc",
    schemaLocation = "http://geode.apache.org/schema/jdbc/jdbc-1.0.xsd")
public class RegionMapping implements CacheElement {

  @XmlAttribute(name = "data-source")
  protected String dataSourceName;
  @XmlAttribute(name = "table")
  protected String tableName;
  @XmlAttribute(name = "pdx-name")
  protected String pdxName;
  @XmlAttribute(name = "ids")
  protected String ids;
  @XmlAttribute(name = "catalog")
  protected String catalog;
  @XmlAttribute(name = "schema")
  protected String schema;
  @XmlAttribute(name = "groups")
  protected String groups;

  @XmlTransient
  protected String regionName;

  public static final String ELEMENT_ID = "jdbc-mapping";

  public RegionMapping() {}

  public RegionMapping(String regionName, String pdxName, String tableName,
      String dataSourceName, String ids, String catalog, String schema, String groups) {
    this.regionName = regionName;
    this.pdxName = pdxName;
    this.tableName = tableName;
    this.dataSourceName = dataSourceName;
    this.ids = ids;
    this.catalog = catalog;
    this.schema = schema;
    this.groups = groups;
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

  public void setGroups(String groups) {
    this.schema = groups;
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

  public String getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }

  public String getGroups() {
    return groups;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnNameForField(String fieldName, TableMetaDataView tableMetaDataView) {
    Set<String> columnNames = tableMetaDataView.getColumnNames();
    if (columnNames.contains(fieldName)) {
      return fieldName;
    }

    List<String> ignoreCaseMatch = columnNames.stream().filter(c -> c.equalsIgnoreCase(fieldName))
        .collect(Collectors.toList());
    if (ignoreCaseMatch.size() > 1) {
      throw new JdbcConnectorException(
          "The SQL table has at least two columns that match the PDX field: " + fieldName);
    }

    if (ignoreCaseMatch.size() == 1) {
      return ignoreCaseMatch.get(0);
    }

    // there is no match either in the configured mapping or the table columns
    return fieldName;
  }

  public String getFieldNameForColumn(String columnName, TypeRegistry typeRegistry) {
    Set<PdxType> pdxTypes = getPdxTypesForClassName(typeRegistry);
    String fieldName = findExactMatch(columnName, pdxTypes);
    if (fieldName == null) {
      fieldName = findCaseInsensitiveMatch(columnName, pdxTypes);
    }
    return fieldName;
  }

  private Set<PdxType> getPdxTypesForClassName(TypeRegistry typeRegistry) {
    Set<PdxType> pdxTypes = typeRegistry.getPdxTypesForClassName(getPdxName());
    if (pdxTypes.isEmpty()) {
      throw new JdbcConnectorException(
          "The class " + getPdxName() + " has not been pdx serialized.");
    }
    return pdxTypes;
  }

  /**
   * Given a column name and a set of pdx types, find the field name in those types that match,
   * ignoring case, the column name.
   *
   * @return the matching field name or null if no match
   * @throws JdbcConnectorException if no fields match
   * @throws JdbcConnectorException if more than one field matches
   */
  private String findCaseInsensitiveMatch(String columnName, Set<PdxType> pdxTypes) {
    HashSet<String> matchingFieldNames = new HashSet<>();
    for (PdxType pdxType : pdxTypes) {
      for (String existingFieldName : pdxType.getFieldNames()) {
        if (existingFieldName.equalsIgnoreCase(columnName)) {
          matchingFieldNames.add(existingFieldName);
        }
      }
    }
    if (matchingFieldNames.isEmpty()) {
      throw new JdbcConnectorException("The class " + getPdxName()
          + " does not have a field that matches the column " + columnName);
    } else if (matchingFieldNames.size() > 1) {
      throw new JdbcConnectorException(
          "Could not determine what pdx field to use for the column name " + columnName
              + " because the pdx fields " + matchingFieldNames + " all match it.");
    }
    return matchingFieldNames.iterator().next();
  }

  /**
   * Given a column name, search the given pdxTypes for a field whose name exactly matches the
   * column name.
   *
   * @return the matching field name or null if no match
   */
  private String findExactMatch(String columnName, Set<PdxType> pdxTypes) {
    for (PdxType pdxType : pdxTypes) {
      if (pdxType.getPdxField(columnName) != null) {
        return columnName;
      }
    }
    return null;
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
        && isEqual(catalog, that.catalog)
        && isEqual(schema, that.schema)
            && isEqual(groups, that.groups);
  }

  private static boolean isEqual(String s1, String s2) {
    return s1 != null ? s1.equals(s2) : s2 == null;
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
    result = 31 * result + (groups != null ? groups.hashCode() : 0);
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
        + ", catalog='" + catalog + '\''
        + ", schema='" + schema + '\''
            + ", groups='" + groups + '\''
        + '}';
  }

  @Override
  public String getId() {
    return ELEMENT_ID;
  }
}
