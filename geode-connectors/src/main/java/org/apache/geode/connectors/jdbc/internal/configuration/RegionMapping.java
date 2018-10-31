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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
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
 *       &lt;sequence>
 *         &lt;element name="field-mapping" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;simpleContent>
 *               &lt;extension base="&lt;http://www.w3.org/2001/XMLSchema>string">
 *                 &lt;attribute name="field-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="column-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/extension>
 *             &lt;/simpleContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/sequence>
 *       &lt;attribute name="connection-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="table" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="pdx-class" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="primary-key-in-value" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@Experimental
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {"fieldMapping"})
@XmlRootElement(name = "mapping", namespace = "http://geode.apache.org/schema/jdbc")
@XSDRootElement(namespace = "http://geode.apache.org/schema/jdbc",
    schemaLocation = "http://geode.apache.org/schema/jdbc/jdbc-1.0.xsd")
public class RegionMapping implements CacheElement {
  private static final String MAPPINGS_DELIMITER = ":";

  @XmlElement(name = "field-mapping", namespace = "http://geode.apache.org/schema/jdbc")
  protected List<FieldMapping> fieldMapping;
  @XmlTransient
  protected boolean fieldMappingModified = false;
  @XmlAttribute(name = "connection-name")
  protected String connectionConfigName;
  @XmlAttribute(name = "table")
  protected String tableName;
  @XmlAttribute(name = "pdx-class")
  protected String pdxClassName;
  @XmlAttribute(name = "primary-key-in-value")
  protected Boolean primaryKeyInValue;

  @XmlTransient
  protected String regionName;

  public static final String ELEMENT_ID = "jdbc-mapping";

  public RegionMapping() {}

  public RegionMapping(String regionName, String pdxClassName, String tableName,
      String connectionConfigName, Boolean primaryKeyInValue) {
    this.regionName = regionName;
    this.pdxClassName = pdxClassName;
    this.tableName = tableName;
    this.connectionConfigName = connectionConfigName;
    this.primaryKeyInValue = primaryKeyInValue;
  }

  public void setFieldMapping(String[] mappings) {
    if (mappings != null) {
      this.fieldMapping =
          Arrays.stream(mappings).filter(Objects::nonNull).filter(s -> !s.isEmpty()).map(s -> {
            String[] keyValuePair = s.split(MAPPINGS_DELIMITER);
            validateParam(keyValuePair, s);
            return new FieldMapping(keyValuePair[0],
                keyValuePair[1]);
          }).collect(Collectors.toList());
      fieldMappingModified = true;
    }
  }

  private void validateParam(String[] paramKeyValue, String mapping) {
    // paramKeyValue is produced by split which will never give us
    // an empty second element
    if (paramKeyValue.length != 2 || paramKeyValue[0].isEmpty()) {
      throw new IllegalArgumentException("Field to column mapping '" + mapping
          + "' is not of the form 'Field" + MAPPINGS_DELIMITER + "Column'");
    }
  }

  public void setConnectionConfigName(String connectionConfigName) {
    this.connectionConfigName = connectionConfigName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setPdxClassName(String pdxClassName) {
    this.pdxClassName = pdxClassName;
  }

  public void setPrimaryKeyInValue(Boolean primaryKeyInValue) {
    this.primaryKeyInValue = primaryKeyInValue;
  }

  public boolean isFieldMappingModified() {
    return fieldMappingModified;
  }

  public List<FieldMapping> getFieldMapping() {
    if (fieldMapping == null) {
      fieldMapping = new ArrayList<>();
    }
    return fieldMapping;
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

  public Boolean getPrimaryKeyInValue() {
    return primaryKeyInValue;
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

  public String getColumnNameForField(String fieldName, TableMetaDataView tableMetaDataView) {
    FieldMapping configured = getFieldMapping().stream()
        .filter(m -> m.getFieldName().equals(fieldName)).findAny().orElse(null);
    if (configured != null) {
      return configured.getColumnName();
    }

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
    String fieldName = null;

    FieldMapping configured = getFieldMapping().stream()
        .filter(m -> m.getColumnName().equals(columnName)).findAny().orElse(null);

    if (configured != null) {
      return configured.getFieldName();
    }

    if (getPdxClassName() == null) {
      if (columnName.equals(columnName.toUpperCase())) {
        fieldName = columnName.toLowerCase();
      } else {
        fieldName = columnName;
      }
    } else {
      Set<PdxType> pdxTypes = getPdxTypesForClassName(typeRegistry);
      fieldName = findExactMatch(columnName, pdxTypes);
      if (fieldName == null) {
        fieldName = findCaseInsensitiveMatch(columnName, pdxTypes);
      }
    }
    assert fieldName != null;

    return fieldName;
  }

  private Set<PdxType> getPdxTypesForClassName(TypeRegistry typeRegistry) {
    Set<PdxType> pdxTypes = typeRegistry.getPdxTypesForClassName(getPdxClassName());
    if (pdxTypes.isEmpty()) {
      throw new JdbcConnectorException(
          "The class " + getPdxClassName() + " has not been pdx serialized.");
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
      throw new JdbcConnectorException("The class " + getPdxClassName()
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
    if (fieldMapping != null ? !fieldMapping.equals(that.fieldMapping)
        : that.fieldMapping != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = regionName != null ? regionName.hashCode() : 0;
    result = 31 * result + (pdxClassName != null ? pdxClassName.hashCode() : 0);
    result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
    result = 31 * result + (connectionConfigName != null ? connectionConfigName.hashCode() : 0);
    result = 31 * result + (primaryKeyInValue ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RegionMapping{" + "regionName='" + regionName + '\'' + ", pdxClassName='"
        + pdxClassName + '\'' + ", tableName='" + tableName + '\'' + ", connectionConfigName='"
        + connectionConfigName + '\'' + ", primaryKeyInValue=" + primaryKeyInValue + '}';
  }

  @Override
  public String getId() {
    return ELEMENT_ID;
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  public static class FieldMapping implements Serializable {
    @XmlAttribute(name = "field-name")
    protected String fieldName;
    @XmlAttribute(name = "column-name")
    protected String columnName;

    public FieldMapping() {}

    public FieldMapping(String fieldName, String columnName) {
      this.fieldName = fieldName;
      this.columnName = columnName;
    }

    /**
     * Gets the value of the fieldName property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getFieldName() {
      return fieldName;
    }

    /**
     * Sets the value of the fieldName property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setFieldName(String value) {
      this.fieldName = value;
    }

    /**
     * Gets the value of the columnName property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getColumnName() {
      return columnName;
    }

    /**
     * Sets the value of the columnName property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setColumnName(String value) {
      this.columnName = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      FieldMapping that = (FieldMapping) o;
      return Objects.equals(fieldName, that.fieldName)
          && Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
      int result = fieldName != null ? fieldName.hashCode() : 0;
      result = 31 * result + (columnName != null ? columnName.hashCode() : 0);

      return result;
    }
  }
}
