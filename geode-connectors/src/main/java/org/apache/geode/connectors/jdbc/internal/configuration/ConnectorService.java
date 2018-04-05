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

import static org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlParser.PARAMS_DELIMITER;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
import org.apache.geode.annotations.TestingOnly;
import org.apache.geode.cache.configuration.CacheElement;
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
 *         &lt;element name="connection" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;simpleContent>
 *               &lt;extension base="&lt;http://www.w3.org/2001/XMLSchema>string">
 *                 &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="url" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="user" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="password" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="parameters" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/extension>
 *             &lt;/simpleContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="region-mapping" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="field-mapping" maxOccurs="unbounded" minOccurs="0">
 *                     &lt;complexType>
 *                       &lt;simpleContent>
 *                         &lt;extension base="&lt;http://www.w3.org/2001/XMLSchema>string">
 *                           &lt;attribute name="field-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                           &lt;attribute name="column-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/extension>
 *                       &lt;/simpleContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                 &lt;/sequence>
 *                 &lt;attribute name="connection-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="region" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="table" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="pdx-class" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="primary-key-in-value" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/sequence>
 *       &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" fixed="connector-service" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@Experimental
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {"connection", "regionMapping"})
@XmlRootElement(name = "connector-service", namespace = "http://geode.apache.org/schema/jdbc")
public class ConnectorService implements CacheElement {
  public static String SCHEMA =
      "http://geode.apache.org/schema/jdbc http://geode.apache.org/schema/jdbc/jdbc-1.0.xsd";

  @XmlElement(namespace = "http://geode.apache.org/schema/jdbc")
  protected List<ConnectorService.Connection> connection;
  @XmlElement(name = "region-mapping", namespace = "http://geode.apache.org/schema/jdbc")
  protected List<ConnectorService.RegionMapping> regionMapping;
  @XmlAttribute(name = "name")
  protected String name;

  /**
   * Gets the value of the connection property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the connection property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getConnection().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link ConnectorService.Connection }
   *
   *
   */
  public List<ConnectorService.Connection> getConnection() {
    if (connection == null) {
      connection = new ArrayList<ConnectorService.Connection>();
    }
    return this.connection;
  }

  /**
   * Gets the value of the regionMapping property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the regionMapping property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getRegionMapping().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link ConnectorService.RegionMapping }
   *
   *
   */
  public List<ConnectorService.RegionMapping> getRegionMapping() {
    if (regionMapping == null) {
      regionMapping = new ArrayList<ConnectorService.RegionMapping>();
    }
    return this.regionMapping;
  }

  /**
   * Gets the value of the name property.
   *
   * @return
   *         possible object is
   *         {@link String }
   *
   */
  public String getName() {
    if (name == null) {
      return "connector-service";
    } else {
      return name;
    }
  }

  /**
   * Sets the value of the name property.
   *
   * @param value
   *        allowed object is
   *        {@link String }
   *
   */
  public void setName(String value) {
    this.name = value;
  }

  @Override
  public String getId() {
    return getName();
  }


  /**
   * <p>
   * Java class for anonymous complex type.
   *
   * <p>
   * The following schema fragment specifies the expected content contained within this class.
   *
   * <pre>
   * &lt;complexType>
   *   &lt;simpleContent>
   *     &lt;extension base="&lt;http://www.w3.org/2001/XMLSchema>string">
   *       &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="url" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="user" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="password" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="parameters" type="{http://www.w3.org/2001/XMLSchema}string" />
   *     &lt;/extension>
   *   &lt;/simpleContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class Connection implements CacheElement {
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "url")
    protected String url;
    @XmlAttribute(name = "user")
    protected String user;
    @XmlAttribute(name = "password")
    protected String password;
    @XmlAttribute(name = "parameters")
    protected String parameters;

    @XmlTransient
    protected Map<String, String> parameterMap = new HashMap<>();

    public Connection() {};

    public Connection(String name, String url, String user, String password, String parameters) {
      this.name = name;
      this.url = url;
      this.user = user;
      this.password = password;
      setParameters(parameters);
    }

    public Connection(String name, String url, String user, String password, String[] parameters) {
      this.name = name;
      this.url = url;
      this.user = user;
      this.password = password;
      setParameters(parameters);
    }

    public Connection(String name, String url, String user, String password,
        Map<String, String> parameterMap) {
      this.name = name;
      this.url = url;
      this.user = user;
      this.password = password;
      setParameters(parameterMap);
    }

    /**
     * Gets the value of the name property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getName() {
      return name;
    }

    /**
     * Sets the value of the name property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setName(String value) {
      this.name = value;
    }

    /**
     * Gets the value of the url property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getUrl() {
      return url;
    }

    /**
     * Sets the value of the url property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setUrl(String value) {
      this.url = value;
    }

    /**
     * Gets the value of the user property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getUser() {
      return user;
    }

    /**
     * Sets the value of the user property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setUser(String value) {
      this.user = value;
    }

    /**
     * Gets the value of the password property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getPassword() {
      return password;
    }

    /**
     * Sets the value of the password property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setPassword(String value) {
      this.password = value;
    }

    /**
     * Gets the value of the parameters property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getParameters() {
      return parameters;
    }

    /**
     * Sets the value of the parameters property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setParameters(String value) {
      if (value == null) {
        return;
      }
      this.setParameters(value.split(","));
    }

    public void setParameters(String[] params) {
      if (params == null) {
        return;
      }

      Arrays.stream(params).forEach(s -> {
        if (!s.isEmpty()) {
          String[] keyValuePair = s.split(PARAMS_DELIMITER);
          validateParam(keyValuePair, s);
          parameterMap.put(keyValuePair[0], keyValuePair[1]);
        }
      });
      this.parameters = Arrays.stream(params).collect(Collectors.joining(","));
    }

    public void setParameters(Map<String, String> parameterMap) {
      if (parameterMap == null) {
        return;
      }

      this.parameterMap = parameterMap;
      this.parameters = parameterMap.keySet().stream().map(k -> k + ":" + parameterMap.get(k))
          .collect(Collectors.joining(","));
    }

    public Map<String, String> getParameterMap() {
      if (this.parameters != null && !this.parameters.isEmpty()) {
        String[] params = this.parameters.split(",");
        Arrays.stream(params).forEach(s -> {
          String[] keyValuePair = s.split(PARAMS_DELIMITER);
          parameterMap.put(keyValuePair[0], keyValuePair[1]);
        });
      }
      return this.parameterMap;
    }

    private void validateParam(String[] paramKeyValue, String param) {
      // paramKeyValue is produced by split which will never give us
      // an empty second element
      if ((paramKeyValue.length != 2) || paramKeyValue[0].isEmpty()) {
        throw new IllegalArgumentException("Parameter '" + param
            + "' is not of the form 'parameterName" + PARAMS_DELIMITER + "value'");
      }
    }

    public Properties getConnectionProperties() {
      Properties properties = new Properties();
      if (parameterMap != null) {
        properties.putAll(parameterMap);
      }
      return properties;
    }

    @Override
    public String getId() {
      return getName();
    }
  }


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
   *       &lt;attribute name="region" type="{http://www.w3.org/2001/XMLSchema}string" />
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
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"fieldMapping"})
  public static class RegionMapping implements CacheElement {
    private static final String MAPPINGS_DELIMITER = ":";

    @XmlElement(name = "field-mapping", namespace = "http://geode.apache.org/schema/jdbc")
    protected List<ConnectorService.RegionMapping.FieldMapping> fieldMapping;
    @XmlTransient
    protected boolean fieldMappingModified = false;
    @XmlAttribute(name = "connection-name")
    protected String connectionConfigName;
    @XmlAttribute(name = "region")
    protected String regionName;
    @XmlAttribute(name = "table")
    protected String tableName;
    @XmlAttribute(name = "pdx-class")
    protected String pdxClassName;
    @XmlAttribute(name = "primary-key-in-value")
    protected Boolean primaryKeyInValue;


    public RegionMapping() {}

    public RegionMapping(String regionName, String pdxClassName, String tableName,
        String connectionConfigName, Boolean primaryKeyInValue) {
      this.regionName = regionName;
      this.pdxClassName = pdxClassName;
      this.tableName = tableName;
      this.connectionConfigName = connectionConfigName;
      this.primaryKeyInValue = primaryKeyInValue;
    }

    public RegionMapping(String regionName, String pdxClassName, String tableName,
        String connectionConfigName, Boolean primaryKeyInValue, List<FieldMapping> fieldMappings) {
      this.regionName = regionName;
      this.pdxClassName = pdxClassName;
      this.tableName = tableName;
      this.connectionConfigName = connectionConfigName;
      this.primaryKeyInValue = primaryKeyInValue;
      this.fieldMapping = fieldMappings;
      if (fieldMappings != null) {
        fieldMappingModified = true;
      }
    }

    @TestingOnly
    public RegionMapping(String regionName, String pdxClassName, String tableName,
        String connectionConfigName, Boolean primaryKeyInValue,
        Map<String, String> configuredFieldToColumnMap) {
      this.regionName = regionName;
      this.pdxClassName = pdxClassName;
      this.tableName = tableName;
      this.connectionConfigName = connectionConfigName;
      this.primaryKeyInValue = primaryKeyInValue;
      if (configuredFieldToColumnMap != null) {
        this.fieldMapping = configuredFieldToColumnMap.keySet().stream()
            .map(key -> new FieldMapping(key, configuredFieldToColumnMap.get(key)))
            .collect(Collectors.toList());
        this.fieldMappingModified = true;
      }
    }

    public void setFieldMapping(String[] mappings) {
      if (mappings != null) {
        this.fieldMapping =
            Arrays.stream(mappings).filter(Objects::nonNull).filter(s -> !s.isEmpty()).map(s -> {
              String[] keyValuePair = s.split(MAPPINGS_DELIMITER);
              validateParam(keyValuePair, s);
              return new ConnectorService.RegionMapping.FieldMapping(keyValuePair[0],
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

    public List<ConnectorService.RegionMapping.FieldMapping> getFieldMapping() {
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
      return getRegionName();
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
       * @return
       *         possible object is
       *         {@link String }
       *
       */
      public String getFieldName() {
        return fieldName;
      }

      /**
       * Sets the value of the fieldName property.
       *
       * @param value
       *        allowed object is
       *        {@link String }
       *
       */
      public void setFieldName(String value) {
        this.fieldName = value;
      }

      /**
       * Gets the value of the columnName property.
       *
       * @return
       *         possible object is
       *         {@link String }
       *
       */
      public String getColumnName() {
        return columnName;
      }

      /**
       * Sets the value of the columnName property.
       *
       * @param value
       *        allowed object is
       *        {@link String }
       *
       */
      public void setColumnName(String value) {
        this.columnName = value;
      }

    }
  }
}
