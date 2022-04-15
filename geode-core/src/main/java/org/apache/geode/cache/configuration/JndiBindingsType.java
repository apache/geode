
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

package org.apache.geode.cache.configuration;

import java.util.ArrayList;
import java.util.List;

import javax.resource.spi.ManagedConnectionFactory;
import javax.sql.ConnectionPoolDataSource;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * A jndi-bindings element will contain the jndi-binding for each of the
 * datasources which are to be bound with the JNDI Context.
 *
 *
 * <p>
 * Java class for jndi-bindings-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="jndi-bindings-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="jndi-binding" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="config-property" maxOccurs="unbounded" minOccurs="0"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;sequence&gt;
 *                             &lt;element name="config-property-name" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *                             &lt;element name="config-property-type" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *                             &lt;element name="config-property-value" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *                           &lt;/sequence&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                 &lt;/sequence&gt;
 *                 &lt;attribute name="blocking-timeout-seconds" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="conn-pooled-datasource-class" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="connection-url" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="idle-timeout-seconds" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="init-pool-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="jdbc-driver-class" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="jndi-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="login-timeout-seconds" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="managed-conn-factory-class" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="max-pool-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="password" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="transaction-type" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="type" use="required"&gt;
 *                   &lt;simpleType&gt;
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *                       &lt;enumeration value="ManagedDataSource"/&gt;
 *                       &lt;enumeration value="SimpleDataSource"/&gt;
 *                       &lt;enumeration value="PooledDataSource"/&gt;
 *                       &lt;enumeration value="XAPooledDataSource"/&gt;
 *                     &lt;/restriction&gt;
 *                   &lt;/simpleType&gt;
 *                 &lt;/attribute&gt;
 *                 &lt;attribute name="user-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="xa-datasource-class" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *       &lt;/sequence&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "jndi-bindings-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"jndiBindings"})
@Experimental
public class JndiBindingsType {

  @XmlElement(name = "jndi-binding", namespace = "http://geode.apache.org/schema/cache")
  protected List<JndiBinding> jndiBindings;

  /**
   * Gets the value of the jndiBindings property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the jndiBindings property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getJndiBindings().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link JndiBindingsType.JndiBinding }
   *
   * @return the {@link List} of {@link JndiBinding}s.
   */
  public List<JndiBinding> getJndiBindings() {
    if (jndiBindings == null) {
      jndiBindings = new ArrayList<>();
    }
    return jndiBindings;
  }


  /**
   * <p>
   * Java class for anonymous complex type.
   *
   * <p>
   * The following schema fragment specifies the expected content contained within this class.
   *
   * <pre>
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;sequence&gt;
   *         &lt;element name="config-property" maxOccurs="unbounded" minOccurs="0"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;sequence&gt;
   *                   &lt;element name="config-property-name" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
   *                   &lt;element name="config-property-type" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
   *                   &lt;element name="config-property-value" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
   *                 &lt;/sequence&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *       &lt;/sequence&gt;
   *       &lt;attribute name="blocking-timeout-seconds" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="conn-pooled-datasource-class" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="connection-url" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="idle-timeout-seconds" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="init-pool-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="jdbc-driver-class" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="jndi-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="login-timeout-seconds" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="managed-conn-factory-class" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="max-pool-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="password" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="transaction-type" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="type" use="required"&gt;
   *         &lt;simpleType&gt;
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
   *             &lt;enumeration value="ManagedDataSource"/&gt;
   *             &lt;enumeration value="SimpleDataSource"/&gt;
   *             &lt;enumeration value="PooledDataSource"/&gt;
   *             &lt;enumeration value="XAPooledDataSource"/&gt;
   *           &lt;/restriction&gt;
   *         &lt;/simpleType&gt;
   *       &lt;/attribute&gt;
   *       &lt;attribute name="user-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="xa-datasource-class" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"configProperties"})
  @Experimental
  public static class JndiBinding extends CacheElement {

    @XmlElement(name = "config-property", namespace = "http://geode.apache.org/schema/cache")
    protected List<ConfigProperty> configProperties;
    @XmlAttribute(name = "blocking-timeout-seconds")
    protected String blockingTimeoutSeconds;
    @XmlAttribute(name = "conn-pooled-datasource-class")
    protected String connPooledDatasourceClass;
    @XmlAttribute(name = "connection-url")
    protected String connectionUrl;
    @XmlAttribute(name = "idle-timeout-seconds")
    protected String idleTimeoutSeconds;
    @XmlAttribute(name = "init-pool-size")
    protected String initPoolSize;
    @XmlAttribute(name = "jdbc-driver-class")
    protected String jdbcDriverClass;
    @XmlAttribute(name = "jndi-name", required = true)
    protected String jndiName;
    @XmlAttribute(name = "login-timeout-seconds")
    protected String loginTimeoutSeconds;
    @XmlAttribute(name = "managed-conn-factory-class")
    protected String managedConnFactoryClass;
    @XmlAttribute(name = "max-pool-size")
    protected String maxPoolSize;
    @XmlAttribute(name = "password")
    protected String password;
    @XmlAttribute(name = "transaction-type")
    protected String transactionType;
    @XmlAttribute(name = "type", required = true)
    protected String type;
    @XmlAttribute(name = "user-name")
    protected String userName;
    @XmlAttribute(name = "xa-datasource-class")
    protected String xaDatasourceClass;

    /**
     * Gets the value of the configProperties property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the configProperties property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getConfigProperties().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link JndiBindingsType.JndiBinding.ConfigProperty }
     *
     * @return the {@link List} of configuration properties.
     */
    public List<ConfigProperty> getConfigProperties() {
      if (configProperties == null) {
        configProperties = new ArrayList<>();
      }
      return configProperties;
    }

    /**
     * Gets the value of the blockingTimeoutSeconds property.
     *
     * possible object is
     * {@link String }
     *
     * @return the blocking timeout in seconds.
     */
    public String getBlockingTimeoutSeconds() {
      return blockingTimeoutSeconds;
    }

    /**
     * Sets the value of the blockingTimeoutSeconds property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the blocking timeout in seconds.
     */
    public void setBlockingTimeoutSeconds(String value) {
      blockingTimeoutSeconds = value;
    }

    /**
     * Gets the value of the connPooledDatasourceClass property.
     *
     * possible object is
     * {@link String }
     *
     * @return the {@link ConnectionPoolDataSource} type.
     */
    public String getConnPooledDatasourceClass() {
      return connPooledDatasourceClass;
    }

    /**
     * Sets the value of the connPooledDatasourceClass property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value {@link ConnectionPoolDataSource} type.
     */
    public void setConnPooledDatasourceClass(String value) {
      connPooledDatasourceClass = value;
    }

    /**
     * Gets the value of the connectionUrl property.
     *
     * possible object is
     * {@link String }
     *
     * @return the connection URL.
     */
    public String getConnectionUrl() {
      return connectionUrl;
    }

    /**
     * Sets the value of the connectionUrl property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the connection URL.
     */
    public void setConnectionUrl(String value) {
      connectionUrl = value;
    }

    /**
     * Gets the value of the idleTimeoutSeconds property.
     *
     * possible object is
     * {@link String }
     *
     * @return the idle timeout in seconds.
     */
    public String getIdleTimeoutSeconds() {
      return idleTimeoutSeconds;
    }

    /**
     * Sets the value of the idleTimeoutSeconds property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the idle timeout in seconds.
     */
    public void setIdleTimeoutSeconds(String value) {
      idleTimeoutSeconds = value;
    }

    /**
     * Gets the value of the initPoolSize property.
     *
     * possible object is
     * {@link String }
     *
     * @return the initial pool size.
     */
    public String getInitPoolSize() {
      return initPoolSize;
    }

    /**
     * Sets the value of the initPoolSize property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the initial pool size.
     */
    public void setInitPoolSize(String value) {
      initPoolSize = value;
    }

    /**
     * Gets the value of the jdbcDriverClass property.
     *
     * possible object is
     * {@link String }
     *
     * @return the JDBC driver class.
     */
    public String getJdbcDriverClass() {
      return jdbcDriverClass;
    }

    /**
     * Sets the value of the jdbcDriverClass property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the JDBC driver class.
     */
    public void setJdbcDriverClass(String value) {
      jdbcDriverClass = value;
    }

    /**
     * Gets the value of the jndiName property.
     *
     * possible object is
     * {@link String }
     *
     * @return the JNDI name.
     */
    public String getJndiName() {
      return jndiName;
    }

    /**
     * Sets the value of the jndiName property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the JNDI name.
     */
    public void setJndiName(String value) {
      jndiName = value;
    }

    /**
     * Gets the value of the loginTimeoutSeconds property.
     *
     * possible object is
     * {@link String }
     *
     * @return the login timeout in seconds.
     */
    public String getLoginTimeoutSeconds() {
      return loginTimeoutSeconds;
    }

    /**
     * Sets the value of the loginTimeoutSeconds property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the login timeout in seconds.
     */
    public void setLoginTimeoutSeconds(String value) {
      loginTimeoutSeconds = value;
    }

    /**
     * Gets the value of the managedConnFactoryClass property.
     *
     * possible object is
     * {@link String }
     *
     * @return the name of the {@link ManagedConnectionFactory} type.
     */
    public String getManagedConnFactoryClass() {
      return managedConnFactoryClass;
    }

    /**
     * Sets the value of the managedConnFactoryClass property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the {@link ManagedConnectionFactory} class name.
     */
    public void setManagedConnFactoryClass(String value) {
      managedConnFactoryClass = value;
    }

    /**
     * Gets the value of the maxPoolSize property.
     *
     * possible object is
     * {@link String }
     *
     * @return the maximum pool size.
     */
    public String getMaxPoolSize() {
      return maxPoolSize;
    }

    /**
     * Sets the value of the maxPoolSize property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the maximum pool size.
     */
    public void setMaxPoolSize(String value) {
      maxPoolSize = value;
    }

    /**
     * Gets the value of the password property.
     *
     * possible object is
     * {@link String }
     *
     * @return the password.
     */
    public String getPassword() {
      return password;
    }

    /**
     * Sets the value of the password property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the password.
     */
    public void setPassword(String value) {
      password = value;
    }

    /**
     * Gets the value of the transactionType property.
     *
     * possible object is
     * {@link String }
     *
     * @return the transaction type.
     */
    public String getTransactionType() {
      return transactionType;
    }

    /**
     * Sets the value of the transactionType property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the transaction type.
     */
    public void setTransactionType(String value) {
      transactionType = value;
    }

    /**
     * Gets the value of the type property.
     *
     * possible object is
     * {@link String }
     *
     * @return the type.
     */
    public String getType() {
      return type;
    }

    /**
     * Sets the value of the type property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the type.
     */
    public void setType(String value) {
      type = value;
    }

    /**
     * Gets the value of the userName property.
     *
     * possible object is
     * {@link String }
     *
     * @return the username.
     */
    public String getUserName() {
      return userName;
    }

    /**
     * Sets the value of the userName property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the username.
     */
    public void setUserName(String value) {
      userName = value;
    }

    /**
     * Gets the value of the xaDatasourceClass property.
     *
     * possible object is
     * {@link String }
     *
     * @return the XA datasource class.
     */
    public String getXaDatasourceClass() {
      return xaDatasourceClass;
    }

    /**
     * Sets the value of the xaDatasourceClass property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the XA datasource class.
     */
    public void setXaDatasourceClass(String value) {
      xaDatasourceClass = value;
    }

    @Override
    public String getId() {
      return getJndiName();
    }

    /**
     * <p>
     * Java class for anonymous complex type.
     *
     * <p>
     * The following schema fragment specifies the expected content contained within this class.
     *
     * <pre>
     * &lt;complexType&gt;
     *   &lt;complexContent&gt;
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
     *       &lt;sequence&gt;
     *         &lt;element name="config-property-name" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
     *         &lt;element name="config-property-type" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
     *         &lt;element name="config-property-value" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
     *       &lt;/sequence&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "",
        propOrder = {"configPropertyName", "configPropertyType", "configPropertyValue"})
    public static class ConfigProperty extends CacheElement {

      @XmlElement(name = "config-property-name", namespace = "http://geode.apache.org/schema/cache",
          required = true)
      protected String configPropertyName;
      @XmlElement(name = "config-property-type", namespace = "http://geode.apache.org/schema/cache",
          required = true)
      protected String configPropertyType;
      @XmlElement(name = "config-property-value",
          namespace = "http://geode.apache.org/schema/cache", required = true)
      protected String configPropertyValue;

      public ConfigProperty() {}

      public ConfigProperty(String name, String type, String value) {
        configPropertyName = name;
        configPropertyType = type;
        configPropertyValue = value;
      }

      public ConfigProperty(String name, String value) {
        configPropertyName = name;
        configPropertyValue = value;
      }

      /**
       * Get the id of the element. The id is the same as the name.
       *
       * @return the id of the element
       */
      @Override
      public String getId() {
        return getName();
      }

      /**
       * Gets the value of the configPropertyName property.
       *
       * possible object is
       * {@link String }
       *
       * @return the name.
       */
      public String getName() {
        return configPropertyName;
      }

      /**
       * Sets the value of the configPropertyName property.
       *
       * allowed object is
       * {@link String }
       *
       * @param value the name.
       */
      public void setName(String value) {
        configPropertyName = value;
      }

      /**
       * Gets the value of the configPropertyType property.
       *
       * possible object is
       * {@link String }
       *
       * @return the type.
       */
      public String getType() {
        return configPropertyType;
      }

      /**
       * Sets the value of the configPropertyType property.
       *
       * allowed object is
       * {@link String }
       *
       * @param value the type.
       */
      public void setType(String value) {
        configPropertyType = value;
      }

      /**
       * Gets the value of the configPropertyValue property.
       *
       * possible object is
       * {@link String }
       *
       * @return the value.
       */
      public String getValue() {
        return configPropertyValue;
      }

      /**
       * Sets the value of the configPropertyValue property.
       *
       * allowed object is
       * {@link String }
       *
       * @param value the value.
       */
      public void setValue(String value) {
        configPropertyValue = value;
      }

    }

  }

}
