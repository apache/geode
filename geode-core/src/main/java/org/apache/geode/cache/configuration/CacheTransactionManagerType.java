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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * A "cache-transaction-manager" element allows insertion of cache-level transaction listeners.
 *
 *
 * <p>
 * Java class for cache-transaction-manager-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="cache-transaction-manager-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="transaction-listener" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
 *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="transaction-writer" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
 *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "cache-transaction-manager-type",
    namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"transactionListeners", "transactionWriter"})
@Experimental
public class CacheTransactionManagerType implements Serializable {

  private static final long serialVersionUID = 1L;
  @XmlElement(name = "transaction-listener", namespace = "http://geode.apache.org/schema/cache")
  protected List<CacheTransactionManagerType.TransactionListener> transactionListeners;
  @XmlElement(name = "transaction-writer", namespace = "http://geode.apache.org/schema/cache")
  protected CacheTransactionManagerType.TransactionWriter transactionWriter;

  /**
   * Gets the value of the transactionListeners property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the transactionListeners property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getTransactionListeners().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link CacheTransactionManagerType.TransactionListener }
   *
   *
   */
  public List<CacheTransactionManagerType.TransactionListener> getTransactionListeners() {
    if (transactionListeners == null) {
      transactionListeners = new ArrayList<CacheTransactionManagerType.TransactionListener>();
    }
    return this.transactionListeners;
  }

  /**
   * Gets the value of the transactionWriter property.
   *
   * possible object is
   * {@link CacheTransactionManagerType.TransactionWriter }
   *
   */
  public CacheTransactionManagerType.TransactionWriter getTransactionWriter() {
    return transactionWriter;
  }

  /**
   * Sets the value of the transactionWriter property.
   *
   * allowed object is
   * {@link CacheTransactionManagerType.TransactionWriter }
   *
   */
  public void setTransactionWriter(CacheTransactionManagerType.TransactionWriter value) {
    this.transactionWriter = value;
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
   *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
   *         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
   *       &lt;/sequence>
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"className", "parameters"})
  public static class TransactionListener implements Serializable {

    private static final long serialVersionUID = 1L;
    @XmlElement(name = "class-name", namespace = "http://geode.apache.org/schema/cache",
        required = true)
    protected String className;
    @XmlElement(name = "parameter", namespace = "http://geode.apache.org/schema/cache")
    protected List<ParameterType> parameters;

    /**
     * Gets the value of the className property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getClassName() {
      return className;
    }

    /**
     * Sets the value of the className property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setClassName(String value) {
      this.className = value;
    }

    /**
     * Gets the value of the parameters property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the parameters property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getParameters().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ParameterType }
     *
     *
     */
    public List<ParameterType> getParameters() {
      if (parameters == null) {
        parameters = new ArrayList<ParameterType>();
      }
      return this.parameters;
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
   *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
   *         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
   *       &lt;/sequence>
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"className", "parameters"})
  public static class TransactionWriter implements Serializable {

    private static final long serialVersionUID = 1L;
    @XmlElement(name = "class-name", namespace = "http://geode.apache.org/schema/cache",
        required = true)
    protected String className;
    @XmlElement(name = "parameter", namespace = "http://geode.apache.org/schema/cache")
    protected List<ParameterType> parameters;

    /**
     * Gets the value of the className property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getClassName() {
      return className;
    }

    /**
     * Sets the value of the className property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setClassName(String value) {
      this.className = value;
    }

    /**
     * Gets the value of the parameters property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the parameters property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getParameters().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ParameterType }
     *
     *
     */
    public List<ParameterType> getParameters() {
      if (parameters == null) {
        parameters = new ArrayList<ParameterType>();
      }
      return this.parameters;
    }

  }

}
