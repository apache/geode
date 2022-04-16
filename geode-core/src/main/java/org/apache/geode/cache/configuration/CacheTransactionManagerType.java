
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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.TransactionWriter;


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
 * &lt;complexType name="cache-transaction-manager-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="transaction-listener" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="transaction-writer" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                 &lt;/sequence&gt;
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
@XmlType(name = "cache-transaction-manager-type",
    namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"transactionListeners", "transactionWriter"})
@Experimental
public class CacheTransactionManagerType {

  @XmlElement(name = "transaction-listener", namespace = "http://geode.apache.org/schema/cache")
  protected List<DeclarableType> transactionListeners;
  @XmlElement(name = "transaction-writer", namespace = "http://geode.apache.org/schema/cache")
  protected DeclarableType transactionWriter;

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
   * {@link DeclarableType }
   *
   * @return the {@link List} of {@link TransactionListener} types.
   */
  public List<DeclarableType> getTransactionListeners() {
    if (transactionListeners == null) {
      transactionListeners = new ArrayList<>();
    }
    return transactionListeners;
  }

  /**
   * Gets the value of the transactionWriter property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   * @return the {@link TransactionWriter} type.
   */
  public DeclarableType getTransactionWriter() {
    return transactionWriter;
  }

  /**
   * Sets the value of the transactionWriter property.
   *
   * allowed object is
   * {@link DeclarableType }
   *
   * @param value the {@link TransactionWriter} type.
   */
  public void setTransactionWriter(DeclarableType value) {
    transactionWriter = value;
  }

}
