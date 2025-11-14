
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

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.pdx.PdxSerializer;


/**
 *
 * A "pdx" element specifies the configuration for the portable data exchange (PDX) method of
 * serialization. The "read-serialized" attribute is "early access".
 *
 *
 * <p>
 * Java class for pdx-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="pdx-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="pdx-serializer" minOccurs="0"&gt;
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
 *       &lt;attribute name="read-serialized" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="ignore-unread-fields" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="persistent" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "pdx-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"pdxSerializer"})
@Experimental
public class PdxType {
  @XmlElement(name = "pdx-serializer", namespace = "http://geode.apache.org/schema/cache")
  protected DeclarableType pdxSerializer;
  @XmlAttribute(name = "read-serialized")
  protected Boolean readSerialized;
  @XmlAttribute(name = "ignore-unread-fields")
  protected Boolean ignoreUnreadFields;
  @XmlAttribute(name = "persistent")
  protected Boolean persistent;
  @XmlAttribute(name = "disk-store-name")
  protected String diskStoreName;

  /**
   * Gets the value of the pdxSerializer property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   * @return the type of the {@link PdxSerializer}.
   */
  public DeclarableType getPdxSerializer() {
    return pdxSerializer;
  }

  /**
   * Sets the value of the pdxSerializer property.
   *
   * allowed object is
   * {@link DeclarableType }
   *
   * @param value the type of {@link PdxSerializer} to use.
   */
  public void setPdxSerializer(DeclarableType value) {
    pdxSerializer = value;
  }

  /**
   * Gets the value of the readSerialized property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return true if read serialized is enabled, false otherwise.
   */
  public Boolean isReadSerialized() {
    return readSerialized;
  }

  /**
   * Sets the value of the readSerialized property.
   *
   * allowed object is
   * {@link Boolean }
   *
   * @param value enables or disables read serialized.
   */
  public void setReadSerialized(Boolean value) {
    readSerialized = value;
  }

  /**
   * Gets the value of the ignoreUnreadFields property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return true if unread fields are ignored, false otherwise.
   */
  public Boolean isIgnoreUnreadFields() {
    return ignoreUnreadFields;
  }

  /**
   * Sets the value of the ignoreUnreadFields property.
   *
   * allowed object is
   * {@link Boolean }
   *
   * @param value determine whether to ignore unread fields.
   */
  public void setIgnoreUnreadFields(Boolean value) {
    ignoreUnreadFields = value;
  }

  /**
   * Gets the value of the persistent property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return true if persistence is enabled, false otherwise.
   */
  public Boolean isPersistent() {
    return persistent;
  }

  /**
   * Sets the value of the persistent property.
   *
   * allowed object is
   * {@link Boolean }
   *
   * @param value enables or disables persistence.
   */
  public void setPersistent(Boolean value) {
    persistent = value;
  }

  /**
   * Gets the value of the diskStoreName property.
   *
   * possible object is
   * {@link String }
   *
   * @return the disk store name.
   */
  public String getDiskStoreName() {
    return diskStoreName;
  }

  /**
   * Sets the value of the diskStoreName property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the disk store name.
   */
  public void setDiskStoreName(String value) {
    diskStoreName = value;
  }
}
