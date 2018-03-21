
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.cache.configuration;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


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
 * &lt;complexType name="pdx-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="pdx-serializer" minOccurs="0">
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
 *       &lt;attribute name="read-serialized" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="ignore-unread-fields" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="persistent" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
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
  protected PdxType.PdxSerializer pdxSerializer;
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
   * @return
   *         possible object is
   *         {@link PdxType.PdxSerializer }
   *
   */
  public PdxType.PdxSerializer getPdxSerializer() {
    return pdxSerializer;
  }

  /**
   * Sets the value of the pdxSerializer property.
   *
   * @param value
   *        allowed object is
   *        {@link PdxType.PdxSerializer }
   *
   */
  public void setPdxSerializer(PdxType.PdxSerializer value) {
    this.pdxSerializer = value;
  }

  /**
   * Gets the value of the readSerialized property.
   *
   * @return
   *         possible object is
   *         {@link Boolean }
   *
   */
  public Boolean isReadSerialized() {
    return readSerialized;
  }

  /**
   * Sets the value of the readSerialized property.
   *
   * @param value
   *        allowed object is
   *        {@link Boolean }
   *
   */
  public void setReadSerialized(Boolean value) {
    this.readSerialized = value;
  }

  /**
   * Gets the value of the ignoreUnreadFields property.
   *
   * @return
   *         possible object is
   *         {@link Boolean }
   *
   */
  public Boolean isIgnoreUnreadFields() {
    return ignoreUnreadFields;
  }

  /**
   * Sets the value of the ignoreUnreadFields property.
   *
   * @param value
   *        allowed object is
   *        {@link Boolean }
   *
   */
  public void setIgnoreUnreadFields(Boolean value) {
    this.ignoreUnreadFields = value;
  }

  /**
   * Gets the value of the persistent property.
   *
   * @return
   *         possible object is
   *         {@link Boolean }
   *
   */
  public Boolean isPersistent() {
    return persistent;
  }

  /**
   * Sets the value of the persistent property.
   *
   * @param value
   *        allowed object is
   *        {@link Boolean }
   *
   */
  public void setPersistent(Boolean value) {
    this.persistent = value;
  }

  /**
   * Gets the value of the diskStoreName property.
   *
   * @return
   *         possible object is
   *         {@link String }
   *
   */
  public String getDiskStoreName() {
    return diskStoreName;
  }

  /**
   * Sets the value of the diskStoreName property.
   *
   * @param value
   *        allowed object is
   *        {@link String }
   *
   */
  public void setDiskStoreName(String value) {
    this.diskStoreName = value;
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
  @XmlType(name = "", propOrder = {"className", "parameter"})
  public static class PdxSerializer {

    @XmlElement(name = "class-name", namespace = "http://geode.apache.org/schema/cache",
        required = true)
    protected String className;
    @XmlElement(namespace = "http://geode.apache.org/schema/cache")
    protected List<ParameterType> parameter;

    /**
     * Gets the value of the className property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getClassName() {
      return className;
    }

    /**
     * Sets the value of the className property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setClassName(String value) {
      this.className = value;
    }

    /**
     * Gets the value of the parameter property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the parameter property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getParameter().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ParameterType }
     *
     *
     */
    public List<ParameterType> getParameter() {
      if (parameter == null) {
        parameter = new ArrayList<ParameterType>();
      }
      return this.parameter;
    }

  }

}
