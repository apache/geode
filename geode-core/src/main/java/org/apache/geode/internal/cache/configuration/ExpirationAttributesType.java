
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

package org.apache.geode.internal.cache.configuration;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 *
 * An "expiration-attributes" element describes expiration.
 *
 *
 * <p>
 * Java class for expiration-attributes-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="expiration-attributes-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="custom-expiry" minOccurs="0">
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
 *       &lt;attribute name="action">
 *         &lt;simpleType>
 *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *             &lt;enumeration value="destroy"/>
 *             &lt;enumeration value="invalidate"/>
 *             &lt;enumeration value="local-destroy"/>
 *             &lt;enumeration value="local-invalidate"/>
 *           &lt;/restriction>
 *         &lt;/simpleType>
 *       &lt;/attribute>
 *       &lt;attribute name="timeout" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "expiration-attributes-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"customExpiry"})
public class ExpirationAttributesType {

  @XmlElement(name = "custom-expiry", namespace = "http://geode.apache.org/schema/cache")
  protected ExpirationAttributesType.CustomExpiry customExpiry;
  @XmlAttribute(name = "action")
  protected String action;
  @XmlAttribute(name = "timeout", required = true)
  protected String timeout;

  /**
   * Gets the value of the customExpiry property.
   *
   * @return
   *         possible object is
   *         {@link ExpirationAttributesType.CustomExpiry }
   *
   */
  public ExpirationAttributesType.CustomExpiry getCustomExpiry() {
    return customExpiry;
  }

  /**
   * Sets the value of the customExpiry property.
   *
   * @param value
   *        allowed object is
   *        {@link ExpirationAttributesType.CustomExpiry }
   *
   */
  public void setCustomExpiry(ExpirationAttributesType.CustomExpiry value) {
    this.customExpiry = value;
  }

  /**
   * Gets the value of the action property.
   *
   * @return
   *         possible object is
   *         {@link String }
   *
   */
  public String getAction() {
    return action;
  }

  /**
   * Sets the value of the action property.
   *
   * @param value
   *        allowed object is
   *        {@link String }
   *
   */
  public void setAction(String value) {
    this.action = value;
  }

  /**
   * Gets the value of the timeout property.
   *
   * @return
   *         possible object is
   *         {@link String }
   *
   */
  public String getTimeout() {
    return timeout;
  }

  /**
   * Sets the value of the timeout property.
   *
   * @param value
   *        allowed object is
   *        {@link String }
   *
   */
  public void setTimeout(String value) {
    this.timeout = value;
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
  public static class CustomExpiry {

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
