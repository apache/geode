
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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 *
 * A "parameter" element describes a parameter used to initialize a Declarable object.
 *
 *
 * <p>
 * Java class for parameter-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="parameter-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;choice>
 *         &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/>
 *         &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/>
 *       &lt;/choice>
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "parameter-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"string", "declarable"})
public class ParameterType {

  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected StringType string;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected DeclarableType declarable;
  @XmlAttribute(name = "name", required = true)
  protected String name;

  /**
   * Gets the value of the string property.
   *
   * @return
   *         possible object is
   *         {@link StringType }
   *
   */
  public StringType getString() {
    return string;
  }

  /**
   * Sets the value of the string property.
   *
   * @param value
   *        allowed object is
   *        {@link StringType }
   *
   */
  public void setString(StringType value) {
    this.string = value;
  }

  /**
   * Gets the value of the declarable property.
   *
   * @return
   *         possible object is
   *         {@link DeclarableType }
   *
   */
  public DeclarableType getDeclarable() {
    return declarable;
  }

  /**
   * Sets the value of the declarable property.
   *
   * @param value
   *        allowed object is
   *        {@link DeclarableType }
   *
   */
  public void setDeclarable(DeclarableType value) {
    this.declarable = value;
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

}
