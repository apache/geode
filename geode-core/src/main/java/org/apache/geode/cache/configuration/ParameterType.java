
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

import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


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
 * This represents a key/value pair, name is the key, another string or object is the value
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "parameter-type", namespace = "http://geode.apache.org/schema/cache")
@Experimental
public class ParameterType extends ObjectType {

  @XmlAttribute(name = "name", required = true)
  protected String name;

  public ParameterType() {};

  public ParameterType(String name) {
    this.name = name;
  }

  public ParameterType(String name, String value) {
    this.name = name;
    setString(value);
  }

  /**
   * Gets the value of the name property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the value of the name property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setName(String value) {
    this.name = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ParameterType that = (ParameterType) o;
    return Objects.equals(name, that.name) && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return name + ":" + super.toString();
  }

}
