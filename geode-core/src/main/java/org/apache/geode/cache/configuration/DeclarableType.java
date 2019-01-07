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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.internal.cli.domain.ClassName;

/**
 *
 * A "declarable" element specifies a Declarable object to be placed in a Region entry.
 *
 *
 * <p>
 * Java class for declarable-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="declarable-type">
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
@XmlType(name = "declarable-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"parameters"})
@Experimental
public class DeclarableType extends ClassNameType implements Serializable {
  @XmlElement(name = "parameter", namespace = "http://geode.apache.org/schema/cache")
  protected List<ParameterType> parameters;

  public DeclarableType() {}

  public DeclarableType(String className) {
    this.className = className;
  }

  public DeclarableType(String className, String jsonProperties) {
    this(className, new ClassName(className, jsonProperties).getInitProperties());
  }

  public DeclarableType(String className, Properties properties) {
    this.className = className;
    if (properties != null) {
      parameters = properties.stringPropertyNames().stream()
          .map(k -> new ParameterType(k, properties.getProperty(k))).collect(Collectors.toList());
    }
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeclarableType that = (DeclarableType) o;
    return Objects.equals(className, that.className) && Objects.equals(parameters, that.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, parameters);
  }

  @Override
  public String toString() {
    if (parameters == null) {
      return className;
    }

    return className + "{"
        + parameters.stream().map(Objects::toString).collect(Collectors.joining(",")) + "}";
  }
}
