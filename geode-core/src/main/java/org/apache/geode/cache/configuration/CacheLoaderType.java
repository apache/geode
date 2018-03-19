
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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * A "cache-loader" element describes a region's CacheLoader.
 *
 *
 * <p>
 * Java class for cache-loader-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="cache-loader-type">
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
@XmlType(name = "cache-loader-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"className", "parameter"})
@Experimental
public class CacheLoaderType {

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
