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
 * The "function service" element configures the behavior of the function service.
 * The function service allows users to execute functions on data stored in Geode.
 *
 *
 * <p>
 * Java class for function-service-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="function-service-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="function" maxOccurs="unbounded" minOccurs="0">
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
@XmlType(name = "function-service-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"functions"})
@Experimental
public class FunctionServiceType implements Serializable {

  private static final long serialVersionUID = 1L;
  @XmlElement(name = "function", namespace = "http://geode.apache.org/schema/cache")
  protected List<FunctionServiceType.Function> functions;

  /**
   * Gets the value of the functions property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the functions property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getFunctions().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link FunctionServiceType.Function }
   *
   *
   */
  public List<FunctionServiceType.Function> getFunctions() {
    if (functions == null) {
      functions = new ArrayList<FunctionServiceType.Function>();
    }
    return this.functions;
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
  public static class Function implements Serializable {

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
