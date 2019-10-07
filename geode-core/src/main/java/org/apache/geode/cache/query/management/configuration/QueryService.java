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
package org.apache.geode.cache.query.management.configuration;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.XSDRootElement;


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
 *       &lt;all>
 *         &lt;element name="method-authorizer" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="parameter" maxOccurs="unbounded" minOccurs="0">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;attribute name="parameterValue" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                 &lt;/sequence>
 *                 &lt;attribute name="class-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/all>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {

})
@XmlRootElement(name = "query-service", namespace = "http://geode.apache.org/schema/query-service")
@XSDRootElement(namespace = "http://geode.apache.org/schema/query-service",
    schemaLocation = "http://geode.apache.org/schema/query-service/query-service-1.0.xsd")
public class QueryService extends CacheElement {

  @XmlElement(name = "method-authorizer",
      namespace = "http://geode.apache.org/schema/query-service")
  protected QueryService.MethodAuthorizer methodAuthorizer;

  public static final String ELEMENT_ID = "query-service";

  /**
   * Gets the value of the methodAuthorizer property.
   *
   * possible object is
   * {@link QueryService.MethodAuthorizer }
   *
   */
  public QueryService.MethodAuthorizer getMethodAuthorizer() {
    return methodAuthorizer;
  }

  /**
   * Sets the value of the methodAuthorizer property.
   *
   * allowed object is
   * {@link QueryService.MethodAuthorizer }
   *
   */
  public void setMethodAuthorizer(QueryService.MethodAuthorizer value) {
    this.methodAuthorizer = value;
  }

  @Override
  public String getId() {
    return ELEMENT_ID;
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
   *         &lt;element name="parameter" maxOccurs="unbounded" minOccurs="0">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;attribute name="parameterValue" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *       &lt;/sequence>
   *       &lt;attribute name="class-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {
      "parameter"
  })
  public static class MethodAuthorizer {

    @XmlElement(namespace = "http://geode.apache.org/schema/query-service")
    protected List<QueryService.MethodAuthorizer.Parameter> parameter;
    @XmlAttribute(name = "class-name", required = true)
    protected String className;

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
     * {@link QueryService.MethodAuthorizer.Parameter }
     *
     *
     */
    public List<QueryService.MethodAuthorizer.Parameter> getParameter() {
      if (parameter == null) {
        parameter = new ArrayList<QueryService.MethodAuthorizer.Parameter>();
      }
      return this.parameter;
    }

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
     *       &lt;attribute name="parameterValue" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "")
    public static class Parameter {

      @XmlAttribute(name = "parameterValue", required = true)
      protected String parameterValue;

      /**
       * Gets the value of the parameterValue property.
       *
       * possible object is
       * {@link String }
       *
       */
      public String getParameterValue() {
        return parameterValue;
      }

      /**
       * Sets the value of the parameterValue property.
       *
       * allowed object is
       * {@link String }
       *
       */
      public void setParameterValue(String value) {
        this.parameterValue = value;
      }

    }

  }

}
