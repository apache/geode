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

import java.io.Serializable;
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
 *                           &lt;attribute name="parameter-value" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
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
    "methodAuthorizer"
})
@XmlRootElement(name = "query-config-service",
    namespace = "http://geode.apache.org/schema/query-config-service")
@XSDRootElement(namespace = "http://geode.apache.org/schema/query-config-service",
    schemaLocation = "http://geode.apache.org/schema/query-config-service/query-config-service-1.0.xsd")
public class QueryConfigService extends CacheElement {

  private static final long serialVersionUID = -6702354810758904467L;

  @XmlElement(name = "method-authorizer",
      namespace = "http://geode.apache.org/schema/query-config-service")
  protected QueryConfigService.MethodAuthorizer methodAuthorizer;

  public static final String ELEMENT_ID = "query-config-service";

  /**
   * Gets the value of the methodAuthorizer property.
   *
   * possible object is
   * {@link QueryConfigService.MethodAuthorizer }
   *
   */
  public QueryConfigService.MethodAuthorizer getMethodAuthorizer() {
    return methodAuthorizer;
  }

  /**
   * Sets the value of the methodAuthorizer property.
   *
   * allowed object is
   * {@link QueryConfigService.MethodAuthorizer }
   *
   */
  public void setMethodAuthorizer(QueryConfigService.MethodAuthorizer value) {
    methodAuthorizer = value;
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
   *                 &lt;attribute name="parameter-value" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
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
  public static class MethodAuthorizer implements Serializable {

    private static final long serialVersionUID = 5433617198900520954L;

    @XmlElement(namespace = "http://geode.apache.org/schema/query-config-service")
    protected List<QueryConfigService.MethodAuthorizer.Parameter> parameter;
    @XmlAttribute(name = "class-name", required = true)
    protected String className;

    /**
     * Gets the value of the parameter property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
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
     * {@link QueryConfigService.MethodAuthorizer.Parameter }
     *
     *
     */
    public List<QueryConfigService.MethodAuthorizer.Parameter> getParameters() {
      if (parameter == null) {
        parameter = new ArrayList<>();
      }
      return parameter;
    }

    /**
     * Sets the value of the parameter property.
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link QueryConfigService.MethodAuthorizer.Parameter }
     *
     */
    public void setParameters(
        List<QueryConfigService.MethodAuthorizer.Parameter> parameters) {
      parameter = parameters;
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
      className = value;
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
     *       &lt;attribute name="parameter-value" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "")
    public static class Parameter implements Serializable {

      private static final long serialVersionUID = -7316124128610501641L;

      @XmlAttribute(name = "parameter-value", required = true)
      protected String parameterValue;

      /**
       * Gets the value of the parameter-value property.
       *
       * possible object is
       * {@link String }
       *
       */
      public String getParameterValue() {
        return parameterValue;
      }

      /**
       * Sets the value of the parameter-value property.
       *
       * allowed object is
       * {@link String }
       *
       */
      public void setParameterValue(String value) {
        parameterValue = value;
      }

    }

  }

}
