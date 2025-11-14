
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

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * A serialization-registration contains a set of serializer or instantiator tags to
 * register customer DataSerializer extensions or DataSerializable implementations respectively.
 *
 *
 * <p>
 * Java class for serialization-registration-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="serialization-registration-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="serializer" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="instantiator" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                 &lt;/sequence&gt;
 *                 &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *       &lt;/sequence&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "serialization-registration-type",
    namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"serializers", "instantiators"})
@Experimental
public class SerializationRegistrationType {

  @XmlElement(name = "serializer", namespace = "http://geode.apache.org/schema/cache")
  protected List<Serializer> serializers;
  @XmlElement(name = "instantiator", namespace = "http://geode.apache.org/schema/cache")
  protected List<Instantiator> instantiators;

  /**
   * Gets the value of the serializer property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the serializer property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getSerializer().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link SerializationRegistrationType.Serializer }
   *
   * @return the value of the serializer property
   */
  public List<Serializer> getSerializers() {
    if (serializers == null) {
      serializers = new ArrayList<>();
    }
    return serializers;
  }

  /**
   * Gets the value of the instantiator property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the instantiator property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getInstantiator().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link SerializationRegistrationType.Instantiator }
   *
   * @return the value of the instantiator property
   */
  public List<Instantiator> getInstantiators() {
    if (instantiators == null) {
      instantiators = new ArrayList<>();
    }
    return instantiators;
  }


  /**
   * <p>
   * Java class for anonymous complex type.
   *
   * <p>
   * The following schema fragment specifies the expected content contained within this class.
   *
   * <pre>
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;sequence&gt;
   *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
   *       &lt;/sequence&gt;
   *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"className"})
  public static class Instantiator {

    @XmlElement(name = "class-name", namespace = "http://geode.apache.org/schema/cache",
        required = true)
    protected String className;
    @XmlAttribute(name = "id", required = true)
    protected String id;

    /**
     * Gets the value of the className property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the className property
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
     * @param value the value of the className property
     */
    public void setClassName(String value) {
      className = value;
    }

    /**
     * Gets the value of the id property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the id property
     */
    public String getId() {
      return id;
    }

    /**
     * Sets the value of the id property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the value of the id property
     */
    public void setId(String value) {
      id = value;
    }

  }


  /**
   * <p>
   * Java class for anonymous complex type.
   *
   * <p>
   * The following schema fragment specifies the expected content contained within this class.
   *
   * <pre>
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;sequence&gt;
   *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
   *       &lt;/sequence&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"className"})
  public static class Serializer {

    @XmlElement(name = "class-name", namespace = "http://geode.apache.org/schema/cache",
        required = true)
    protected String className;

    /**
     * Gets the value of the className property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the className property
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
     * @param value the value of the className property
     */
    public void setClassName(String value) {
      className = value;
    }

  }

}
