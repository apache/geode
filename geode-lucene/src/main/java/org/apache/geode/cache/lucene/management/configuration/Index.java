
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

package org.apache.geode.cache.lucene.management.configuration;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.DeclarableType;
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
 *       &lt;sequence>
 *         &lt;element name="field" maxOccurs="unbounded">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="analyzer" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="serializer" type="{http://geode.apache.org/schema/cache}declarable-type" minOccurs="0"/>
 *       &lt;/sequence>
 *       &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "field",
    "serializer"
})
@XmlRootElement(name = "index", namespace = "http://geode.apache.org/schema/lucene")
@XSDRootElement(namespace = "http://geode.apache.org/schema/lucene",
    schemaLocation = "http://geode.apache.org/schema/lucene/lucene-1.0.xsd")
public class Index extends CacheElement {
  @XmlElement(namespace = "http://geode.apache.org/schema/lucene", required = true)
  protected List<Index.Field> field;
  @XmlElement(namespace = "http://geode.apache.org/schema/lucene")
  protected DeclarableType serializer;
  @XmlAttribute(name = "name")
  protected String name;

  /**
   * Gets the value of the field property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the field property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getField().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link Index.Field }
   *
   *
   */
  public List<Index.Field> getField() {
    if (field == null) {
      field = new ArrayList<Index.Field>();
    }
    return field;
  }

  /**
   * Gets the value of the serializer property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   */
  public DeclarableType getSerializer() {
    return serializer;
  }

  /**
   * Sets the value of the serializer property.
   *
   * allowed object is
   * {@link DeclarableType }
   *
   */
  public void setSerializer(DeclarableType value) {
    serializer = value;
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
    name = value;
  }

  @Override
  public String getId() {
    return getName();
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
   *       &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="analyzer" type="{http://www.w3.org/2001/XMLSchema}string" />
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "")
  public static class Field {

    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "analyzer")
    protected String analyzer;

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
      name = value;
    }

    /**
     * Gets the value of the analyzer property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getAnalyzer() {
      return analyzer;
    }

    /**
     * Sets the value of the analyzer property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setAnalyzer(String value) {
      analyzer = value;
    }

  }

}
