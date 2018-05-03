
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
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.w3c.dom.Element;

import org.apache.geode.annotations.Experimental;


/**
 *
 * A "region" element describes a region (and its entries) in Geode distributed cache.
 * It may be used to create a new region or may be used to add new entries to an existing
 * region. Note that the "name" attribute specifies the simple name of the region; it
 * cannot contain a "/". If "refid" is set then it defines the default region attributes
 * to use for this region. A nested "region-attributes" element can override these defaults.
 * If the nested "region-attributes" element has its own "refid" then it will cause the
 * "refid" on the region to be ignored. "refid" can be set to the name of a RegionShortcut
 * or a ClientRegionShortcut (see the javadocs of those enum classes for their names).
 *
 *
 * <p>
 * Java class for region-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="region-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="region-attributes" type="{http://geode.apache.org/schema/cache}region-attributes-type" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="index" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;choice minOccurs="0">
 *                   &lt;element name="functional">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;attribute name="expression" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                           &lt;attribute name="from-clause" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                           &lt;attribute name="imports" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                   &lt;element name="primary-key">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;attribute name="field" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                 &lt;/choice>
 *                 &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="expression" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="from-clause" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="imports" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="key-index" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="type" default="range">
 *                   &lt;simpleType>
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                       &lt;enumeration value="range"/>
 *                       &lt;enumeration value="hash"/>
 *                     &lt;/restriction>
 *                   &lt;/simpleType>
 *                 &lt;/attribute>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="entry" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="key">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;choice>
 *                             &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/>
 *                             &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/>
 *                           &lt;/choice>
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                   &lt;element name="value">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;choice>
 *                             &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/>
 *                             &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/>
 *                           &lt;/choice>
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;any processContents='lax' namespace='##other' maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="region" type="{http://geode.apache.org/schema/cache}region-type" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="refid" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "region-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"regionAttributes", "index", "entry", "regionElements", "region"})
@Experimental
public class RegionConfig implements CacheElement {

  @XmlElement(name = "region-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionAttributesType> regionAttributes;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionConfig.Index> index;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionConfig.Entry> entry;
  @XmlAnyElement(lax = true)
  protected List<CacheElement> regionElements;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionConfig> region;
  @XmlAttribute(name = "name", required = true)
  protected String name;
  @XmlAttribute(name = "refid")
  protected String refid;

  public RegionConfig() {}

  public RegionConfig(String name, String refid) {
    this.name = name;
    this.refid = refid;
  }

  /**
   * Gets the value of the regionAttributes property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the regionAttributes property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getRegionAttributes().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionAttributesType }
   *
   *
   */
  public List<RegionAttributesType> getRegionAttributes() {
    if (regionAttributes == null) {
      regionAttributes = new ArrayList<RegionAttributesType>();
    }
    return this.regionAttributes;
  }

  /**
   * Gets the value of the index property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the index property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getIndex().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionConfig.Index }
   *
   *
   */
  public List<RegionConfig.Index> getIndex() {
    if (index == null) {
      index = new ArrayList<RegionConfig.Index>();
    }
    return this.index;
  }

  /**
   * Gets the value of the entry property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the entry property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getEntry().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionConfig.Entry }
   *
   *
   */
  public List<RegionConfig.Entry> getEntry() {
    if (entry == null) {
      entry = new ArrayList<RegionConfig.Entry>();
    }
    return this.entry;
  }

  /**
   * Gets the value of the any property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the any property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getCustomRegionElements().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link Element }
   * {@link CacheElement }
   *
   *
   */
  public List<CacheElement> getCustomRegionElements() {
    if (regionElements == null) {
      regionElements = new ArrayList<>();
    }
    return this.regionElements;
  }

  /**
   * Gets the value of the region property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the region property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getRegion().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionConfig }
   *
   *
   */
  public List<RegionConfig> getRegion() {
    if (region == null) {
      region = new ArrayList<RegionConfig>();
    }
    return this.region;
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

  /**
   * Gets the value of the refid property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getRefid() {
    return refid;
  }

  /**
   * Sets the value of the refid property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setRefid(String value) {
    this.refid = value;
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
   *       &lt;sequence>
   *         &lt;element name="key">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;choice>
   *                   &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/>
   *                   &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/>
   *                 &lt;/choice>
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *         &lt;element name="value">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;choice>
   *                   &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/>
   *                   &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/>
   *                 &lt;/choice>
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
  @XmlType(name = "", propOrder = {"key", "value"})
  public static class Entry {

    @XmlElement(namespace = "http://geode.apache.org/schema/cache", required = true)
    protected RegionConfig.Entry.Type key;
    @XmlElement(namespace = "http://geode.apache.org/schema/cache", required = true)
    protected RegionConfig.Entry.Type value;

    public Entry() {};

    public Entry(String key, String value) {
      this.key = new Type(key);
      this.value = new Type(value);
    }

    public Entry(Type key, Type value) {
      this.key = key;
      this.value = value;
    }

    /**
     * Gets the value of the key property.
     *
     * possible object is
     * {@link RegionConfig.Entry.Type }
     *
     */
    public RegionConfig.Entry.Type getKey() {
      return key;
    }

    /**
     * Sets the value of the key property.
     *
     * allowed object is
     * {@link RegionConfig.Entry.Type }
     *
     */
    public void setKey(RegionConfig.Entry.Type value) {
      this.key = value;
    }

    /**
     * Gets the value of the value property.
     *
     * possible object is
     * {@link RegionConfig.Entry.Type }
     *
     */
    public RegionConfig.Entry.Type getValue() {
      return value;
    }

    /**
     * Sets the value of the value property.
     *
     * allowed object is
     * {@link RegionConfig.Entry.Type }
     *
     */
    public void setValue(RegionConfig.Entry.Type value) {
      this.value = value;
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
     *       &lt;choice>
     *         &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/>
     *         &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/>
     *       &lt;/choice>
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {"string", "declarable"})
    public static class Type {
      @XmlElement(namespace = "http://geode.apache.org/schema/cache")
      protected StringType string;
      @XmlElement(namespace = "http://geode.apache.org/schema/cache")
      protected DeclarableType declarable;

      public Type() {}

      public Type(String string) {
        this.string = new StringType(string);
      }

      public Type(DeclarableType declarable) {
        this.declarable = declarable;
      }

      /**
       * Gets the value of the string property.
       *
       * possible object is
       * {@link StringType }
       *
       */
      public StringType getString() {
        return string;
      }

      /**
       * Sets the value of the string property.
       *
       * allowed object is
       * {@link StringType }
       *
       */
      public void setString(String value) {
        this.string = new StringType(value);
      }

      /**
       * Gets the value of the declarable property.
       *
       * possible object is
       * {@link DeclarableType }
       *
       */
      public DeclarableType getDeclarable() {
        return declarable;
      }

      /**
       * Sets the value of the declarable property.
       *
       * allowed object is
       * {@link DeclarableType }
       *
       */
      public void setDeclarable(DeclarableType value) {
        this.declarable = value;
      }

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
   * &lt;complexType>
   *   &lt;complexContent>
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *       &lt;choice minOccurs="0">
   *         &lt;element name="functional">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;attribute name="expression" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                 &lt;attribute name="from-clause" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                 &lt;attribute name="imports" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *         &lt;element name="primary-key">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;attribute name="field" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *       &lt;/choice>
   *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="expression" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="from-clause" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="imports" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="key-index" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="type" default="range">
   *         &lt;simpleType>
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
   *             &lt;enumeration value="range"/>
   *             &lt;enumeration value="hash"/>
   *           &lt;/restriction>
   *         &lt;/simpleType>
   *       &lt;/attribute>
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class Index implements CacheElement {
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "expression")
    protected String expression;
    @XmlAttribute(name = "from-clause")
    protected String fromClause;
    @XmlAttribute(name = "imports")
    protected String imports;
    @XmlAttribute(name = "key-index")
    protected Boolean keyIndex;
    @XmlAttribute(name = "type")
    protected String type; // for non-key index type, range or hash

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

    /**
     * Gets the value of the expression property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getExpression() {
      return expression;
    }

    /**
     * Sets the value of the expression property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setExpression(String value) {
      this.expression = value;
    }

    /**
     * Gets the value of the fromClause property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getFromClause() {
      return fromClause;
    }

    /**
     * Sets the value of the fromClause property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setFromClause(String value) {
      this.fromClause = value;
    }

    /**
     * Gets the value of the imports property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getImports() {
      return imports;
    }

    /**
     * Sets the value of the imports property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setImports(String value) {
      this.imports = value;
    }

    /**
     * Gets the value of the keyIndex property.
     *
     * possible object is
     * {@link Boolean }
     *
     */
    public Boolean isKeyIndex() {
      return keyIndex;
    }

    /**
     * Sets the value of the keyIndex property.
     *
     * allowed object is
     * {@link Boolean }
     *
     */
    public void setKeyIndex(Boolean value) {
      this.keyIndex = value;
    }

    /**
     * Gets the value of the type property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getType() {
      if (type == null) {
        return "range";
      } else {
        return type;
      }
    }

    /**
     * Sets the value of the type property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setType(String value) {
      if ("range".equalsIgnoreCase(value) || "hash".equalsIgnoreCase(value)) {
        this.type = value.toLowerCase();
      } else {
        throw new IllegalArgumentException("Invalid index type " + value);
      }
    }

    @Override
    public String getId() {
      return getName();
    }
  }

}
