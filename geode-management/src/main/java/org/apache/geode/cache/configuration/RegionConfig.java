
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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * this holds the attributes that belongs to region element in cache.xml, but can not be
 * configured through manage v2 API (yet)
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "region-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"indexes", "regionElements", "entries", "regions"})
@Experimental
public class RegionConfig extends BasicRegionConfig {
  @XmlElement(name = "index", namespace = "http://geode.apache.org/schema/cache")
  protected List<Index> indexes;
  @XmlAnyElement(lax = true)
  protected List<CacheElement> regionElements;

  @XmlElement(name = "entry", namespace = "http://geode.apache.org/schema/cache")
  protected List<Entry> entries;

  @XmlElement(name = "region", namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionConfig> regions;

  public RegionConfig() {}

  public RegionConfig(String name, String type) {
    super(name, type);
  }

  // a convenience constructor to turn a BasicRegionConfig into RegionConfig
  public RegionConfig(BasicRegionConfig regionConfig) {
    this.name = regionConfig.getName();
    this.type = regionConfig.getType();
    this.regionAttributes = regionConfig.getRegionAttributes();
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
   * getIndexes().add(newItem);
   * </pre>
   */
  public List<Index> getIndexes() {
    if (indexes == null) {
      indexes = new ArrayList<>();
    }
    return this.indexes;
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
   */
  public List<CacheElement> getCustomRegionElements() {
    if (regionElements == null) {
      regionElements = new ArrayList<>();
    }
    return this.regionElements;
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
   * getEntries().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionConfig.Entry }
   *
   *
   */
  public List<Entry> getEntries() {
    if (entries == null) {
      entries = new ArrayList<Entry>();
    }
    return this.entries;
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
   * getRegions().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionConfig }
   *
   *
   */
  public List<RegionConfig> getRegions() {
    if (regions == null) {
      regions = new ArrayList<RegionConfig>();
    }
    return this.regions;
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
  public static class Entry implements Serializable {

    @XmlElement(namespace = "http://geode.apache.org/schema/cache", required = true)
    protected ObjectType key;
    @XmlElement(namespace = "http://geode.apache.org/schema/cache", required = true)
    protected ObjectType value;

    public Entry() {};

    public Entry(String key, String value) {
      this.key = new ObjectType(key);
      this.value = new ObjectType(value);
    }

    public Entry(ObjectType key, ObjectType value) {
      this.key = key;
      this.value = value;
    }

    /**
     * Gets the value of the key property.
     *
     * possible object is
     * {@link ObjectType }
     *
     */
    public ObjectType getKey() {
      return key;
    }

    /**
     * Sets the value of the key property.
     *
     * allowed object is
     * {@link ObjectType }
     *
     */
    public void setKey(ObjectType value) {
      this.key = value;
    }

    /**
     * Gets the value of the value property.
     *
     * possible object is
     * {@link ObjectType }
     *
     */
    public ObjectType getValue() {
      return value;
    }

    /**
     * Sets the value of the value property.
     *
     * allowed object is
     * {@link ObjectType }
     *
     */
    public void setValue(ObjectType value) {
      this.value = value;
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
  public static class Index implements CacheElement, Serializable {
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
     * Sets the value of the type property. Also sets the keyIndex property to true if the type
     * being set is "key".
     *
     * allowed object is
     * {@link String }
     *
     * @deprecated Index should only be a "key" or "range" type which is set using
     *             {@link #setKeyIndex(Boolean)}
     */
    public void setType(String value) {
      if ("range".equalsIgnoreCase(value) || "hash".equalsIgnoreCase(value)
          || "key".equalsIgnoreCase(value)) {
        this.type = value.toLowerCase();
      } else {
        throw new IllegalArgumentException("Invalid index type " + value);
      }

      setKeyIndex("key".equalsIgnoreCase(value));
    }

    @Override
    public String getId() {
      return getName();
    }
  }
}
