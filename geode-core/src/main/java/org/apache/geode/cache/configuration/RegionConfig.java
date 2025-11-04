
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

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAnyElement;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.configuration.RegionType;


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
 * &lt;complexType name="region-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="region-attributes" type="{http://geode.apache.org/schema/cache}region-attributes-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *         &lt;element name="index" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;choice minOccurs="0"&gt;
 *                   &lt;element name="functional"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;attribute name="expression" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                           &lt;attribute name="from-clause" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                           &lt;attribute name="imports" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                   &lt;element name="primary-key"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;attribute name="field" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                 &lt;/choice&gt;
 *                 &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="expression" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="from-clause" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="imports" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="key-index" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="type" default="range"&gt;
 *                   &lt;simpleType&gt;
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *                       &lt;enumeration value="range"/&gt;
 *                       &lt;enumeration value="hash"/&gt;
 *                     &lt;/restriction&gt;
 *                   &lt;/simpleType&gt;
 *                 &lt;/attribute&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="entry" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="key"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;choice&gt;
 *                             &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/&gt;
 *                             &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/&gt;
 *                           &lt;/choice&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                   &lt;element name="value"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;choice&gt;
 *                             &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/&gt;
 *                             &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/&gt;
 *                           &lt;/choice&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;any processContents='lax' namespace='##other' maxOccurs="unbounded" minOccurs="0"/&gt;
 *         &lt;element name="region" type="{http://geode.apache.org/schema/cache}region-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *       &lt;/sequence&gt;
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="refid" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "region-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"regionAttributes", "indexes", "entries", "regionElements", "regions"})
@Experimental
public class RegionConfig implements Identifiable<String>, Serializable {

  @XmlElement(name = "region-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected RegionAttributesType regionAttributes;
  @XmlElement(name = "index", namespace = "http://geode.apache.org/schema/cache")
  protected List<Index> indexes;

  @XmlElement(name = "entry", namespace = "http://geode.apache.org/schema/cache")
  protected List<Entry> entries;

  @XmlAnyElement(lax = true)
  protected List<CacheElement> regionElements;

  @XmlElement(name = "region", namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionConfig> regions;

  @XmlAttribute(name = "name", required = true)
  protected String name;

  @XmlAttribute(name = "refid")
  protected String type;

  public RegionConfig() {}

  public RegionConfig(String name, String refid) {
    this.name = name;
    type = refid;
  }

  public RegionAttributesType getRegionAttributes() {
    return regionAttributes;
  }

  public void setRegionAttributes(RegionAttributesType regionAttributes) {
    this.regionAttributes = regionAttributes;
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
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionConfig.Index }
   *
   * @return the value of the index property
   */
  public List<Index> getIndexes() {
    if (indexes == null) {
      indexes = new ArrayList<>();
    }
    return indexes;
  }

  /**
   * Gets the value of the entry property.
   * Currently, users can not create regions with initial entries using management v2 api.
   * this entry list will be ignored when creating the region
   *
   * @return the value of the entry property
   */
  public List<Entry> getEntries() {
    if (entries == null) {
      entries = new ArrayList<>();
    }
    return entries;
  }

  /**
   * Gets the list of custom region elements
   * Currently, users can not create regions with custom region elements using management v2 api.
   * this cache element list will be ignored when creating the region
   *
   * @return the list of custom region elements
   */
  public List<CacheElement> getCustomRegionElements() {
    if (regionElements == null) {
      regionElements = new ArrayList<>();
    }
    return regionElements;
  }

  /**
   * Gets the list of the sub regions
   * Currently, users can not create regions with sub regions using management v2 api.
   * This sub region list will be ignored when creating the region.
   *
   * @return the list of the sub regions
   */
  public List<RegionConfig> getRegions() {
    if (regions == null) {
      regions = new ArrayList<>();
    }
    return regions;
  }

  /**
   * Gets the value of the name property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the name property
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
   * @param value the value of the name property
   */
  public void setName(String value) throws IllegalArgumentException {
    if (value == null) {
      return;
    }

    name = value.startsWith(SEPARATOR) ? value.substring(1) : value;
  }

  /**
   * Gets the value of the type property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the type property
   */
  public String getType() {
    return type;
  }

  /**
   * Sets the value of the type property.
   *
   * allowed object is
   * {@link String }
   *
   * @param regionType the value of the type property
   */
  public void setType(RegionType regionType) {
    if (regionType != null) {
      setType(regionType.name());
    }
  }

  public void setType(String regionType) {
    if (regionType != null) {
      type = regionType.toUpperCase();
    }
  }

  @Override
  @JsonIgnore
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
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;sequence&gt;
   *         &lt;element name="key"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;choice&gt;
   *                   &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/&gt;
   *                   &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/&gt;
   *                 &lt;/choice&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *         &lt;element name="value"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;choice&gt;
   *                   &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/&gt;
   *                   &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/&gt;
   *                 &lt;/choice&gt;
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
  @XmlType(name = "", propOrder = {"key", "value"})
  public static class Entry implements Serializable {

    @XmlElement(namespace = "http://geode.apache.org/schema/cache", required = true)
    protected ObjectType key;
    @XmlElement(namespace = "http://geode.apache.org/schema/cache", required = true)
    protected ObjectType value;

    public Entry() {}

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
     * @return the value of the key property
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
     * @param value the value of the key property
     */
    public void setKey(ObjectType value) {
      key = value;
    }

    /**
     * Gets the value of the value property.
     *
     * possible object is
     * {@link ObjectType }
     *
     * @return the value of the value property
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
     * @param value the value of the value property
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
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;choice minOccurs="0"&gt;
   *         &lt;element name="functional"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;attribute name="expression" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                 &lt;attribute name="from-clause" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                 &lt;attribute name="imports" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *         &lt;element name="primary-key"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;attribute name="field" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *       &lt;/choice&gt;
   *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="expression" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="from-clause" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="imports" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="key-index" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="type" default="range"&gt;
   *         &lt;simpleType&gt;
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
   *             &lt;enumeration value="range"/&gt;
   *             &lt;enumeration value="hash"/&gt;
   *           &lt;/restriction&gt;
   *         &lt;/simpleType&gt;
   *       &lt;/attribute&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class Index implements Identifiable<String> {
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

    public Index() {}

    public Index(Index index) {
      name = index.name;
      expression = index.expression;
      fromClause = index.fromClause;
      imports = index.imports;
      keyIndex = index.keyIndex;
      type = index.type;
    }

    /**
     * Gets the value of the name property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the name property
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
     * @param value the value of the name property
     */
    public void setName(String value) {
      name = value;
    }

    /**
     * Gets the value of the expression property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the expression property
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
     * @param value the value of the expression property
     */
    public void setExpression(String value) {
      expression = value;
    }

    /**
     * Gets the value of the fromClause property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the fromClause property
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
     * @param value the value of the fromClause property
     */
    public void setFromClause(String value) {
      fromClause = value;
    }

    /**
     * Gets the value of the imports property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the imports property
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
     * @param value the value of the imports property
     */
    public void setImports(String value) {
      imports = value;
    }

    /**
     * Gets the value of the keyIndex property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return the value of the keyIndex property
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
     * @param value the value of the keyIndex property
     */
    public void setKeyIndex(Boolean value) {
      keyIndex = value;
    }

    /**
     * Gets the value of the type property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the type property
     */
    public String getType() {
      // this should return a "key" value because some production code relies on this method
      // returning a type string that would turn into IndexType enum object
      if (keyIndex == Boolean.TRUE) {
        return "key";
      }

      if (type == null) {
        return "range";
      } else {
        return type;
      }
    }

    /**
     * Sets the value of the type property. Also sets the keyIndex property to true if the type
     * being set is {@code "key"}.
     *
     * @param type the value of the type property
     * @throws IllegalArgumentException if type is unknown
     */
    public void setType(String type) {
      if ("range".equalsIgnoreCase(type) || "hash".equalsIgnoreCase(type)) {
        this.type = type.toLowerCase();
        setKeyIndex(false);
      }
      // we need to avoid setting the "type" to key since by xsd definition, it should only contain
      // "hash" and "range" value.
      else if ("key".equalsIgnoreCase(type)) {
        this.type = null;
        setKeyIndex(true);
      } else {
        throw new IllegalArgumentException("Invalid index type " + type);
      }
    }

    @Override
    @JsonIgnore
    public String getId() {
      return getName();
    }
  }

}
