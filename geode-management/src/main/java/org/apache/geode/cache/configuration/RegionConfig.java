
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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.RestfulEndpoint;


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
    propOrder = {"regionAttributes", "indexes", "entries", "regionElements", "regions"})
@Experimental
public class RegionConfig extends AbstractCacheElement implements RestfulEndpoint {

  public static final String REGION_CONFIG_ENDPOINT = "/regions";

  @XmlElement(name = "region-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected RegionAttributesType regionAttributes;
  @XmlElement(name = "index", namespace = "http://geode.apache.org/schema/cache")
  protected List<Index> indexes;

  @ApiModelProperty(hidden = true)
  @XmlElement(name = "entry", namespace = "http://geode.apache.org/schema/cache")
  protected List<Entry> entries;

  @XmlAnyElement(lax = true)
  protected List<CacheElement> regionElements;

  @ApiModelProperty(hidden = true)
  @XmlElement(name = "region", namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionConfig> regions;

  @XmlAttribute(name = "name", required = true)
  protected String name;

  @XmlAttribute(name = "refid")
  protected String type;

  public RegionConfig() {}

  public RegionConfig(String name, String refid) {
    this.name = name;
    this.type = refid;
  }

  public RegionConfig(RegionConfig config) {
    this.regionAttributes = config.getRegionAttributes();
    this.type = config.getType();
    this.entries = config.getEntries();
    this.indexes = config.getIndexes();
    this.name = config.getName();
    this.regionElements = config.getCustomRegionElements();
    this.regions = config.getRegions();
    this.setGroup(config.getGroup());
  }

  @Override
  public String getEndpoint() {
    return REGION_CONFIG_ENDPOINT;
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
   *
   */
  public List<Index> getIndexes() {
    if (indexes == null) {
      indexes = new ArrayList<>();
    }
    return this.indexes;
  }

  /**
   * Gets the value of the entry property.
   * Currently, users can not create regions with initial entries using management v2 api.
   * this entry list will be ignored when creating the region
   */
  public List<Entry> getEntries() {
    if (entries == null) {
      entries = new ArrayList<>();
    }
    return this.entries;
  }

  /**
   * Gets the list of custom region elements
   * Currently, users can not create regions with custom region elements using management v2 api.
   * this cache element list will be ignored when creating the region
   */
  @ApiModelProperty(hidden = true)
  public List<CacheElement> getCustomRegionElements() {
    if (regionElements == null) {
      regionElements = new ArrayList<>();
    }
    return this.regionElements;
  }

  /**
   * Gets the list of the sub regions
   * Currently, users can not create regions with sub regions using management v2 api.
   * This sub region list will be ignored when creating the region.
   */
  public List<RegionConfig> getRegions() {
    if (regions == null) {
      regions = new ArrayList<>();
    }
    return this.regions;
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
  public void setName(String value) throws IllegalArgumentException {
    if (value == null) {
      return;
    }

    boolean regionPrefixedWithSlash = value.startsWith("/");
    String[] regionSplit = value.split("/");

    boolean hasSubRegions =
        regionPrefixedWithSlash ? regionSplit.length > 2 : regionSplit.length > 1;
    if (hasSubRegions) {
      throw new IllegalArgumentException("Sub-regions are unsupported");
    }

    this.name = regionPrefixedWithSlash ? regionSplit[1] : value;
  }

  /**
   * Gets the value of the type property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setType(RegionType regionType) {
    if (regionType != null) {
      setType(regionType.name());
    }
  }

  public void setType(String regionType) {
    if (regionType != null) {
      this.type = regionType.toUpperCase();
    }
  }

  @Override
  @JsonIgnore
  public String getId() {
    return getName();
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    RegionConfig config = (RegionConfig) that;
    return Objects.equals(getName(), config.getName()) &&
        Objects.equals(getType(), config.getType());
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
  public static class Index extends AbstractCacheElement implements RestfulEndpoint {
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
    @XmlTransient
    protected String regionName;

    public Index() {}

    public Index(Index index) {
      this.name = index.name;
      this.expression = index.expression;
      this.fromClause = index.fromClause;
      this.imports = index.imports;
      this.keyIndex = index.keyIndex;
      this.type = index.type;
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
     * being set is "key".
     *
     * allowed object is
     * {@link String }
     *
     * @deprecated Index should only be a "key" or "range" type which is set using
     *             {@link #setKeyIndex(Boolean)}
     */
    public void setType(String value) {
      if ("range".equalsIgnoreCase(value) || "hash".equalsIgnoreCase(value)) {
        this.type = value.toLowerCase();
        setKeyIndex(false);
      }
      // we need to avoid setting the "type" to key since by xsd definition, it should only contain
      // "hash" and "range" value.
      else if ("key".equalsIgnoreCase(value)) {
        this.type = null;
        setKeyIndex(true);
      } else {
        throw new IllegalArgumentException("Invalid index type " + value);
      }
    }

    public String getRegionName() {
      return regionName;
    }

    public void setRegionName(String regionName) {
      this.regionName = regionName;
      if (fromClause == null) {
        fromClause = "/" + regionName;
      } else if (!fromClause.contains(regionName)) {
        throw new IllegalArgumentException(
            "Invalid regionName for this index with fromClause = " + fromClause);
      }
    }

    @Override
    @JsonIgnore
    public String getId() {
      return getName();
    }

    @Override
    public String getEndpoint() {
      if (StringUtils.isBlank(regionName)) {
        throw new IllegalArgumentException("regionName is required.");
      }
      return RegionConfig.REGION_CONFIG_ENDPOINT + "/" + regionName + "/indexes";
    }
  }

}
