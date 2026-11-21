
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlType;

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
 * &lt;complexType&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="field" maxOccurs="unbounded"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="analyzer" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="serializer" type="{http://geode.apache.org/schema/cache}declarable-type" minOccurs="0"/&gt;
 *       &lt;/sequence&gt;
 *       &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "field",
    "vectorField",
    "serializer"
})
@XmlRootElement(name = "index", namespace = "http://geode.apache.org/schema/lucene")
@XSDRootElement(namespace = "http://geode.apache.org/schema/lucene",
    schemaLocation = "http://geode.apache.org/schema/lucene/lucene-1.0.xsd")
public class Index extends CacheElement {
  @XmlElement(namespace = "http://geode.apache.org/schema/lucene")
  protected List<Index.Field> field;
  @XmlElement(name = "vector-field", namespace = "http://geode.apache.org/schema/lucene")
  protected List<Index.VectorField> vectorField;
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
   * @return the value of the field property.
   */
  public List<Index.Field> getField() {
    if (field == null) {
      field = new ArrayList<>();
    }
    return field;
  }

  /**
   * Gets the value of the vectorField property for cluster configuration / REST management.
   *
   * @return live list of configured {@link Index.VectorField} descriptors
   * @since Geode 2.1
   */
  public List<Index.VectorField> getVectorField() {
    if (vectorField == null) {
      vectorField = new ArrayList<>();
    }
    return vectorField;
  }

  /**
   * Gets the value of the serializer property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   * @return the value of the serializer property
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
   * @param value the value of the serializer property to set
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
   * @param value the value of the name property to set
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
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="analyzer" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
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
     * @param value the value of the name property to set
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
     * @return the value of the analyzer property
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
     * @param value the value of the analyzer property to set
     */
    public void setAnalyzer(String value) {
      analyzer = value;
    }

  }

  /**
   * JAXB configuration element for a {@code <lucene:vector-field>} descriptor.
   *
   * @since Geode 2.1
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "")
  public static class VectorField implements Serializable {
    private static final long serialVersionUID = 1L;

    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "dimension", required = true)
    protected int dimension;
    @XmlAttribute(name = "similarity")
    protected String similarity;
    @XmlAttribute(name = "max-connections")
    protected Integer maxConnections;
    @XmlAttribute(name = "beam-width")
    protected Integer beamWidth;
    @XmlAttribute(name = "quantize-sq8")
    protected boolean quantizeSQ8;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getDimension() {
      return dimension;
    }

    public void setDimension(int dimension) {
      this.dimension = dimension;
    }

    public String getSimilarity() {
      return similarity;
    }

    public void setSimilarity(String similarity) {
      this.similarity = similarity;
    }

    public Integer getMaxConnections() {
      return maxConnections;
    }

    public void setMaxConnections(Integer maxConnections) {
      this.maxConnections = maxConnections;
    }

    public Integer getBeamWidth() {
      return beamWidth;
    }

    public void setBeamWidth(Integer beamWidth) {
      this.beamWidth = beamWidth;
    }

    public boolean isQuantizeSQ8() {
      return quantizeSQ8;
    }

    public void setQuantizeSQ8(boolean quantizeSQ8) {
      this.quantizeSQ8 = quantizeSQ8;
    }
  }

}
