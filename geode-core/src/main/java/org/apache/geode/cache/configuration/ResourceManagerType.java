
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

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * The "resource manager" element configures the behavior of the resource manager.
 * The resource manager provides support for resource management of its associated Cache.
 *
 *
 * <p>
 * Java class for resource-manager-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="resource-manager-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;attribute name="critical-heap-percentage" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="eviction-heap-percentage" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="critical-off-heap-percentage" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="eviction-off-heap-percentage" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "resource-manager-type", namespace = "http://geode.apache.org/schema/cache")
@Experimental
public class ResourceManagerType {

  @XmlAttribute(name = "critical-heap-percentage")
  protected String criticalHeapPercentage;
  @XmlAttribute(name = "eviction-heap-percentage")
  protected String evictionHeapPercentage;
  @XmlAttribute(name = "critical-off-heap-percentage")
  protected String criticalOffHeapPercentage;
  @XmlAttribute(name = "eviction-off-heap-percentage")
  protected String evictionOffHeapPercentage;

  /**
   * Gets the value of the criticalHeapPercentage property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the criticalHeapPercentage property
   */
  public String getCriticalHeapPercentage() {
    return criticalHeapPercentage;
  }

  /**
   * Sets the value of the criticalHeapPercentage property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the criticalHeapPercentage property
   */
  public void setCriticalHeapPercentage(String value) {
    criticalHeapPercentage = value;
  }

  /**
   * Gets the value of the evictionHeapPercentage property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the evictionHeapPercentage property
   */
  public String getEvictionHeapPercentage() {
    return evictionHeapPercentage;
  }

  /**
   * Sets the value of the evictionHeapPercentage property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the evictionHeapPercentage property
   */
  public void setEvictionHeapPercentage(String value) {
    evictionHeapPercentage = value;
  }

  /**
   * Gets the value of the criticalOffHeapPercentage property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the criticalOffHeapPercentage property
   */
  public String getCriticalOffHeapPercentage() {
    return criticalOffHeapPercentage;
  }

  /**
   * Sets the value of the criticalOffHeapPercentage property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the criticalOffHeapPercentage property
   */
  public void setCriticalOffHeapPercentage(String value) {
    criticalOffHeapPercentage = value;
  }

  /**
   * Gets the value of the evictionOffHeapPercentage property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the evictionOffHeapPercentage property
   */
  public String getEvictionOffHeapPercentage() {
    return evictionOffHeapPercentage;
  }

  /**
   * Sets the value of the evictionOffHeapPercentage property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the evictionOffHeapPercentage property
   */
  public void setEvictionOffHeapPercentage(String value) {
    evictionOffHeapPercentage = value;
  }

}
