
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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

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
 * &lt;complexType name="resource-manager-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;attribute name="critical-heap-percentage" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="eviction-heap-percentage" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="critical-off-heap-percentage" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="eviction-off-heap-percentage" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
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
   */
  public void setCriticalHeapPercentage(String value) {
    this.criticalHeapPercentage = value;
  }

  /**
   * Gets the value of the evictionHeapPercentage property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setEvictionHeapPercentage(String value) {
    this.evictionHeapPercentage = value;
  }

  /**
   * Gets the value of the criticalOffHeapPercentage property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setCriticalOffHeapPercentage(String value) {
    this.criticalOffHeapPercentage = value;
  }

  /**
   * Gets the value of the evictionOffHeapPercentage property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setEvictionOffHeapPercentage(String value) {
    this.evictionOffHeapPercentage = value;
  }

}
