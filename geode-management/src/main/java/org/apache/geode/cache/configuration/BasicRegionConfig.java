
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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonIgnore;

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
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {"regionAttributes"})
@Experimental
public class BasicRegionConfig extends CacheElement implements RestfulEndpoint {

  public static final String REGION_CONFIG_ENDPOINT = "/regions";

  @XmlElement(name = "region-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected RegionAttributesType regionAttributes;

  @XmlAttribute(name = "name", required = true)
  protected String name;

  @XmlAttribute(name = "refid")
  protected String type;

  public BasicRegionConfig() {}

  public BasicRegionConfig(String name, String refid) {
    this.name = name;
    this.type = refid;
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
   * Gets the value of the name property.
   *
   * possible object is {@link String }
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the value of the name property.
   *
   * allowed object is {@link String }
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
   * possible object is {@link String }
   */
  public String getType() {
    return type;
  }

  /**
   * Sets the value of the type property.
   *
   * allowed object is {@link String }
   */
  public void setType(RegionType regionType) {
    if (regionType != null) {
      setType(regionType.name());
    }
  }

  public void setType(String regionType) {
    if (regionType != null) {
      this.type = regionType.toUpperCase();
      setShortcutAttributes();
    }
  }

  private void setShortcutAttributes() {
    if (regionAttributes == null) {
      regionAttributes = new RegionAttributesType();
    }

    switch (type) {
      case "PARTITION": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        break;
      }
      case "REPLICATE": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        break;
      }
      case "PARTITION_REDUNDANT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setRedundantCopy("1");
        break;
      }
      case "PARTITION_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
        break;
      }
      case "PARTITION_REDUNDANT_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
        regionAttributes.setRedundantCopy("1");
        break;
      }
      case "PARTITION_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_REDUNDANT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setRedundantCopy("1");
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_PERSISTENT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_REDUNDANT_PERSISTENT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
        regionAttributes.setRedundantCopy("1");
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_HEAP_LRU": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.LOCAL_DESTROY);
        break;

      }
      case "PARTITION_REDUNDANT_HEAP_LRU": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setRedundantCopy("1");
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.LOCAL_DESTROY);
        break;
      }

      case "REPLICATE_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        break;
      }
      case "REPLICATE_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;

      }
      case "REPLICATE_PERSISTENT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "REPLICATE_HEAP_LRU": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PRELOADED);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        regionAttributes.setInterestPolicy("all");
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.LOCAL_DESTROY);
        break;
      }
      case "LOCAL": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.NORMAL);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        break;
      }
      case "LOCAL_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        break;
      }
      case "LOCAL_HEAP_LRU": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.NORMAL);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.LOCAL_DESTROY);
        break;
      }
      case "LOCAL_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.NORMAL);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "LOCAL_PERSISTENT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        regionAttributes.setLruHeapPercentage(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_PROXY": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setLocalMaxMemory("0");
        break;
      }
      case "PARTITION_PROXY_REDUNDANT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setLocalMaxMemory("0");
        regionAttributes.setRedundantCopy("1");
        break;
      }
      case "REPLICATE_PROXY": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.EMPTY);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        break;
      }
      default:
        throw new IllegalArgumentException("invalid type " + type);
    }
  }

  @Override
  @JsonIgnore
  public String getId() {
    return getName();
  }
}
