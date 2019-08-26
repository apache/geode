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

package org.apache.geode.management.configuration;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.CorrespondWith;
import org.apache.geode.management.api.RestfulEndpoint;
import org.apache.geode.management.runtime.RuntimeRegionInfo;

/**
 * this holds the region attributes you can configure using management rest api
 *
 * for regions created using gfsh but listed using management rest api, the attributes not supported
 * by management rest api won't be shown.
 */
public class Region extends CacheElement implements RestfulEndpoint,
    CorrespondWith<RuntimeRegionInfo> {
  public static final String REGION_CONFIG_ENDPOINT = "/regions";

  private String name;
  private RegionType type;

  private String keyConstraint;
  private String valueConstraint;
  private String diskStoreName;
  private Integer redundantCopies;

  public Region() {}

  @Override
  public boolean isGlobalRuntime() {
    return true;
  }

  @Override
  @JsonIgnore
  public String getEndpoint() {
    return REGION_CONFIG_ENDPOINT;
  }

  @Override
  @JsonIgnore
  public String getId() {
    return getName();
  }

  public String getName() {
    return name;
  }

  public void setName(String value) {
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

  public RegionType getType() {
    return type;
  }

  public void setType(RegionType type) {
    this.type = type;
  }

  /**
   * @return the redundant copies of a region
   */
  public Integer getRedundantCopies() {
    if (type != null && type.withRedundant() && redundantCopies == null) {
      return 1;
    }
    return redundantCopies;
  }

  public void setRedundantCopies(Integer redundantCopies) {
    this.redundantCopies = redundantCopies;
  }

  public String getKeyConstraint() {
    return keyConstraint;
  }

  public void setKeyConstraint(String keyConstraint) {
    this.keyConstraint = keyConstraint;
  }

  public String getValueConstraint() {
    return valueConstraint;
  }

  public void setValueConstraint(String valueConstraint) {
    this.valueConstraint = valueConstraint;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public void setDiskStoreName(String diskStoreName) {
    this.diskStoreName = diskStoreName;
  }

  /**
   * two regions are equal if name and type are equal.
   */
  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    Region config = (Region) that;
    return Objects.equals(getName(), config.getName()) &&
        Objects.equals(getType(), config.getType());
  }


  @Override
  public int hashCode() {
    return Objects.hash(getName());
  }
}
