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

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.CorrespondWith;
import org.apache.geode.management.api.RestfulEndpoint;
import org.apache.geode.management.runtime.RuntimeRegionInfo;

public class Region extends CacheElement implements RestfulEndpoint,
    CorrespondWith<RuntimeRegionInfo> {
  public static final String REGION_CONFIG_ENDPOINT = "/regions";

  private String name;
  private RegionType type;

  private String keyConstraint;
  private String valueConstraint;
  private String diskStoreName;

  public Region() {}

  public Region(String name, RegionType type) {
    this.name = name;
    this.type = type;
  }

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
}
