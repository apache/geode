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
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.CorrespondWith;
import org.apache.geode.management.api.RestfulEndpoint;
import org.apache.geode.management.runtime.RuntimeInfo;

public class Index extends CacheElement
    implements RestfulEndpoint, CorrespondWith<RuntimeInfo> {

  private String name;
  private String expression;
  private String fromClause;
  private Boolean keyIndex;
  private String regionName;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public String getFromClause() {
    return fromClause;
  }

  public void setFromClause(String fromClause) {
    this.fromClause = fromClause;
  }

  public Boolean getKeyIndex() {
    return keyIndex;
  }

  public void setKeyIndex(Boolean keyIndex) {
    this.keyIndex = keyIndex;
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  @Override
  @JsonIgnore
  public String getId() {
    return getName();
  }

  @Override
  public String getEndpoint() {
    if (StringUtils.isBlank(regionName)) {
      return null;
    }
    return RegionConfig.REGION_CONFIG_ENDPOINT + "/" + regionName + "/indexes";
  }
}
