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

import org.apache.geode.management.runtime.RuntimeInfo;

public class Index extends GroupableConfiguration<RuntimeInfo> {

  private String name;
  private String expression;
  private String regionPath;
  private Boolean keyIndex;

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

  public String getRegionPath() {
    return regionPath;
  }

  /**
   * the regionPath can be in any of these forms, e.g.:
   * 1. regionName
   * 2. /regionName
   * 3. /regionName alias
   * 4. /regionName.entrySet()
   * 5. /regionName.fieldName.entrySet() alias
   * <P>
   * Note: only the regionName portion of the path is used when filtering.
   */
  public void setRegionPath(String regionPath) {
    this.regionPath = regionPath;
  }

  public Boolean getKeyIndex() {
    return keyIndex;
  }

  public void setKeyIndex(Boolean keyIndex) {
    this.keyIndex = keyIndex;
  }

  /**
   * @return the regionName specified in the regionPath.
   *         Note this method assumes the regionName should not contain "."
   */
  @JsonIgnore
  public String getRegionName() {
    if (regionPath == null) {
      return null;
    }

    String regionName = regionPath.trim().split(" ")[0];
    regionName = StringUtils.removeStart(regionName, "/");
    if (regionName.contains(".")) {
      regionName = regionName.substring(0, regionName.indexOf('.'));
    }

    return regionName;
  }

  /**
   * Returns {@link #getName()}.
   */
  @Override
  @JsonIgnore
  public String getId() {
    return getName();
  }

  @Override
  public String getEndpoint() {
    String regionName = getRegionName();
    if (StringUtils.isBlank(regionName)) {
      return "/indexes";
    }
    return Region.REGION_CONFIG_ENDPOINT + "/" + regionName + "/indexes";
  }
}
