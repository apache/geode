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



import static org.apache.geode.management.configuration.Region.SEPARATOR;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.management.runtime.IndexInfo;

public class Index extends AbstractConfiguration<IndexInfo> implements RegionScoped {
  public static final String INDEXES = "/indexes";
  private String name;
  private String expression;
  private String regionPath;
  private IndexType indexType;

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

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
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
    regionName = StringUtils.removeStart(regionName, SEPARATOR);
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
  public Links getLinks() {
    String regionName = getRegionName();
    // /indexes/indexName is not implemented in controller anymore. region name is required for the
    // self link
    if (StringUtils.isBlank(regionName)) {
      return new Links(null, INDEXES);
    }
    Links links = new Links(getId(), Region.REGION_CONFIG_ENDPOINT + "/" + regionName + INDEXES);
    links.addLink("region", Region.REGION_CONFIG_ENDPOINT + "/" + regionName);
    return links;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Index index = (Index) o;

    if (!Objects.equals(name, index.name)) {
      return false;
    }
    if (!Objects.equals(expression, index.expression)) {
      return false;
    }
    if (!Objects.equals(regionPath, index.regionPath)) {
      return false;
    }
    return indexType == index.indexType;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (expression != null ? expression.hashCode() : 0);
    result = 31 * result + (regionPath != null ? regionPath.hashCode() : 0);
    result = 31 * result + (indexType != null ? indexType.hashCode() : 0);
    return result;
  }
}
