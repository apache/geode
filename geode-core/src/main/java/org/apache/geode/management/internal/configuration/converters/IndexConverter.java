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

package org.apache.geode.management.internal.configuration.converters;



import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;

public class IndexConverter extends ConfigurationConverter<Index, RegionConfig.Index> {

  /**
   * based on the cache.xsd, index type can have only two values "range", "hash"
   * <attribute name="type" default="range">
   * <simpleType>
   * <restriction base="{http://www.w3.org/2001/XMLSchema}string">
   * <enumeration value="range"/>
   * <enumeration value="hash"/>
   * </restriction>
   * </simpleType>
   * </attribute>
   */
  private static final String HASH = "hash";
  private static final String RANGE = "range";

  @Override
  protected Index fromNonNullXmlObject(RegionConfig.Index regionConfigIndex) {
    Index index = new Index();
    index.setName(regionConfigIndex.getName());
    index.setExpression(regionConfigIndex.getExpression());
    index.setRegionPath(regionConfigIndex.getFromClause());

    Boolean keyIndex = regionConfigIndex.isKeyIndex();
    if (keyIndex != null && keyIndex) {
      index.setIndexType(IndexType.KEY);
    } else if (HASH.equalsIgnoreCase(regionConfigIndex.getType())) {
      index.setIndexType(IndexType.HASH_LEGACY);
    }
    // functional is the default type
    else {
      index.setIndexType(IndexType.RANGE);
    }

    return index;
  }

  @Override
  protected RegionConfig.Index fromNonNullConfigObject(Index index) {
    RegionConfig.Index regionConfigIndex = new RegionConfig.Index();
    regionConfigIndex.setName(index.getName());
    regionConfigIndex.setFromClause(index.getRegionPath());
    regionConfigIndex.setExpression(index.getExpression());

    if (index.getIndexType() == IndexType.KEY) {
      regionConfigIndex.setKeyIndex(true);
    } else if (index.getIndexType() == IndexType.HASH_LEGACY) {
      regionConfigIndex.setType(HASH);
    } else {
      regionConfigIndex.setType(RANGE);
    }

    return regionConfigIndex;
  }
}
