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

  @Override
  protected Index fromNonNullXmlObject(RegionConfig.Index regionConfigIndex) {
    Index index = new Index();
    index.setName(regionConfigIndex.getName());
    index.setExpression(regionConfigIndex.getExpression());
    index.setRegionPath(regionConfigIndex.getFromClause());

    if (regionConfigIndex.getType() != null) {
      index.setIndexType(IndexType.valueOfSynonym(regionConfigIndex.getType()));
    }

    return index;
  }

  @Override
  protected RegionConfig.Index fromNonNullConfigObject(Index index) {
    RegionConfig.Index regionConfigIndex = new RegionConfig.Index();
    regionConfigIndex.setName(index.getName());
    regionConfigIndex.setFromClause(index.getRegionPath());
    regionConfigIndex.setExpression(index.getExpression());

    if (index.getIndexType() != null) {
      regionConfigIndex.setType(index.getIndexType().getSynonym());
    }

    return regionConfigIndex;
  }
}
