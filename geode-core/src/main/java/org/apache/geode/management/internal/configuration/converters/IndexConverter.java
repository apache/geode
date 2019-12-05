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

import static java.util.stream.Collectors.toMap;
import static org.apache.geode.management.configuration.IndexType.FUNCTIONAL;
import static org.apache.geode.management.configuration.IndexType.HASH_DEPRECATED;
import static org.apache.geode.management.configuration.IndexType.PRIMARY_KEY;

import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;

public class IndexConverter extends ConfigurationConverter<Index, RegionConfig.Index> {
  private static final Map<IndexType, String> INDEX_TYPE_TO_XML_NAME =
      new EnumMap<>(IndexType.class);
  private static final Map<String, IndexType> XML_NAME_TO_INDEX_TYPE;

  static {
    INDEX_TYPE_TO_XML_NAME.put(FUNCTIONAL, "RANGE");
    INDEX_TYPE_TO_XML_NAME.put(HASH_DEPRECATED, "HASH");
    INDEX_TYPE_TO_XML_NAME.put(PRIMARY_KEY, "KEY");

    XML_NAME_TO_INDEX_TYPE = INDEX_TYPE_TO_XML_NAME.entrySet().stream()
        .collect(toMap(Entry::getValue, Entry::getKey));
  }

  @Override
  protected Index fromNonNullXmlObject(RegionConfig.Index regionConfigIndex) {
    Index index = new Index();
    index.setName(regionConfigIndex.getName());
    index.setExpression(regionConfigIndex.getExpression());
    index.setRegionPath(regionConfigIndex.getFromClause());
    index.setKeyIndex(regionConfigIndex.isKeyIndex());

    if (regionConfigIndex.getType() != null) {
      String typeName = regionConfigIndex.getType().toUpperCase();
      if (XML_NAME_TO_INDEX_TYPE.containsKey(typeName)) {
        index.setIndexType(XML_NAME_TO_INDEX_TYPE.get(typeName));
      } else {
        throw new IllegalArgumentException("Unexpected index type provided: " + typeName);
      }
    }

    return index;
  }

  @Override
  protected RegionConfig.Index fromNonNullConfigObject(Index index) {
    RegionConfig.Index regionConfigIndex = new RegionConfig.Index();
    regionConfigIndex.setName(index.getName());
    regionConfigIndex.setFromClause(index.getRegionPath());
    regionConfigIndex.setExpression(index.getExpression());
    regionConfigIndex.setKeyIndex(index.getKeyIndex());

    if (index.getIndexType() != null) {
      regionConfigIndex.setType(INDEX_TYPE_TO_XML_NAME.get(index.getIndexType()));
    }

    return regionConfigIndex;
  }
}
