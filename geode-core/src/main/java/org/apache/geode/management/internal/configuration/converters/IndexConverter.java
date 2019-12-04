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
  protected Index fromNonNullXmlObject(RegionConfig.Index xmlObject) {
    Index index = new Index();
    index.setName(xmlObject.getName());
    index.setExpression(xmlObject.getExpression());
    index.setRegionPath(xmlObject.getFromClause());
    index.setKeyIndex(xmlObject.isKeyIndex());

    if (xmlObject.getType() != null) {
      switch(xmlObject.getType().toUpperCase()) {
        case "RANGE":
          index.setIndexType(IndexType.FUNCTIONAL);
        case "HASH":
          index.setIndexType(IndexType.HASH_DEPRECATED);
        case "KEY":
          index.setIndexType(IndexType.PRIMARY_KEY);
      }
    }

    return index;
  }

  @Override
  protected RegionConfig.Index fromNonNullConfigObject(Index configObject) {
    RegionConfig.Index index = new RegionConfig.Index();
    index.setName(configObject.getName());
    index.setFromClause(configObject.getRegionPath());
    index.setExpression(configObject.getExpression());
    index.setKeyIndex(configObject.getKeyIndex());

    if (configObject.getIndexType() != null) {
      switch(configObject.getIndexType()) {
        case FUNCTIONAL:
          index.setType("RANGE");
        case PRIMARY_KEY:
          index.setType("KEY");
        case HASH_DEPRECATED:
          index.setType("HASH");
      }
    }

    return index;
  }
}
