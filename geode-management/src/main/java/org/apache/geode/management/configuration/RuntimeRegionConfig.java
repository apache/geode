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


import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.RegionConfig;

@Experimental
public class RuntimeRegionConfig extends RegionConfig implements MultiGroupCacheElement {
  private long entryCount;

  public RuntimeRegionConfig() {}

  public RuntimeRegionConfig(RegionConfig config) {
    super(config);
  }

  public long getEntryCount() {
    return entryCount;
  }

  public void setEntryCount(long entrySize) {
    this.entryCount = entrySize;
  }

  public List<String> getGroups() {
    return groups;
  }

  public List<RuntimeIndex> getRuntimeIndexes(String indexId) {
    Stream<Index> stream = getIndexes().stream();
    if (StringUtils.isNotBlank(indexId)) {
      stream = stream.filter(i -> i.getId().equals(indexId));
    }
    return stream
        .map(e -> {
          RuntimeIndex index = new RuntimeIndex(e);
          index.setRegionName(getName());
          return index;
        })
        .collect(Collectors.toList());
  }
}
