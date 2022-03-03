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
package org.apache.geode.internal.cache.control;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.Region;

public class FilterByPath implements RegionFilter {

  private final Set<String> included;
  private final Set<String> excluded;

  public FilterByPath(Set<String> included, Set<String> excluded) {
    super();
    if (included != null) {
      this.included = new HashSet<>();
      for (String regionName : included) {
        this.included
            .add((!regionName.startsWith(SEPARATOR)) ? (SEPARATOR + regionName) : regionName);
      }
    } else {
      this.included = null;
    }
    if (excluded != null) {
      this.excluded = new HashSet<>();
      for (String regionName : excluded) {
        this.excluded
            .add((!regionName.startsWith(SEPARATOR)) ? (SEPARATOR + regionName) : regionName);
      }
    } else {
      this.excluded = null;
    }
  }


  @Override
  public boolean include(Region<?, ?> region) {
    String fullPath = region.getFullPath();
    if (included != null) {
      return included.contains(fullPath);
    }
    if (excluded != null) {
      return !excluded.contains(fullPath);
    }

    return true;
  }

}
