/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.cache.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.lang.Identifiable;

@Experimental
public interface CacheElement extends Identifiable<String>, Serializable {

  static <T extends CacheElement> T findElement(List<T> list, String id) {
    return list.stream().filter(o -> o.getId().equals(id)).findFirst().orElse(null);
  }

  static <T extends CacheElement> void removeElement(List<T> list, String id) {
    list.removeIf(t -> t.getId().equals(id));
  }

  static RegionConfig findRegionConfiguration(CacheConfig cacheConfig, String regionPath) {
    return findElement(cacheConfig.getRegion(), regionPath);
  }

  static <T extends CacheElement> List<T> findCustomCacheElements(CacheConfig cacheConfig,
      Class<T> classT) {
    List<T> newList = new ArrayList<>();
    // streaming won't work here, because it's trying to cast element into CacheElement
    for (Object element : cacheConfig.getCustomCacheElements()) {
      if (classT.isInstance(element)) {
        newList.add(classT.cast(element));
      }
    }
    return newList;
  }

  static <T extends CacheElement> T findCustomCacheElement(CacheConfig cacheConfig,
      String elementId, Class<T> classT) {
    return findElement(findCustomCacheElements(cacheConfig, classT), elementId);
  }

  static <T extends CacheElement> List<T> findCustomRegionElements(CacheConfig cacheConfig,
      String regionPath, Class<T> classT) {
    List<T> newList = new ArrayList<>();
    RegionConfig regionConfig = findRegionConfiguration(cacheConfig, regionPath);
    if (regionConfig == null) {
      return newList;
    }

    // streaming won't work here, because it's trying to cast element into CacheElement
    for (Object element : regionConfig.getCustomRegionElements()) {
      if (classT.isInstance(element)) {
        newList.add(classT.cast(element));
      }
    }
    return newList;
  }

  static <T extends CacheElement> T findCustomRegionElement(CacheConfig cacheConfig,
      String regionPath, String elementId, Class<T> classT) {
    return findElement(findCustomRegionElements(cacheConfig, regionPath, classT), elementId);
  }
}
