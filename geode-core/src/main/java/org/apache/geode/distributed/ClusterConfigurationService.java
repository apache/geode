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

package org.apache.geode.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.cache.configuration.CacheConfig;
import org.apache.geode.internal.cache.configuration.CacheElement;
import org.apache.geode.internal.cache.configuration.RegionConfig;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;

@Experimental
public interface ClusterConfigurationService {

  /**
   * retrieves the configuration object of a member group
   *
   * @param group the member group name, if null, then "cluster" is assumed
   * @param additionalBindClass custom element classes if needed
   * @return the configuration object
   */
  CacheConfig getCacheConfig(String group, Class<? extends CacheElement>... additionalBindClass);

  /**
   * update the cluster configuration of a member group
   *
   * @param group the member group name, if null, then "cluster" is assumed
   * @param mutator the change you want to apply to the configuration
   * @param additionalBindClass custom element classes if needed
   */
  void updateCacheConfig(String group, UnaryOperator<CacheConfig> mutator,
      Class<? extends CacheElement>... additionalBindClass);


  default <T extends CacheElement> T getCustomCacheElement(String group, String id,
      Class<T> classT) {
    CacheConfig cacheConfig = getCacheConfig(group, classT);
    return findCustomCacheElement(cacheConfig, id, classT);
  }

  default void saveCustomCacheElement(String group, CacheElement element) {
    updateCacheConfig(group, cacheConfig -> {
      CacheElement foundElement =
          findCustomCacheElement(cacheConfig, element.getId(), element.getClass());
      if (foundElement != null) {
        cacheConfig.getCustomCacheElements().remove(foundElement);
      }
      cacheConfig.getCustomCacheElements().add(element);
      return cacheConfig;
    }, element.getClass());
  }

  default void deleteCustomCacheElement(String group, String id,
      Class<? extends CacheElement> classT) {
    updateCacheConfig(group, config -> {
      CacheElement cacheElement = findCustomCacheElement(config, id, classT);
      if (cacheElement == null) {
        return null;
      }
      config.getCustomCacheElements().remove(cacheElement);
      return config;
    }, classT);
  }

  default <T extends CacheElement> T getCustomRegionElement(String group, String regionPath,
      String id, Class<T> classT) {
    CacheConfig cacheConfig = getCacheConfig(group, classT);
    return findCustomRegionElement(cacheConfig, regionPath, id, classT);
  }

  default void saveCustomRegionElement(String group, String regionPath, CacheElement element) {
    updateCacheConfig(group, cacheConfig -> {
      RegionConfig regionConfig = findRegionConfiguration(cacheConfig, regionPath);
      if (regionConfig == null) {
        throw new EntityNotFoundException(
            String.format("region %s does not exist in group %s", regionPath, group));
      }

      CacheElement oldElement =
          findCustomRegionElement(cacheConfig, regionPath, element.getId(), element.getClass());

      if (oldElement != null) {
        regionConfig.getCustomRegionElements().remove(oldElement);
      }
      regionConfig.getCustomRegionElements().add(element);
      return cacheConfig;
    }, element.getClass());
  }

  default void deleteCustomRegionElement(String group, String regionPath, String id,
      Class<? extends CacheElement> classT) {
    updateCacheConfig(group, cacheConfig -> {
      RegionConfig regionConfig = findRegionConfiguration(cacheConfig, regionPath);
      if (regionConfig == null) {
        throw new EntityNotFoundException(
            String.format("region %s does not exist in group %s", regionPath, group));
      }
      CacheElement element = findCustomRegionElement(cacheConfig, regionPath, id, classT);
      if (element == null) {
        return null;
      }
      regionConfig.getCustomRegionElements().remove(element);
      return cacheConfig;
    }, classT);
  }


  default <T extends Identifiable<String>> T findIdentifiable(List<T> list, String id) {
    return list.stream().filter(o -> o.getId().equals(id)).findFirst().orElse(null);
  }

  default RegionConfig findRegionConfiguration(CacheConfig cacheConfig, String regionPath) {
    return findIdentifiable(cacheConfig.getRegion(), regionPath);
  }

  default <T extends CacheElement> List<T> findCustomCacheElements(CacheConfig cacheConfig,
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

  default <T extends CacheElement> T findCustomCacheElement(CacheConfig cacheConfig,
      String elementId, Class<T> classT) {
    return findIdentifiable(findCustomCacheElements(cacheConfig, classT), elementId);
  }

  default <T extends CacheElement> List<T> findCustomRegionElements(CacheConfig cacheConfig,
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

  default <T extends CacheElement> T findCustomRegionElement(CacheConfig cacheConfig,
      String regionPath, String elementId, Class<T> classT) {
    return findIdentifiable(findCustomRegionElements(cacheConfig, regionPath, classT), elementId);
  }
}
