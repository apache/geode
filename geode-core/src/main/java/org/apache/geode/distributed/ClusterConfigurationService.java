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

import java.util.List;
import java.util.function.UnaryOperator;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.lang.Identifiable;

@Experimental
public interface ClusterConfigurationService {
  /**
   * if you want the output xml have schemaLocation correctly set for your namespace, use this
   * instead of registerBindClass(Class)./
   *
   * @param clazz e.g. CacheConfig.class
   * @param nameSpaceAndLocation e.g. "http://geode.apache.org/schema/cache
   *        http://geode.apache.org/schema/cache/cache-1.0.xsd"
   */
  void registerBindClassWithSchema(Class clazz, String nameSpaceAndLocation);

  default void registerBindClass(Class clazz) {
    registerBindClassWithSchema(clazz, null);
  }

  /**
   * retrieves the configuration object of a member group
   *
   * @param group the member group name, if null, then "cluster" is assumed
   * @return the configuration object
   */
  CacheConfig getCacheConfig(String group);

  /**
   * update the cluster configuration of a member group
   *
   * @param group the member group name, if null, then "cluster" is assumed
   * @param mutator the change you want to apply to the configuration
   */
  void updateCacheConfig(String group, UnaryOperator<CacheConfig> mutator);


  <T extends CacheElement> T getCustomCacheElement(String group, String id, Class<T> classT);

  void saveCustomCacheElement(String group, CacheElement element);

  void deleteCustomCacheElement(String group, String id, Class<? extends CacheElement> classT);

  <T extends CacheElement> T getCustomRegionElement(String group, String regionPath, String id,
      Class<T> classT);

  void saveCustomRegionElement(String group, String regionPath, CacheElement element);

  void deleteCustomRegionElement(String group, String regionPath, String id,
      Class<? extends CacheElement> classT);

  <T extends Identifiable<String>> T findIdentifiable(List<T> list, String id);

  <T extends Identifiable<String>> void removeFromList(List<T> list, String id);

  RegionConfig findRegionConfiguration(CacheConfig cacheConfig, String regionPath);

  <T extends CacheElement> List<T> findCustomCacheElements(CacheConfig cacheConfig,
      Class<T> classT);

  <T extends CacheElement> T findCustomCacheElement(CacheConfig cacheConfig, String elementId,
      Class<T> classT);

  <T extends CacheElement> List<T> findCustomRegionElements(CacheConfig cacheConfig,
      String regionPath, Class<T> classT);

  <T extends CacheElement> T findCustomRegionElement(CacheConfig cacheConfig, String regionPath,
      String elementId, Class<T> classT);
}
