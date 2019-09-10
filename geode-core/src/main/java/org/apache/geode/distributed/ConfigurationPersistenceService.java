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

import java.util.Set;
import java.util.function.UnaryOperator;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.management.configuration.AbstractConfiguration;

@Experimental
public interface ConfigurationPersistenceService {
  String CLUSTER_CONFIG = AbstractConfiguration.CLUSTER;

  /**
   * retrieves all the group names in the cluster
   */
  Set<String> getGroups();

  /**
   * retrieves the configuration object of a member group
   *
   * @param group the member group name, if null, then "cluster" is assumed
   * @return the configuration object
   */
  CacheConfig getCacheConfig(String group);

  CacheConfig getCacheConfig(String group, boolean createNew);

  /**
   * update the cluster configuration of a member group
   *
   * @param group the member group name, if null, then "cluster" is assumed
   * @param mutator the change you want to apply to the configuration. mutator returns null
   *        indicating that no update
   *        is done to the CacheConfig, or it returns the updated CacheConfig
   */
  void updateCacheConfig(String group, UnaryOperator<CacheConfig> mutator);
}
