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

package org.apache.geode.management.internal.cli;

import org.apache.geode.cache.configuration.CacheConfig;

/**
 * A marker for {@link org.apache.geode.management.cli.SingleGfshCommand} classes that should
 * iterate over multiple {@link org.apache.geode.cache.configuration.CacheConfig} objects
 * when updating the {@link org.apache.geode.distributed.ConfigurationPersistenceService}.
 */

public interface UpdateAllConfigurationsByDefault {
  /**
   * A filter to determine which {@link org.apache.geode.cache.configuration.CacheConfig} objects
   * should be updated by
   * {@link org.apache.geode.management.cli.SingleGfshCommand#updateClusterConfig(String, org.apache.geode.cache.configuration.CacheConfig, Object)}.
   *
   * Typical use will be to examine the configuration to verify the change that will be applied by
   * {@link org.apache.geode.management.cli.SingleGfshCommand#updateClusterConfig(String, org.apache.geode.cache.configuration.CacheConfig, Object)}
   * will be valid.
   *
   * @param group Name of the group for the given CacheConfig.
   * @param config The persisted configuration object. Can be null.
   * @param configObject The configuration object set by the gfsh command
   * @return true if this configuration should be updated.
   */
  boolean configurationFilter(String group, CacheConfig config, Object configObject);
}
