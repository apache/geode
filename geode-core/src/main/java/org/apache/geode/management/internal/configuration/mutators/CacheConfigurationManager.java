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

package org.apache.geode.management.internal.configuration.mutators;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.AbstractConfiguration;

/**
 * Defines the behavior to mutate a configuration change into a pre-existing cache config
 */
@Experimental
public abstract class CacheConfigurationManager<T extends AbstractConfiguration>
    implements ConfigurationManager<T> {
  private static final Logger logger = LogService.getLogger();

  /**
   * specify how to add the config to the existing cache config. Note at this point, the config
   * should have passed all the validations already.
   */
  public abstract void add(T config, CacheConfig existing);

  public abstract void update(T config, CacheConfig existing);

  public abstract void delete(T config, CacheConfig existing);

  public abstract List<T> list(T filterConfig, CacheConfig existing);

  public abstract T get(String id, CacheConfig existing);

  /**
   * @param incoming the one that's about to be persisted
   * @param group the group name of the existing cache element
   * @param existing the existing cache element on another group
   * @throws IllegalArgumentException if the incoming CacheElement is not compatible with the
   *         existing
   *
   *         Note: incoming and existing should have the same ID already
   */
  public void checkCompatibility(T incoming, String group, T existing) {};

  public final boolean add(InternalConfigurationPersistenceService service, T config,
      String groupName) {
    return updateCacheConfig(service, config, groupName, this::add);
  }

  public final boolean delete(InternalConfigurationPersistenceService service, T config,
      String groupName) {
    return updateCacheConfig(service, config, groupName, this::delete);
  }

  public final boolean update(InternalConfigurationPersistenceService service, T config,
      String groupName) {
    return updateCacheConfig(service, config, groupName, this::update);
  }

  public final List<T> list(InternalConfigurationPersistenceService service, T filterConfig,
      String groupName) {
    CacheConfig currentPersistedConfig =
        service.getCacheConfig(
            AbstractConfiguration.isCluster(groupName) ? AbstractConfiguration.CLUSTER : groupName,
            true);
    return list(filterConfig, currentPersistedConfig);
  }

  boolean updateCacheConfig(InternalConfigurationPersistenceService service, T config,
      String groupName, BiConsumer<T, CacheConfig> consumer) {
    AtomicBoolean success = new AtomicBoolean(true);
    service.updateCacheConfig(groupName, cacheConfigForGroup -> {
      try {
        consumer.accept(config, cacheConfigForGroup);
      } catch (Exception e) {
        String message = "Failed to update cluster configuration for " + groupName + ".";
        logger.error(message, e);
        success.set(false);
        // returning null indicating no changes needs to be persisted.
        return null;
      }
      return cacheConfigForGroup;
    });
    return success.get();
  }

}
