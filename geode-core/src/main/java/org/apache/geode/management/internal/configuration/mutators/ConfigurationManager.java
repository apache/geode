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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.api.RespondsWith;

/**
 * Defines the behavior to mutate a configuration change into a pre-existing cache config from a
 * locator
 * {@link org.apache.geode.distributed.ConfigurationPersistenceService}. Created with an object of
 * type {@link CacheElement}, which represents the configuration change.
 */
@Experimental
public interface ConfigurationManager<T extends CacheElement & RespondsWith<R>, R extends CacheElement> {
  /**
   * specify how to add the config to the existing cache config. Note at this point, the config
   * should have passed all the validations already.
   */
  void add(T config, CacheConfig existing);

  void update(T config, CacheConfig existing);

  void delete(T config, CacheConfig existing);

  List<R> list(T filterConfig, CacheConfig existing);

  T get(String id, CacheConfig existing);

  /**
   * @param incoming the one that's about to be persisted
   * @param group the group name of the existing cache element
   * @param existing the existing cache element on another group
   * @throws IllegalArgumentException if the incoming CacheElement is not compatible with the
   *         existing
   *
   *         Note: incoming and exiting should have the same ID already
   */
  default void checkCompatibility(T incoming, String group, T existing) {};
}
