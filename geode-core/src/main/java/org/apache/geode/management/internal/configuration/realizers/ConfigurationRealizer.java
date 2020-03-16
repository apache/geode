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
package org.apache.geode.management.internal.configuration.realizers;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * Defines the behavior to realize a configuration change in the cache on a server. Created with an
 * object of type {@link CacheElement}, which represents the
 * configuration change.
 */
@Experimental
public interface ConfigurationRealizer<T extends AbstractConfiguration<R>, R extends RuntimeInfo> {
  RealizationResult create(T config, InternalCache cache) throws Exception;

  default boolean exists(T config, InternalCache cache) {
    return get(config, cache) != null;
  }

  R get(T config, InternalCache cache);

  RealizationResult update(T config, InternalCache cache) throws Exception;

  RealizationResult delete(T config, InternalCache cache) throws Exception;

  default boolean isReadyOnly() {
    return false;
  }
}
