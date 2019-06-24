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
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.api.RealizationResult;

/**
 * Defines the behavior to realize a configuration change in the cache on a server. Created with an
 * object of type {@link org.apache.geode.cache.configuration.CacheElement}, which represents the
 * configuration change.
 */
@Experimental
public interface ConfigurationRealizer<T extends CacheElement> {
  RealizationResult create(T config, Cache cache);

  boolean exists(T config, Cache cache);

  RealizationResult update(T config, Cache cache);

  RealizationResult delete(T config, Cache cache);
}
