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

package org.apache.geode.services.management;

import java.util.Properties;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;

/**
 * Entry point for creating a cache and bootstrapping Geode using the BootstrappingService
 *
 * @since Geode 1.13.0
 */
@Experimental
public interface ManagementService {

  /**
   * Creates a Geode Cache given some configuration.
   *
   * @param properties system properties to use when creating the Cache.
   *
   * @throws CacheXmlException If a problem occurs while parsing the declarative caching XML file.
   * @throws TimeoutException If a {@link Region#put(Object, Object)} times out while initializing
   *         the cache.
   * @throws CacheWriterException If a {@code CacheWriterException} is thrown while initializing the
   *         cache.
   * @throws GatewayException If a {@code GatewayException} is thrown while initializing the cache.
   * @throws RegionExistsException If the declarative caching XML file describes a region that
   *         already exists
   *         (including the root region).
   * @throws IllegalStateException if cache already exists and is not compatible with the new
   *         configuration.
   * @throws AuthenticationFailedException if authentication fails.
   * @throws AuthenticationRequiredException if the distributed system is in secure mode and this
   *         new member is not
   *         configured with security credentials.
   */
  Cache createCache(Properties properties)
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException;
}
