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
   * @throws Exception is Cache cannot be created.
   */
  Cache createCache(Properties properties) throws Exception;
}
