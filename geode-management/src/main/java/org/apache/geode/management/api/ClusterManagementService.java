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

package org.apache.geode.management.api;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;

/**
 * Defines the behavior for a management service object, which is responsible for applying and
 * persisting cache configuration changes on locators and/or servers.
 */
@Experimental
public interface ClusterManagementService {

  /**
   * Creates a cache element, represented by a configuration object.
   *
   * @param config representing the change to be configured.
   * @return ClusterManagementResult object representing the result of the creation.
   */
  ClusterManagementResult createCacheElement(CacheElement config);

  /**
   * Deletes an existing element, represented by a configuration object.
   *
   * @param config representing the element to be deleted.
   * @return ClusterManagementResult object representing the result of the deletion.
   */
  ClusterManagementResult deleteCacheElement(CacheElement config);

  /**
   * Updates an existing element, represented by a configuration object.
   *
   * @param config representing the element to be updated.
   * @return ClusterManagementResult object representing the result of the update.
   */
  ClusterManagementResult updateCacheElement(CacheElement config);
}
