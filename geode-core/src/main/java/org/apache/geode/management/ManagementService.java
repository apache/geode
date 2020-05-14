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
package org.apache.geode.management;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.BaseManagementService;

/**
 * Interface to the GemFire management service for a single Cache.
 *
 * @since GemFire 7.0
 */
public abstract class ManagementService implements IManagementService {

  /**
   * Returns a newly created or the existing instance of the management service for a cache.
   *
   * @param cache Cache for which to get the management service.
   */
  public static ManagementService getManagementService(Cache cache) {
    return BaseManagementService
        .getManagementService(((InternalCache) cache).getCacheForProcessingClientRequests());
  }

  /**
   * Returns the existing instance of the management service for a cache.
   *
   * @param cache Cache for which to get the management service.
   * @return The existing management service if one exists, null otherwise.
   */
  public static ManagementService getExistingManagementService(Cache cache) {
    return BaseManagementService.getExistingManagementService((InternalCache) cache);
  }
}
