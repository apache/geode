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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.Cache;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

/**
 * Interface for a service that is linked to a cache.
 * 
 * These services are loaded during cache initialization using the java ServiceLoader and can be
 * retrieved from the cache by calling Cache.getService(YourInterface.class)
 */
public interface CacheService {
  /**
   * Initialize the service with a cache.
   * 
   * Services are initialized in random order, fairly early on in cache initialization. In
   * particular, the cache.xml has not yet been parsed.
   */
  public void init(Cache cache);

  /**
   * Return the class or interface used to look up this service.
   */
  public Class<? extends CacheService> getInterface();

  /**
   * Returns the MBean associated with this server
   * 
   * @return the MBean associated with this server
   */
  public CacheServiceMBeanBase getMBean();
}
