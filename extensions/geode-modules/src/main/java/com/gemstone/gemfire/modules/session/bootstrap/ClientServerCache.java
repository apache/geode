/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.gemstone.gemfire.modules.session.bootstrap;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;

/**
 * This is a singleton class which maintains configuration properties as well as starting a Client-Server cache.
 */
public class ClientServerCache extends AbstractCache {

  protected static final String DEFAULT_CACHE_XML_FILE_NAME = "cache-client.xml";

  static {
    instance = new ClientServerCache();
  }

  private ClientServerCache() {
    // Singleton
    super();
  }

  public static AbstractCache getInstance() {
    return instance;
  }

  @Override
  protected void createOrRetrieveCache() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Creating cache");
    }
    // Get the existing cache if any
    try {
      this.cache = ClientCacheFactory.getAnyInstance();
    } catch (CacheClosedException e) {
    }

    // If no cache exists, create one
    String message = null;
    if (this.cache == null) {
      // enable pool subscription so that default cache can be used by hibernate module
      this.cache = new ClientCacheFactory(createDistributedSystemProperties()).create();
      message = "Created ";
    } else {
      message = "Retrieved ";
    }
    getLogger().info(message + this.cache);
  }

  @Override
  protected void rebalanceCache() {
    getLogger().warn("The client cannot rebalance the server's cache.");
  }

  @Override
  protected String getDefaultCacheXmlFileName() {
    return DEFAULT_CACHE_XML_FILE_NAME;
  }
}
