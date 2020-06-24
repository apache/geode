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
package org.apache.geode.services.management.impl;

import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.services.management.ManagementService;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ModuleServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

public class ManagementServiceImpl implements ManagementService {
  private ModuleService moduleService;

  @Override
  public void init(ModuleService moduleService) {
    this.moduleService = moduleService;
  }

  @Override
  public ModuleServiceResult<Boolean> createCache(Properties properties) {
    try {
      Cache cache = new CacheFactory(properties)
          .setModuleService(moduleService)
          .create();

      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setPort(0);

      cacheServer.start();

      Region<Object, Object> test =
          cache.createRegionFactory(RegionShortcut.PARTITION).create("Test");

      return Success.of(true);
    } catch (Exception e) {
      e.printStackTrace();
      return Failure.of(e.getMessage());
    }
  }
}
