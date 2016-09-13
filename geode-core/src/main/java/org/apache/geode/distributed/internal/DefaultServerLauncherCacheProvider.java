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
package org.apache.geode.distributed.internal;

import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.ServerLauncherCacheProvider;
import org.apache.geode.internal.cache.CacheConfig;

public class DefaultServerLauncherCacheProvider
    implements ServerLauncherCacheProvider {

  @Override
  public Cache createCache(Properties gemfireProperties, ServerLauncher serverLauncher) {
    final CacheConfig cacheConfig = serverLauncher.getCacheConfig();
    final CacheFactory cacheFactory = new CacheFactory(gemfireProperties);

    if (cacheConfig.pdxPersistentUserSet) {
      cacheFactory.setPdxPersistent(cacheConfig.isPdxPersistent());
    }

    if (cacheConfig.pdxDiskStoreUserSet) {
      cacheFactory.setPdxDiskStore(cacheConfig.getPdxDiskStore());
    }

    if (cacheConfig.pdxIgnoreUnreadFieldsUserSet) {
      cacheFactory.setPdxIgnoreUnreadFields(cacheConfig.getPdxIgnoreUnreadFields());
    }

    if (cacheConfig.pdxReadSerializedUserSet) {
      cacheFactory.setPdxReadSerialized(cacheConfig.isPdxReadSerialized());
    }

    if (cacheConfig.pdxSerializerUserSet) {
      cacheFactory.setPdxSerializer(cacheConfig.getPdxSerializer());
    }

    return cacheFactory.create();
  }
}
