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
package org.apache.geode.internal.cache.backup;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;

class FlushToDisk {

  private final InternalCache cache;

  FlushToDisk(InternalCache cache) {
    this.cache = cache;
  }

  void run() {
    if (cache != null) {
      for (DiskStore diskStore : cache.listDiskStoresIncludingRegionOwned()) {
        ((DiskStoreImpl) diskStore).lockRVVForAllDiskRegions();
      }
      cache.listDiskStoresIncludingRegionOwned().forEach(DiskStore::flush);
    }
  }
}
