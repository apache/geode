/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gemstone.gemfire.cache.lucene.test;

import java.util.function.Consumer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.RegionListener;

/**
 * Allows spying on operations that happen to an the regions underlying a lucene index.
 */
public class IndexRegionSpy {

  public static void beforeWrite(Cache cache, final Consumer<Object> beforeWrite) {
    GemFireCacheImpl gemfireCache = (GemFireCacheImpl) cache;
    gemfireCache.addRegionListener(new SpyRegionListener(beforeWrite));
  }

  private static class SpyRegionListener implements RegionListener {

    private final Consumer<Object> beforeWrite;

    public SpyRegionListener(final Consumer<Object> beforeWrite) {
      this.beforeWrite = beforeWrite;
    }

    @Override
    public RegionAttributes beforeCreate(final Region parent,
                                         final String regionName,
                                         final RegionAttributes attrs,
                                         final InternalRegionArguments internalRegionArgs)
    {
      return attrs;
    }

    @Override public void afterCreate(final Region region) {
      if(region.getName().contains(".files") || region.getName().contains(".chunks")) {
        region.getAttributesMutator().addCacheListener(new CacheListenerAdapter() {
          @Override public void afterCreate(final EntryEvent event) {
            beforeWrite.accept(event.getKey());
          }

          @Override public void afterDestroy(final EntryEvent event) {
            beforeWrite.accept(event.getKey());
          }

          @Override public void afterInvalidate(final EntryEvent event) {
            beforeWrite.accept(event.getKey());
          }

          @Override public void afterUpdate(final EntryEvent event) {
            beforeWrite.accept(event.getKey());
          }
        });
      }

    }
  }
}
