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
package org.apache.geode.cache.query.internal.index;

import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.index.IndexManager.TestHook;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;

public class IndexUtils {

  public static final boolean indexesEnabled = System.getProperty("query.disableIndexes") == null;

  public static final boolean useOnlyExactIndexs = false;

  @MutableForTesting
  public static TestHook testHook;

  public static void setTestHook(TestHook testHook) {
    IndexUtils.testHook = testHook;
  }

  public static IndexManager getIndexManager(InternalCache cache, Region region,
      boolean createIfNotAvailable) {
    if (region == null || region.isDestroyed()) {
      return null;
    }
    InternalRegion lRegion = (InternalRegion) region;
    IndexManager idxMgr = lRegion.getIndexManager();
    if (idxMgr == null && createIfNotAvailable) {
      // JUst before creating new IndexManager.
      if (testHook != null && region instanceof PartitionedRegion) {
        testHook.hook(0);
      }
      Object imSync = lRegion.getIMSync();
      synchronized (imSync) {
        // Double checked locking
        if (lRegion.getIndexManager() == null) {
          idxMgr = new IndexManager(cache, region);
          lRegion.setIndexManager(idxMgr);
        } else {
          idxMgr = lRegion.getIndexManager();
        }
      }
    }
    return idxMgr;
  }

  public static IndexData findIndex(String regionpath, String[] defintions,
      CompiledValue indexedExpression, String projectionAttributes, InternalCache cache,
      boolean usePrimaryIndex, ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {

    DefaultQueryService qs = (DefaultQueryService) cache.getLocalQueryService();
    IndexData indxData = null;
    if (usePrimaryIndex) {
      if (useOnlyExactIndexs) {
        indxData =
            qs.getIndex(regionpath, defintions, IndexType.PRIMARY_KEY, indexedExpression, context);
      } else {
        indxData = qs.getBestMatchIndex(regionpath, defintions, IndexType.PRIMARY_KEY,
            indexedExpression, context);
      }
      // If we cannot find a primary key index, we can now look for a hash index
      // because both rely on usePrimaryIndex evaluating to true only if the query
      // is and equality or not equals condition
      if (indxData == null) {
        if (useOnlyExactIndexs) {
          indxData =
              qs.getIndex(regionpath, defintions, IndexType.HASH, indexedExpression, context);
        } else {
          indxData = qs.getBestMatchIndex(regionpath, defintions, IndexType.HASH, indexedExpression,
              context);
        }
      }
    }

    // If Primary Key Index not found or is not valid search for FUNCTIONAL
    // Index
    if (indxData == null || !indxData._index.isValid()) {
      if (useOnlyExactIndexs) {
        indxData =
            qs.getIndex(regionpath, defintions, IndexType.FUNCTIONAL, indexedExpression, context);
      } else {
        indxData = qs.getBestMatchIndex(regionpath, defintions, IndexType.FUNCTIONAL,
            indexedExpression, context);
      }
    } else {
      // if exact PRIMARY_KEY Index not found then try to find exact FUNCTIONAL Index
      if (indxData._matchLevel != 0) {
        IndexData functionalIndxData = qs.getIndex(regionpath, defintions,
            IndexType.FUNCTIONAL /* do not use pk index */, indexedExpression, context);
        // if FUNCTIONAL Index is exact match then use or else use PRIMARY_KEY Index
        if (functionalIndxData != null && functionalIndxData._index.isValid()) {
          indxData = functionalIndxData;
        }
      }
    }
    return indxData;
  }

}
