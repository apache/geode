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
/*
 * IndexUtils.java
 *
 * Created on March 4, 2005, 5:39 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.cache.query.internal.*;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager.TestHook;

/**
 */
public class IndexUtils  {

  public static final boolean indexesEnabled = System
      .getProperty("query.disableIndexes") == null;
  public static final boolean useOnlyExactIndexs = false;

  public static TestHook testHook;

  public static void setTestHook(TestHook testHook) {
    IndexUtils.testHook = testHook;
  }

  public static IndexManager getIndexManager(Region region,
      boolean createIfNotAvailable) {
    if (region == null || region.isDestroyed()) return null;
    LocalRegion lRegion = (LocalRegion) region;
    IndexManager idxMgr = lRegion.getIndexManager();
    if (idxMgr == null && createIfNotAvailable) {
      // JUst before creating new IndexManager. 
      if (testHook != null && region instanceof PartitionedRegion) {
        testHook.hook(0);
      }
      Object imSync = lRegion.getIMSync();
      synchronized (imSync) {
        //Double checked locking
        if (lRegion.getIndexManager() == null) {
          idxMgr = new IndexManager(region);
          lRegion.setIndexManager(idxMgr);
        }
        else {
          idxMgr = lRegion.getIndexManager();
        }
      }
    }
    return idxMgr;
  }

  public static IndexData findIndex(String regionpath, String defintions[],
      CompiledValue indexedExpression, String projectionAttributes, Cache cache,
      boolean usePrimaryIndex, ExecutionContext context) throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    DefaultQueryService qs = (DefaultQueryService) ((GemFireCacheImpl)cache).getLocalQueryService();
    //IndexProtocol index = null;
    IndexData indxData = null;
    if (usePrimaryIndex) {
      if (useOnlyExactIndexs) {
        indxData = qs.getIndex(regionpath, defintions, IndexType.PRIMARY_KEY,
            indexedExpression, context);
      } else {
        indxData = qs.getBestMatchIndex(regionpath, defintions,
            IndexType.PRIMARY_KEY, indexedExpression, context);
      }
      //If we cannot find a primary key index, we can now look for a hash index
      //because both rely on usePrimaryIndex evaluating to true only if the query
      //is and equality or not equals condition
      if (indxData == null) {
        if (useOnlyExactIndexs) {
          indxData = qs.getIndex(regionpath, defintions, IndexType.HASH,
              indexedExpression, context);
        } else {
          indxData = qs.getBestMatchIndex(regionpath, defintions,
              IndexType.HASH, indexedExpression, context);
        }
      }
    }

      
    //If Primary Key Index not found or is not valid search for FUNCTIONAL
    // Index
    if (indxData == null || !indxData._index.isValid() ) {
      if (useOnlyExactIndexs) {
        indxData = qs.getIndex(regionpath, defintions, IndexType.FUNCTIONAL,
            indexedExpression, context);
      } else {
        indxData = qs.getBestMatchIndex(regionpath, defintions,
            IndexType.FUNCTIONAL, indexedExpression, context);
      }
    }
    else {
      //if exact PRIMARY_KEY Index not found then try to find exact FUNCTIONAL
      // Index
      //if (!fromClause.equals(index.getCanonicalizedFromClause())) {
      if (indxData._matchLevel != 0 ) {
        IndexData functionalIndxData = qs.getIndex(regionpath, defintions,
            IndexType.FUNCTIONAL /* do not use pk index*/, indexedExpression, context);
        //if FUNCTIONAL Index is exact match then use or else use PRIMARY_KEY
        // Index
        //if (functionalIndxInfo != null &&
        // fromClause.equals(functionalIndex.getCanonicalizedFromClause())
        if (functionalIndxData != null && functionalIndxData._index.isValid()) {
          indxData = functionalIndxData;
        }
      }
    }
    return indxData;
  }
  
}
