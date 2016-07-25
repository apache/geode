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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DynamicRegionFactory;

/** This class provides non-published methods that allow the cache
    to initialize and close the factory.

    @since GemFire 4.3
 */
public class DynamicRegionFactoryImpl extends DynamicRegionFactory {
  /** create an instance of the factory.  This is normally only done
      by DynamicRegionFactory's static initialization
   */
  public DynamicRegionFactoryImpl() {
  }
  
  /** close the factory.  Only do this if you're closing the cache, too */
  public void close() {
    _close();
  }
  
  /** initialize the factory for use with a new cache */
  public void internalInit( GemFireCacheImpl c ) throws CacheException {
    _internalInit(c);
  }
}
