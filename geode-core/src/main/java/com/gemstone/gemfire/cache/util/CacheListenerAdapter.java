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
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

/**
 * <p>Utility class that implements all methods in <code>CacheListener</code>
 * with empty implementations. Applications can subclass this class and only
 * override the methods for the events of interest.<p>
 * 
 * <p>Subclasses declared in a Cache XML file, it must also implement {@link Declarable}
 * </p>
 * 
 * 
 * @since 3.0
 */
public abstract class CacheListenerAdapter<K,V> implements CacheListener<K,V> {

  public void afterCreate(EntryEvent<K,V> event) {
  }

  public void afterDestroy(EntryEvent<K,V> event) {
  }

  public void afterInvalidate(EntryEvent<K,V> event) {
  }

  public void afterRegionDestroy(RegionEvent<K,V> event) {
  }
  
  public void afterRegionCreate(RegionEvent<K,V> event) {
  }
  
  public void afterRegionInvalidate(RegionEvent<K,V> event) {
  }

  public void afterUpdate(EntryEvent<K,V> event) {
  }

  public void afterRegionClear(RegionEvent<K,V> event) {
  }

  public void afterRegionLive(RegionEvent<K,V> event) {
  }
  
  public void close() {
  }
}
