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

import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

/**
 * Utility class that implements all methods in <code>CacheWriter</code>
 * with empty implementations. Applications can subclass this class and
 * only override the methods for the events of interest.
 *
 *
 * @since GemFire 3.0
 */
public class CacheWriterAdapter<K,V> implements CacheWriter<K,V> {

  public void beforeCreate(EntryEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeDestroy(EntryEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeRegionDestroy(RegionEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeRegionClear(RegionEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeUpdate(EntryEvent<K,V> event) throws CacheWriterException {
  }

  public void close() {
  }

}
