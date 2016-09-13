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
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.cache.*;

/**
 * This implementation of {@link com.gemstone.gemfire.cache.Region.Entry}
 * does nothing but provide an instance of {@link com.gemstone.gemfire.cache.CacheStatistics}
 */
public class DummyEntry implements Region.Entry {
  
  private final Region region;
  private final Object key;
  private final Object value;
  private final CacheStatistics stats;
  private final Object userAttribute;

  DummyEntry(Region region, Object key, Object cachedObject,
             Object userAttribute, CacheStatistics stats) {
    this.region = region;
    this.key = key;
    this.value = cachedObject;
    this.userAttribute = userAttribute;
    this.stats = stats;
  }
  
  public boolean isLocal() {
    return false;
  }

  public Object getKey() {
    return this.key;
  }
  
  public Object getValue() {
    return this.value;
  }
  
  public Region getRegion() {
    return this.region;
  }
  
  public CacheStatistics getStatistics() {
    return this.stats;
  }
  
  public Object getUserAttribute() {
    return this.userAttribute;
  }
  
  public Object setUserAttribute(Object userAttribute) {
    throw new UnsupportedOperationException();
  }
  
  public boolean isDestroyed() {
    throw new UnsupportedOperationException();
  }

  public Object setValue(Object arg0) {
    throw new UnsupportedOperationException();
  }
}
