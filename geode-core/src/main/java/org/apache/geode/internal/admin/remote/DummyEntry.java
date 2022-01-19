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
package org.apache.geode.internal.admin.remote;

import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.Region;

/**
 * This implementation of {@link org.apache.geode.cache.Region.Entry} does nothing but provide an
 * instance of {@link org.apache.geode.cache.CacheStatistics}
 */
public class DummyEntry implements Region.Entry {

  private final Region region;
  private final Object key;
  private final Object value;
  private final CacheStatistics stats;
  private final Object userAttribute;

  DummyEntry(Region region, Object key, Object cachedObject, Object userAttribute,
      CacheStatistics stats) {
    this.region = region;
    this.key = key;
    value = cachedObject;
    this.userAttribute = userAttribute;
    this.stats = stats;
  }

  @Override
  public boolean isLocal() {
    return false;
  }

  @Override
  public Object getKey() {
    return key;
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public Region getRegion() {
    return region;
  }

  @Override
  public CacheStatistics getStatistics() {
    return stats;
  }

  @Override
  public Object getUserAttribute() {
    return userAttribute;
  }

  @Override
  public Object setUserAttribute(Object userAttribute) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDestroyed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object setValue(Object arg0) {
    throw new UnsupportedOperationException();
  }
}
