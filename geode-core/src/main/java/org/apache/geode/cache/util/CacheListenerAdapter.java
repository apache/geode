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
package org.apache.geode.cache.util;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;

/**
 * <p>
 * Utility class that implements all methods in <code>CacheListener</code> with empty
 * implementations. Applications can subclass this class and only override the methods for the
 * events of interest.
 * <p>
 *
 * <p>
 * Subclasses declared in a Cache XML file, it must also implement {@link Declarable}
 * </p>
 *
 *
 * @since GemFire 3.0
 */
public abstract class CacheListenerAdapter<K, V> implements CacheListener<K, V> {

  @Override
  public void afterCreate(EntryEvent<K, V> event) {}

  @Override
  public void afterDestroy(EntryEvent<K, V> event) {}

  @Override
  public void afterInvalidate(EntryEvent<K, V> event) {}

  @Override
  public void afterRegionDestroy(RegionEvent<K, V> event) {}

  @Override
  public void afterRegionCreate(RegionEvent<K, V> event) {}

  @Override
  public void afterRegionInvalidate(RegionEvent<K, V> event) {}

  @Override
  public void afterUpdate(EntryEvent<K, V> event) {}

  @Override
  public void afterRegionClear(RegionEvent<K, V> event) {}

  @Override
  public void afterRegionLive(RegionEvent<K, V> event) {}

  @Override
  public void close() {}
}
