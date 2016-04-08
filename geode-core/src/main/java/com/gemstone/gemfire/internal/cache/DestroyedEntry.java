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

import com.gemstone.gemfire.cache.CacheStatistics;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;

/**
 * Represents a destroyed entry that can be returned from an <code>Iterator</code>
 * instead of null.  All methods throw {@link EntryDestroyedException} except for
 * {@link #isDestroyed()}.
 * 
 */
public class DestroyedEntry implements Region.Entry<Object, Object> {
  private final String msg;
  
  public DestroyedEntry(String msg) {
    this.msg = msg;
  }
  
  @Override
  public Object getKey() {
    throw entryDestroyed();
  }

  @Override
  public Object getValue() {
    throw entryDestroyed();
  }

  @Override
  public Region<Object, Object> getRegion() {
    throw entryDestroyed();
  }

  @Override
  public boolean isLocal() {
    throw entryDestroyed();
  }

  @Override
  public CacheStatistics getStatistics() {
    throw entryDestroyed();
  }

  @Override
  public Object getUserAttribute() {
    throw entryDestroyed();
  }

  @Override
  public Object setUserAttribute(Object userAttribute) {
    throw entryDestroyed();
  }

  @Override
  public boolean isDestroyed() {
    return true;
  }

  @Override
  public Object setValue(Object value) {
    throw entryDestroyed();
  }

  private EntryDestroyedException entryDestroyed() {
    return new EntryDestroyedException(msg);
  }
}
