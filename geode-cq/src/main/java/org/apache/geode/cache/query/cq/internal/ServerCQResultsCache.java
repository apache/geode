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
package org.apache.geode.cache.query.cq.internal;

import java.util.Set;

/**
 * Holds the keys that are part of the CQ query results.
 * Using this, the CQ engine can determine whether to execute query on an old value from EntryEvent
 * or not,which is an expensive operation.
 */
interface ServerCQResultsCache {
  Object TOKEN = new Object();

  void setInitialized();

  boolean isInitialized();

  void add(Object key);

  void remove(Object key, boolean isTokenMode);

  void invalidate();

  boolean contains(Object key);

  void markAsDestroyed(Object key);

  int size();

  Set<Object> getKeys();

  boolean isOldValueRequiredForQueryProcessing(Object key);

  void clear();
}
