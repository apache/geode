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
package org.apache.geode.internal.protocol.protobuf.security;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;

/**
 * Access layer for cache operations that performs the appropriate authorization checks before
 * performing the operation
 */
public interface SecureCache {
  <K, V> void getAll(String regionName, Iterable<K> keys, BiConsumer<K, V> successConsumer,
      BiConsumer<K, Exception> failureConsumer);

  <K, V> V get(String regionName, K key);

  <K, V> void put(String regionName, K key, V value);

  <K, V> void putAll(String regionName, Map<K, V> entries,
      BiConsumer<K, Exception> failureConsumer);

  <K, V> V remove(String regionName, K key);

  Collection<String> getRegionNames();

  int getSize(String regionName);

  <K> Set<K> keySet(String regionName);

  SecureFunctionService getFunctionService();

  void clear(String regionName);

  <K, V> V putIfAbsent(String regionName, K decodedKey, V decodedValue);

  Object query(String query, Object[] bindParameters) throws NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException;
}
