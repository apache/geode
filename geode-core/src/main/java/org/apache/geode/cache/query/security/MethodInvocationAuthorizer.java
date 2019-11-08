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
package org.apache.geode.cache.query.security;

import java.lang.reflect.Method;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;

/**
 * The root interface that should be implemented by method invocation authorizer instances.
 * The authorizer is responsible for determining whether a {@link java.lang.reflect.Method} is
 * allowed to be executed on a specific {@link java.lang.Object} instance.
 * <p/>
 *
 * There are mainly four security risks when allowing users to execute arbitrary methods in OQL,
 * which should be addressed by implementations of this interface:
 * <p>
 * <ul>
 * <li>{@code Java Reflection}: do anything through {@link Object#getClass()} or similar.
 * <li>{@code Cache Modification}: execute {@link Cache} operations (close, get regions, etc.).
 * <li>{@code Region Modification}: execute {@link Region} operations (destroy, invalidate, etc.).
 * <li>{@code Region Entry Modification}: execute in-place modifications on the region entries.
 * </ul>
 * </p>
 *
 * Implementations of this interface should be thread-safe: multiple threads might be authorizing
 * several method invocations using the same instance at the same time.
 */
public interface MethodInvocationAuthorizer {

  /**
   * Executes the authorization logic to determine whether the {@code method} is allowed to be
   * executed on the {@code target} object instance.
   * <p/>
   *
   * <b>Implementation Note</b>: the query engine will remember whether the method invocation has
   * been already authorized or not for the current query context, so this method will be called
   * once in the lifetime of a query for every new method seen while traversing the objects.
   * Nevertheless, the implementation should be lighting fast as it will be called by the
   * OQL engine in runtime during the query execution.
   *
   * @param method the {@link Method} that should be authorized.
   * @param target the {@link Object} on which the {@link Method} will be executed.
   * @return {@code true} if the {@code method} can be executed on on the {@code target} instance,
   *         {@code false} otherwise.
   */
  boolean authorize(Method method, Object target);

  void initialize(Cache cache, Set<String> parameters);
}
