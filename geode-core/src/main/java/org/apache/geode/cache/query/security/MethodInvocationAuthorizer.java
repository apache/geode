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

import org.apache.geode.cache.query.internal.AttributeDescriptor;
import org.apache.geode.cache.query.internal.MethodDispatch;

/**
 * The root interface that should be implemented by method invocation authorizer instances.
 * The authorizer is responsible for determining whether a {@link java.lang.reflect.Method} is
 * allowed to be executed on a specific {@link java.lang.Object} instance.
 * <p/>
 *
 * Implementations of this interface must be thread-safe: multiple threads might be authorizing
 * several method invocations using the same instance at the same time.
 * <p/>
 *
 * @see org.apache.geode.cache.query.internal.MethodDispatch
 * @see org.apache.geode.cache.query.internal.AttributeDescriptor
 */
public interface MethodInvocationAuthorizer {

  /**
   * Executes the authorization logic to determine whether the {@code method} is allowed to be
   * executed on the {@code target} object instance.
   * <p/>
   *
   * <b>Implementation Note</b>: both the {@link MethodDispatch} and {@link AttributeDescriptor}
   * classes will remember whether the method invocation is already authorized, so that
   * {@code authorize} will be called once in the lifetime of a Geode member for every new method
   * seen while traversing the objects.
   * Nevertheless, the implementation should be lighting fast as it will be called by the OQL engine
   * in runtime during the query execution.
   *
   * @param method the {@link Method} that should be authorized.
   * @param target the {@link Object} on which the {@link Method} will be executed.
   * @return {@code true} if the {@code method} can be executed on on the {@code target} instance,
   *         {@code false} otherwise.
   */
  boolean authorize(Method method, Object target);
}
