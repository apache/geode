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

import java.lang.reflect.Method;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;

/**
 * If this class is modified, the changes should also be applied to TestMethodAuthorizer.txt to keep
 * the two files identical
 */
public class TestMethodAuthorizer implements MethodInvocationAuthorizer {

  private Set<String> parameters;

  public TestMethodAuthorizer(Cache cache, Set<String> parameters) {
    // To allow an exception to be thrown from a mock
    cache.isClosed();
    this.parameters = parameters;
  }

  public Set<String> getParameters() {
    return this.parameters;
  }

  @Override
  public boolean authorize(Method method, Object target) {
    return parameters.contains(method.getName());
  }
}
