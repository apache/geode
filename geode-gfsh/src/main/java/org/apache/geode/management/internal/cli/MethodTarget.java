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
package org.apache.geode.management.internal.cli;

import java.lang.reflect.Method;

/**
 * Represents a method and its target object for command execution.
 * Replaces org.springframework.shell.core.MethodTarget from Shell 1.x.
 *
 * @since GemFire 7.0
 */
public class MethodTarget {
  private final Method method;
  private final Object target;

  public MethodTarget(Method method, Object target) {
    this.method = method;
    this.target = target;
  }

  public Method getMethod() {
    return method;
  }

  public Object getTarget() {
    return target;
  }

  @Override
  public String toString() {
    return "MethodTarget{" +
        "method=" + method.getName() +
        ", target=" + target.getClass().getSimpleName() +
        '}';
  }
}
