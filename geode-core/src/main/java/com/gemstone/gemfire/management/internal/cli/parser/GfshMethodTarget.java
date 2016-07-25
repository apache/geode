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
package com.gemstone.gemfire.management.internal.cli.parser;

import java.lang.reflect.Method;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * A method that can be executed via a shell command.
 * 
 * @since GemFire 7.0
 */
public class GfshMethodTarget {

  // Fields
  private final Method method;
  private final Object target;
  private final String remainingBuffer;
  private final String key;

  /**
   * Constructor for a <code>null remainingBuffer</code> and <code>key</code>
   * 
   * @param method
   *          the method to invoke (required)
   * @param target
   *          the object on which the method is to be invoked (required)
   */
  public GfshMethodTarget(final Method method, final Object target) {
    this(method, target, null, null);
  }

  /**
   * Constructor that allows all fields to be set
   * 
   * @param method
   *          the method to invoke (required)
   * @param target
   *          the object on which the method is to be invoked (required)
   * @param remainingBuffer
   *          can be blank
   * @param key
   *          can be blank
   */
  public GfshMethodTarget(final Method method, final Object target,
      final String remainingBuffer, final String key) {
    Assert.notNull(method, "Method is required");
    Assert.notNull(target, "Target is required");
    this.key = StringUtils.trimToEmpty(key);
    this.method = method;
    this.remainingBuffer = remainingBuffer;
    this.target = target;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (getClass() != other.getClass()) {
      return false;
    }
    GfshMethodTarget gfshMethodTarget = (GfshMethodTarget) other;
    if (method == null) {
      if (gfshMethodTarget.getMethod() != null) {
        return false;
      }
    } else if (!method.equals(gfshMethodTarget.getMethod())) {
      return false;
    }
    if (target == null) {
      if (gfshMethodTarget.getTarget() != null) {
        return false;
      }
    } else if (!target.equals(gfshMethodTarget.getTarget())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return ObjectUtils.nullSafeHashCode(new Object[] { method, target });
  }

  @Override
  public final String toString() {
    StringBuilder builder = new StringBuilder();
    builder
        .append(GfshMethodTarget.class.getSimpleName())
        .append("[key=" + key)
        .append(
            ",remainingBuffer=" + remainingBuffer + ",target=" + target
                + ",method=" + method + "]");
    return builder.toString();
  }

  public String getKey() {
    return this.key;
  }

  public Method getMethod() {
    return this.method;
  }

  public String getRemainingBuffer() {
    return this.remainingBuffer;
  }

  public Object getTarget() {
    return this.target;
  }
}
