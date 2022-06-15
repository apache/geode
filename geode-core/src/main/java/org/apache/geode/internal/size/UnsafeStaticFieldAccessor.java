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
 *
 */

package org.apache.geode.internal.size;

import java.lang.reflect.Field;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.unsafe.internal.sun.misc.Unsafe;

/**
 * Accesses the value of a non-primitive static field using {@link Unsafe}, bypassing the
 * {@link Field}'s access and validity checks.
 */
class UnsafeStaticFieldAccessor {
  @Immutable
  static final Unsafe UNSAFE = new Unsafe();
  private final Object base;
  private final long offset;

  /**
   * Creates an accessor for non-primitive static field f.
   *
   * @param f the field
   * @throws UnsupportedOperationException if the field's declaring class is hidden
   */
  UnsafeStaticFieldAccessor(Field f) {
    base = UNSAFE.staticFieldBase(f);
    offset = UNSAFE.staticFieldOffset(f);
  }

  /**
   * Returns the value of the static field.
   *
   * @return the value of the static field
   */
  Object get() {
    return UNSAFE.getObject(base, offset);
  }
}
