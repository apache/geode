/*
 * Copyright 2022 VMware, Inc.
 * https://network.tanzu.vmware.com/legal_documents/vmware_eula
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
