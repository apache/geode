/*
 * Copyright 2022 VMware, Inc.
 * https://network.tanzu.vmware.com/legal_documents/vmware_eula
 */

package org.apache.geode.internal.size;

import java.lang.reflect.Field;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.unsafe.internal.sun.misc.Unsafe;

/**
 * Accesses the value of a non-primitive instance field using {@link Unsafe}, bypassing the
 * {@link Field}'s access and validity checks.
 */
class UnsafeInstanceFieldAccessor {
  @Immutable
  static final Unsafe UNSAFE = new Unsafe();

  private final long offset;

  /**
   * Creates an accessor for non-primitive instance field f.
   *
   * @param f the field
   * @throws UnsupportedOperationException if the field's declaring class is hidden
   */
  UnsafeInstanceFieldAccessor(Field f) {
    offset = UNSAFE.objectFieldOffset(f);
  }

  /**
   * Returns the value of the field for object o.
   *
   * @param o the object in which to access the field value
   * @return the value of the field for object o
   */
  Object get(Object o) {
    return UNSAFE.getObject(o, offset);
  }
}
