/*
 * Copyright 2022 VMware, Inc.
 * https://network.tanzu.vmware.com/legal_documents/vmware_eula
 */

package org.apache.geode.internal.size;

import static java.lang.reflect.Modifier.isStatic;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.internal.size.ObjectTraverser.VisitStack;

class FieldStacker {
  private final List<UnsafeInstanceFieldAccessor> instanceFieldAccessors = new ArrayList<>();
  private final List<UnsafeStaticFieldAccessor> staticFieldAccessors = new ArrayList<>();

  FieldStacker(Class<?> clazz) {
    Class<?> c = clazz;
    do {
      for (Field field : c.getDeclaredFields()) {
        if (!field.getType().isPrimitive()) {
          registerAccessorFor(field);
        }
      }
      c = c.getSuperclass();
    } while (c != null);
  }

  /**
   * Adds the values of object's non-primitive fields to stack. If stack accepts static fields
   * from the object's class, the values of the class's non-primitive static fields are also added.
   *
   * @param object the object whose field values to add to the stack
   * @param stack the stack to which to add the field values
   */
  void stackFields(Object object, VisitStack stack) {
    for (UnsafeInstanceFieldAccessor accessor : instanceFieldAccessors) {
      stack.add(object, accessor.get(object));
    }
    if (stack.shouldIncludeStatics(object.getClass())) {
      for (UnsafeStaticFieldAccessor accessor : staticFieldAccessors) {
        stack.add(object, accessor.get());
      }
    }
  }

  private void registerAccessorFor(Field field) {
    try {
      if (isStatic(field.getModifiers())) {
        staticFieldAccessors.add(new UnsafeStaticFieldAccessor(field));
      } else {
        instanceFieldAccessors.add(new UnsafeInstanceFieldAccessor(field));
      }
    } catch (UnsupportedOperationException ignored) {
      // Java 17+ does not give offsets for fields of lambdas, records, and other hidden classes.
      // Quietly ignore these fields.
    }
  }
}
