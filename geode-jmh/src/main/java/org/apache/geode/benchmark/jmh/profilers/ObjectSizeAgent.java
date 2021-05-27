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

package org.apache.geode.benchmark.jmh.profilers;

import static java.lang.reflect.Modifier.isStatic;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ObjectSizeAgent {
  private static volatile Instrumentation instrumentation;

  private static final Map<Class<?>, Field[]> fieldsCache = new ConcurrentHashMap<>();

  public static void premain(final String agentArgs, final Instrumentation instrumentation) {
    ObjectSizeAgent.instrumentation = instrumentation;
  }


  public static void agentmain(final String agentArgs, final Instrumentation instrumentation) {
    ObjectSizeAgent.instrumentation = instrumentation;
  }

  private static Instrumentation getInstrumentation() {
    if (instrumentation == null) {
      throw new IllegalStateException("ObjectSizeAgent not initialized.");
    }
    return instrumentation;
  }

  public static long sizeOf(final Object object) {
    if (null == object) {
      return 0L;
    }

    return getInstrumentation().getObjectSize(object);
  }

  public static long sizeOfDeep(final Object object) {
    if (null == object) {
      return 0L;
    }

    if (isSimpleObject(object)) {
      return sizeOf(object);
    }

    final Deque<Object> unseen = new ArrayDeque<>();
    final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());

    long size = 0L;

    unseen.push(object);
    while (!unseen.isEmpty()) {
      size += sizeOfDeep(unseen.pop(), unseen, seen);
    }

    return size;
  }

  private static boolean isSimpleObject(Object object) {
    final Class<?> clazz = object.getClass();
    return !clazz.isArray() && getFields(clazz).length == 0;
  }

  private static long sizeOfDeep(final Object object, final Deque<Object> unseen,
      final Set<Object> seen) {
    if (!seen.add(object)) {
      return 0L;
    }

    Class<?> clazz = object.getClass();
    if (clazz.isArray()) {
      if (!clazz.getComponentType().isPrimitive()) {
        final int length = Array.getLength(object);
        for (int i = 0; i < length; i++) {
          addIfNotNull(unseen, Array.get(object, i));
        }
      }
    } else {
      try {
        while (null != clazz) {
          final Field[] fields = getFields(clazz);
          for (final Field field : fields) {
            addIfNotNull(unseen, field.get(object));
          }
          clazz = clazz.getSuperclass();
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    return sizeOf(object);
  }

  private static Field[] getFields(final Class<?> clazz) {
    final Field[] fields = fieldsCache.get(clazz);
    if (null != fields) {
      return fields;
    }

    return fieldsCache.computeIfAbsent(clazz, (k) -> {
      final Field[] filtered = getFilteredFields(k.getDeclaredFields());
      Field.setAccessible(filtered, true);
      return filtered;
    });
  }

  private static Field[] getFilteredFields(Field[] fields) {
    int count = getFilteredFieldsCount(fields);
    final Field[] filtered = new Field[count];
    for (final Field field : fields) {
      if (!(isStatic(field.getModifiers()) || field.getType().isPrimitive())) {
        filtered[--count] = field;
      }
    }
    return filtered;
  }

  private static int getFilteredFieldsCount(Field[] fields) {
    int counter = fields.length;
    for (final Field field : fields) {
      if (isStatic(field.getModifiers()) || field.getType().isPrimitive()) {
        counter--;
      }
    }
    return counter;
  }

  private static void addIfNotNull(Collection<Object> collection, final Object object) {
    if (null != object) {
      collection.add(object);
    }
  }
}
