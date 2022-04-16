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
package org.apache.geode.internal.size;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;

import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;


public class ObjectTraverser {
  private final Map<Class<?>, Field[]> FIELD_CACHE =
      new CopyOnWriteWeakHashMap<>();
  private final Map<Class<?>, Field[]> STATIC_FIELD_CACHE =
      new CopyOnWriteWeakHashMap<>();
  @Immutable
  private static final Field[] NON_PRIMITIVE_ARRAY = new Field[0];
  @Immutable
  private static final Field[] PRIMITIVE_ARRAY = new Field[0];

  /**
   * Visit all objects reachable from a given root object, using a breadth first search. Using this
   * method requires some heap space - probably between 8 - 30 bytes per reachable object.
   *
   * @param root object to traverse from
   * @param visitor a visitor to visit each node
   * @param includeStatics if true, then first time we see a new object type, all of its static
   *        fields will be visited.
   */
  public void breadthFirstSearch(Object root, Visitor visitor, boolean includeStatics)
      throws IllegalArgumentException, IllegalAccessException {
    VisitStack stack = new VisitStack(visitor, includeStatics);

    stack.add(null, root);
    while (!stack.isEmpty()) {
      Object next = stack.next();
      doSearch(next, stack);
    }

  }

  private void doSearch(Object root, VisitStack stack)
      throws IllegalArgumentException, IllegalAccessException {
    Class<?> clazz = root.getClass();
    boolean includeStatics = stack.shouldIncludeStatics(clazz);
    Field[] nonPrimitiveFields = getNonPrimitiveFields(clazz, includeStatics);

    if (nonPrimitiveFields == NON_PRIMITIVE_ARRAY) {
      int length = Array.getLength(root);
      for (int i = 0; i < length; i++) {
        Object value = Array.get(root, i);
        stack.add(root, value);
      }
      return;
    }

    if (includeStatics) {
      for (Field field : getStaticFields(clazz)) {
        Object value = field.get(root);
        stack.add(root, value);
      }
    }

    for (Field field : nonPrimitiveFields) {
      Object value = field.get(root);
      stack.add(root, value);
    }
  }

  private Field[] getNonPrimitiveFields(Class<?> clazz, boolean includeStatics) {
    Field[] result = FIELD_CACHE.get(clazz);
    if (result == null) {
      cacheFields(clazz, includeStatics);
      result = FIELD_CACHE.get(clazz);
    }
    return result;
  }

  private Field[] getStaticFields(Class<?> clazz) {
    Field[] result = STATIC_FIELD_CACHE.get(clazz);
    if (result == null) {
      cacheFields(clazz, true);
      result = STATIC_FIELD_CACHE.get(clazz);
    }
    return result;
  }

  private void cacheFields(final Class<?> clazz, boolean includeStatics) {
    if (clazz != null && clazz.isArray()) {
      Class<?> componentType = clazz.getComponentType();
      if (componentType.isPrimitive()) {
        FIELD_CACHE.put(clazz, PRIMITIVE_ARRAY);
        STATIC_FIELD_CACHE.put(clazz, PRIMITIVE_ARRAY);
      } else {
        FIELD_CACHE.put(clazz, NON_PRIMITIVE_ARRAY);
        STATIC_FIELD_CACHE.put(clazz, NON_PRIMITIVE_ARRAY);
      }
      return;
    }

    ArrayList<Field> staticFields = new ArrayList<>();
    ArrayList<Field> nonPrimitiveFields = new ArrayList<>();

    Class<?> currentClass = clazz;
    while (currentClass != null) {
      Field[] fields = currentClass.getDeclaredFields();
      for (Field field : fields) {
        Class<?> fieldType = field.getType();
        if (!fieldType.isPrimitive()) {
          if (Modifier.isStatic(field.getModifiers())) {
            if (includeStatics) {
              field.setAccessible(true);
              staticFields.add(field);
            }
          } else {
            field.setAccessible(true);
            nonPrimitiveFields.add(field);
          }
        }
      }

      currentClass = currentClass.getSuperclass();
    }

    FIELD_CACHE.put(clazz, nonPrimitiveFields.toArray(new Field[0]));
    if (includeStatics) {
      STATIC_FIELD_CACHE.put(clazz, staticFields.toArray(new Field[0]));
    }
  }

  Map<Class<?>, Field[]> getStaticFieldCache() {
    return STATIC_FIELD_CACHE;
  }

  public interface Visitor {
    /**
     * Visit an object
     *
     * @param parent the parent of the object
     * @param object the object we are visiting
     * @return true the search should continue on and visit the children of this object as well
     */
    boolean visit(Object parent, Object object);
  }



  private static class VisitStack {
    private final ReferenceOpenHashSet<Object> seen = new ReferenceOpenHashSet<>();
    private final LinkedList<Object> stack = new LinkedList<>();
    private final Visitor visitor;
    private final boolean includeStatics;

    VisitStack(Visitor visitor, boolean includeStatics) {
      this.visitor = visitor;
      this.includeStatics = includeStatics;
    }

    public void add(Object parent, Object object) {
      if (object == null) {
        return;
      }
      boolean newObject = !seen.contains(object);
      if (newObject) {
        seen.add(object);
        boolean visitChildren = visitor.visit(parent, object);
        if (visitChildren) {
          stack.add(object);
        }
      }
    }

    public Object next() {
      return stack.removeFirst();
    }

    public boolean isEmpty() {
      return stack.isEmpty();
    }

    public boolean shouldIncludeStatics(Class<?> clazz) {
      if (!includeStatics) {
        return false;
      }
      boolean keyExists = seen.contains(clazz);
      seen.add(clazz);
      return !keyExists;
    }
  }
}
