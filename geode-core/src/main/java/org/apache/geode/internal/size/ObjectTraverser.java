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
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;


public class ObjectTraverser {
  @MakeNotStatic
  private static final Map<Class, FieldSet> FIELD_CACHE =
      new CopyOnWriteWeakHashMap<>();
  @Immutable
  private static final FieldSet NON_PRIMATIVE_ARRAY = new FieldSet(null, null);

  /**
   * Visit all objects reachable from a given root object, using a breadth first search. Using this
   * method requires some heap space - probably between 8 - 30 bytes per reachable object.
   *
   * @param root object to traverse from
   * @param visitor a visitor to visit each node
   * @param includeStatics if true, the first time we see a new object type, we will visit all of
   *        the static fields.
   */
  public static void breadthFirstSearch(Object root, Visitor visitor, boolean includeStatics)
      throws IllegalArgumentException, IllegalAccessException {
    VisitStack stack = new VisitStack(visitor, includeStatics);

    stack.add(null, root);
    while (!stack.isEmpty()) {
      Object next = stack.next();
      doSearch(next, stack);
    }

  }

  private static void doSearch(Object root, VisitStack stack)
      throws IllegalArgumentException, IllegalAccessException {
    Class clazz = root.getClass();
    boolean includeStatics = stack.shouldIncludeStatics(clazz);
    FieldSet set = FIELD_CACHE.get(clazz);
    if (set == null) {
      set = cacheFieldSet(clazz);
    }

    if (set == NON_PRIMATIVE_ARRAY) {
      Class componentType = clazz.getComponentType();
      int length = Array.getLength(root);
      for (int i = 0; i < length; i++) {
        Object value = Array.get(root, i);
        stack.add(root, value);
      }
      return;
    }

    if (includeStatics) {
      for (Field field : set.getStaticFields()) {
        Object value = field.get(root);
        stack.add(root, value);
      }
    }

    for (Field field : set.getNonPrimativeFields()) {
      Object value = field.get(root);
      stack.add(root, value);
    }
  }

  private static FieldSet cacheFieldSet(Class clazz) {
    FieldSet set = buildFieldSet(clazz);
    FIELD_CACHE.put(clazz, set);
    return set;
  }

  private static FieldSet buildFieldSet(Class clazz) {
    ArrayList<Field> staticFields = new ArrayList<>();
    ArrayList<Field> nonPrimativeFields = new ArrayList<>();

    while (clazz != null) {
      if (clazz.isArray()) {
        Class componentType = clazz.getComponentType();
        if (!componentType.isPrimitive()) {
          return NON_PRIMATIVE_ARRAY;
        } else {
          return new FieldSet(new Field[0], new Field[0]);
        }
      }

      Field[] fields = clazz.getDeclaredFields();
      for (int i = 0; i < fields.length; i++) {
        Field field = fields[i];
        Class fieldType = field.getType();
        // skip static fields if we've already counted them once
        if (!fieldType.isPrimitive()) {
          field.setAccessible(true);
          if (Modifier.isStatic(field.getModifiers())) {
            staticFields.add(field);
          } else {
            nonPrimativeFields.add(field);
          }
        }
      }

      clazz = clazz.getSuperclass();
    }

    return new FieldSet(staticFields.toArray(new Field[0]),
        nonPrimativeFields.toArray(new Field[0]));
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
    private final ReferenceOpenHashSet seen = new ReferenceOpenHashSet();
    private final LinkedList stack = new LinkedList();
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

    public boolean shouldIncludeStatics(Class clazz) {
      if (!includeStatics) {
        return false;
      }
      boolean keyExists = seen.contains(clazz);
      seen.add(clazz);
      return !keyExists;
    }
  }

  private ObjectTraverser() {

  }

  private static class FieldSet {
    private final Field[] staticFields;
    private final Field[] nonPrimativeFields;

    public FieldSet(Field[] staticFields, Field[] nonPrimativeFields) {
      this.staticFields = staticFields;
      this.nonPrimativeFields = nonPrimativeFields;
    }

    public Field[] getStaticFields() {
      return staticFields;
    }

    public Field[] getNonPrimativeFields() {
      return nonPrimativeFields;
    }


  }
}
