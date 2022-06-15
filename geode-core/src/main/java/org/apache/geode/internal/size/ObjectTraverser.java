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
import java.util.LinkedList;
import java.util.Map;

import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;

import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;

public class ObjectTraverser {
  private final Map<Class<?>, FieldStacker> fieldStackers =
      new CopyOnWriteWeakHashMap<>();

  /**
   * Visit all objects reachable from a given root object, using a breadth first search. Using this
   * method requires some heap space - probably between 8 - 30 bytes per reachable object.
   *
   * @param root object to traverse from
   * @param visitor a visitor to visit each node
   * @param includeStatics if true, then first time we see a new object type, all of its static
   *        fields will be visited.
   */
  public void breadthFirstSearch(Object root, Visitor visitor, boolean includeStatics) {
    VisitStack stack = new VisitStack(visitor, includeStatics);

    stack.add(null, root);

    while (!stack.isEmpty()) {
      Object next = stack.next();
      doSearch(next, stack);
    }
  }

  private void doSearch(Object object, VisitStack stack) {
    Class<?> clazz = object.getClass();
    if (clazz.isArray()) {
      if (!clazz.getComponentType().isPrimitive()) {
        addArrayElements(object, stack);
      }
      return;
    }
    FieldStacker fieldStacker = fieldStackers.computeIfAbsent(clazz, FieldStacker::new);
    fieldStacker.stackFields(object, stack);
  }

  private static void addArrayElements(Object array, VisitStack stack) {
    int length = Array.getLength(array);
    for (int i = 0; i < length; i++) {
      Object value = Array.get(array, i);
      stack.add(array, value);
    }
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

  static class VisitStack {
    private final ReferenceOpenHashSet<Object> seen = new ReferenceOpenHashSet<>();
    private final LinkedList<Object> stack = new LinkedList<>();
    private final Visitor visitor;
    private final boolean includeStatics;

    VisitStack(Visitor visitor, boolean includeStatics) {
      this.visitor = visitor;
      this.includeStatics = includeStatics;
    }

    void add(Object parent, Object object) {
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

    Object next() {
      return stack.removeFirst();
    }

    boolean isEmpty() {
      return stack.isEmpty();
    }

    boolean shouldIncludeStatics(Class<?> clazz) {
      if (!includeStatics) {
        return false;
      }
      boolean keyExists = seen.contains(clazz);
      seen.add(clazz);
      return !keyExists;
    }
  }
}
