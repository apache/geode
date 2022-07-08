// Copyright (c) VMware, Inc. 2022. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.apache.geode.internal.size.ObjectTraverser.Visitor;
import org.apache.geode.util.internal.GeodeGlossary;

class ObjectTraverserTest {
  @Test
  void doesNotVisitPrimitiveInstanceFields() {
    Object root = new ObjectWithOnlyPrimitiveFields();

    List<Object> visits = new ArrayList<>();
    Visitor visitor = (parent, object) -> visits.add(object);

    boolean includeStatics = false;
    new ObjectTraverser().breadthFirstSearch(root, visitor, includeStatics);

    assertThat(visits)
        .containsExactly(root);
  }

  @Test
  void doesNotVisitPrimitiveStaticFields() {
    Object root = new ObjectWithOnlyPrimitiveFields();

    List<Object> visits = new ArrayList<>();
    Visitor visitor = (p, o) -> visits.add(o);

    boolean includeStatics = true;
    new ObjectTraverser().breadthFirstSearch(root, visitor, includeStatics);

    assertThat(visits)
        .containsExactly(root);
  }

  @Test
  void visitsEachReferenceInstanceField() {
    Object instanceField1 = new Object();
    Object instanceField2 = new Object();
    ObjectWithReferenceFields root = new ObjectWithReferenceFields(instanceField1, instanceField2);
    Object staticField1 = new Object();
    Object staticField2 = new Object();
    ObjectWithReferenceFields.staticField1 = staticField1;
    ObjectWithReferenceFields.staticField2 = staticField2;

    List<Object> visits = new ArrayList<>();
    Visitor visitor = (parent, object) -> visits.add(object);

    boolean includeStatics = false;
    new ObjectTraverser().breadthFirstSearch(root, visitor, includeStatics);

    assertThat(visits)
        .containsExactlyInAnyOrder(root, instanceField1, instanceField2)
        .doesNotContain(staticField1, staticField2); // because includeStatics = false;
  }

  @Test
  void visitsEachReferenceStaticField() {
    Object instanceField1 = new Object();
    Object instanceField2 = new Object();
    ObjectWithReferenceFields root = new ObjectWithReferenceFields(instanceField1, instanceField2);
    Object staticField1 = new Object();
    Object staticField2 = new Object();
    ObjectWithReferenceFields.staticField1 = staticField1;
    ObjectWithReferenceFields.staticField2 = staticField2;

    List<Object> visits = new ArrayList<>();
    Visitor visitor = (parent, object) -> visits.add(object);

    boolean includeStatics = true;
    new ObjectTraverser().breadthFirstSearch(root, visitor, includeStatics);

    assertThat(visits)
        .containsExactlyInAnyOrder(
            root, instanceField1, instanceField2,
            staticField1, staticField2); // because includeStatics = false;
  }

  @Test
  void visitsEachPrimitiveArrayInstanceField() {
    boolean[] instanceBooleanArray = {true, false, false, true};
    int[] instanceIntArray = {1, 2, 3, 4};
    boolean[] staticBooleanArray = {false, false, true, true};
    int[] staticIntArray = {-1, -2, -3, -4};

    Object root = new ObjectWithReferenceFields(instanceIntArray, instanceBooleanArray);
    ObjectWithReferenceFields.staticField1 = staticBooleanArray;
    ObjectWithReferenceFields.staticField2 = staticIntArray;

    List<Object> visits = new ArrayList<>();
    Visitor visitor = (parent, object) -> visits.add(object);

    boolean includeStatics = false;
    new ObjectTraverser().breadthFirstSearch(root, visitor, includeStatics);

    assertThat(visits)
        .containsExactlyInAnyOrder(root, instanceBooleanArray, instanceIntArray)
        .doesNotContain(staticBooleanArray, staticIntArray); // because includeStatics = false;
  }

  @Test
  void visitsEachPrimitiveArrayStaticField() {
    boolean[] instanceBooleanArray = {true, false, false, true};
    int[] instanceIntArray = {1, 2, 3, 4};
    boolean[] staticBooleanArray = {false, false, true, true};
    int[] staticIntArray = {-1, -2, -3, -4};

    Object root = new ObjectWithReferenceFields(instanceIntArray, instanceBooleanArray);
    ObjectWithReferenceFields.staticField1 = staticBooleanArray;
    ObjectWithReferenceFields.staticField2 = staticIntArray;

    List<Object> visits = new ArrayList<>();
    Visitor visitor = (parent, object) -> visits.add(object);

    boolean includeStatics = true;
    new ObjectTraverser().breadthFirstSearch(root, visitor, includeStatics);

    assertThat(visits).containsExactlyInAnyOrder(
        root, instanceBooleanArray, instanceIntArray,
        staticBooleanArray, staticIntArray);
  }

  @Test
  void visitsEachReferenceArrayInstanceFieldAndTheirElements() {
    Object[] instanceField1 = {new Object()};
    Object[] instanceField2 = {new Object(), new Object()};
    Object[] staticField1 = {new Object(), new Object(), new Object()};
    Object[] staticField2 = {new Object(), new Object(), new Object(), new Object()};

    Object root = new ObjectWithReferenceFields(instanceField2, instanceField1);
    ObjectWithReferenceFields.staticField1 = staticField1;
    ObjectWithReferenceFields.staticField2 = staticField2;

    List<Object> visits = new ArrayList<>();
    Visitor visitor = (parent, object) -> visits.add(object);

    boolean includeStatics = false; // Do not visit static fields or their elements.

    new ObjectTraverser().breadthFirstSearch(root, visitor, includeStatics);

    Set<Object> expectedVisits = new HashSet<>();
    expectedVisits.add(root);
    expectedVisits.add(instanceField1);
    expectedVisits.add(instanceField2);
    Collections.addAll(expectedVisits, instanceField1);
    Collections.addAll(expectedVisits, instanceField2);

    Set<Object> staticFieldsAndTheirElements = new HashSet<>();
    staticFieldsAndTheirElements.add(staticField1);
    staticFieldsAndTheirElements.add(staticField2);
    Collections.addAll(staticFieldsAndTheirElements, staticField1);
    Collections.addAll(staticFieldsAndTheirElements, staticField2);

    assertThat(visits)
        .containsExactlyInAnyOrderElementsOf(expectedVisits)
        .doesNotContainAnyElementsOf(staticFieldsAndTheirElements);
  }

  @Test
  void visitsEachReferenceArrayStaticFieldAndTheirElements() {
    Object[] instanceField1 = {new Object()};
    Object[] instanceField2 = {new Object(), new Object()};
    Object[] staticField1 = {new Object(), new Object(), new Object()};
    Object[] staticField2 = {new Object(), new Object(), new Object(), new Object()};

    Object root = new ObjectWithReferenceFields(instanceField2, instanceField1);
    ObjectWithReferenceFields.staticField1 = staticField1;
    ObjectWithReferenceFields.staticField2 = staticField2;

    List<Object> visits = new ArrayList<>();
    Visitor visitor = (parent, object) -> visits.add(object);

    boolean includeStatics = true; // Visit static fields and their elements
    new ObjectTraverser().breadthFirstSearch(root, visitor, includeStatics);

    Set<Object> expectedVisits = new HashSet<>();
    expectedVisits.add(root);

    // instance fields and their elements
    expectedVisits.add(instanceField1);
    expectedVisits.add(instanceField2);
    Collections.addAll(expectedVisits, instanceField1);
    Collections.addAll(expectedVisits, instanceField2);

    // static fields and their elements
    expectedVisits.add(staticField1);
    expectedVisits.add(staticField2);
    Collections.addAll(expectedVisits, staticField1);
    Collections.addAll(expectedVisits, staticField2);

    assertThat(visits)
        .containsExactlyInAnyOrderElementsOf(expectedVisits);
  }

  @Test
  public void visitsEachObjectInCyclicGraphOnlyOnce() {
    Set<Object> set = new HashSet<>();
    Object plainObject = new Object();
    Object[] arrayOfObjects = new Object[2];
    ArrayList<Object> arrayList = new ArrayList<>();

    set.add(plainObject);
    set.add(arrayOfObjects);
    arrayOfObjects[0] = arrayList;
    arrayList.add(set); // Creates a cycle: set -> arrayOfObjects -> arrayList -> set

    List<Object> visited = new ArrayList<>();
    Visitor visitor = (parent, object) -> visited.add(object);

    new ObjectTraverser().breadthFirstSearch(set, visitor, false);

    assertThat(visited)
        .containsOnlyOnce(set, plainObject, arrayOfObjects, arrayList);
  }

  @Test
  public void doesNotVisitFieldsOfObjectIfVisitReturnsFalse() {
    Set<Object> set1 = new HashSet<>();
    final Set<Object> set2 = new HashSet<>();
    Object object3 = new Object();
    set1.add(set2);
    set2.add(object3);

    List<Object> visited = new ArrayList<>();

    Visitor visitor = (parent, object) -> {
      visited.add(object);
      return object != set2;
    };

    new ObjectTraverser().breadthFirstSearch(set1, visitor, true);

    assertThat(visited)
        .containsOnlyOnce(set1, set2)
        .doesNotContain(object3);
  }

  @Disabled(value = "Disabled because it needs to be verified manually")
  @Test
  public void testHistogram() throws Exception {
    Set<Object> set1 = new HashSet<>();
    final Set<Object> set2 = new HashSet<>();
    Object object3 = new Object();
    set1.add(set2);
    set2.add(object3);

    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "ObjectSizer.SIZE_OF_CLASS",
        "org.apache.geode.internal.size.SizeOfUtil0");
    System.out.println(ObjectGraphSizer.histogram(set1, true));
  }

  private static class ObjectWithOnlyPrimitiveFields {
    private static final boolean B = false;
    private static final double D = 1.2d;
    private static final float F = 3.4f;
    private static final int I = 5;
    private static final long L = 6L;
    private final boolean b = !B;
    private final double d = -D;
    private final float f = -F;
    private final int i = -I;
    private final long l = -L;
  }

  private static class ObjectWithReferenceFields {
    private static Object staticField1;
    private static Object staticField2;
    private final Object instanceField1;
    private final Object instanceField2;

    ObjectWithReferenceFields(Object instanceField1, Object instanceField2) {
      this.instanceField1 = instanceField1;
      this.instanceField2 = instanceField2;
    }
  }
}
