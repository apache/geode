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
package org.apache.geode.test.junit.runners;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.junit.runner.Description;

/**
 * Converts a {@link Description} into a form that compares all informative fields.
 */
class ComparableDescription {
  private final Class<?> testClass;
  private final String displayName;
  private final Collection<Annotation> annotations = new HashSet<>();
  private final List<ComparableDescription> children = new ArrayList<>();

  /**
   * Returns a comparable version of the description, retaining annotations on all descriptions.
   */
  public static ComparableDescription comparingAllDetails(Description description) {
    return new ComparableDescription(description, true);
  }

  /**
   * Returns a comparable version of the description, discarding annotations on test descriptions.
   */
  public static ComparableDescription ignoringTestAnnotations(Description description) {
    return new ComparableDescription(description, false);
  }

  private ComparableDescription(Description description, boolean includeTestAnnotations) {
    testClass = description.getTestClass();
    displayName = description.getDisplayName();
    if (description.isTest() && includeTestAnnotations) {
      annotations.addAll(description.getAnnotations());
    }
    description.getChildren().stream()
        .map(child -> new ComparableDescription(child, includeTestAnnotations))
        .forEach(children::add);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ComparableDescription that = (ComparableDescription) o;
    return Objects.equals(testClass, that.testClass)
        && Objects.equals(displayName, that.displayName)
        && Objects.equals(annotations, that.annotations)
        && Objects.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(testClass, displayName, annotations, children);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" +
        "testClass=" + testClass +
        ", displayName='" + displayName + '\'' +
        ", annotations=" + annotations +
        ", children=" + children +
        '}';
  }
}
