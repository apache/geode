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

import static java.util.Objects.isNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;

import junitparams.JUnitParamsRunner;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

public class DHERunner extends JUnitParamsRunner {
  private boolean described = false;

  public DHERunner(Class<?> testClass) throws InitializationError {
    super(testClass);
  }

  @Override
  public Description getDescription() {
    Description description = super.getDescription();
    if (!described) {
      described = true;
      describeTree("", description);
    }
    return description;
  }

  @Override
  public Description describeMethod(FrameworkMethod method) {
    Description paramsDescription = super.describeMethod(method);
    Class<?> testClass = method.getDeclaringClass();
    Description patched = patchMethod(testClass, paramsDescription);
    return patched;
  }

  private static Description patchMethod(Class<?> testClass, Description methodDescription) {
    if (methodDescription.isTest()) {
      return methodDescription;
    }
    String displayName = methodDescription.getDisplayName();
    Method method = Arrays.stream(testClass.getDeclaredMethods())
        .filter(m -> m.getName().equals(displayName))
        .findFirst()
        .get();
    Category categoryAnnotation = method.getAnnotation(Category.class);
    if (isNull(categoryAnnotation)) {
      return methodDescription;
    }
    Description patched = Description.createSuiteDescription(displayName);
    methodDescription.getChildren().stream()
        .map(testDescription -> patchTest(testDescription, categoryAnnotation))
        .forEach(patched::addChild);
    return patched;
  }

  private static Description patchTest(Description testDescription, Annotation annotation) {
    Class<?> testClass = testDescription.getTestClass();
    String displayName = testDescription.getMethodName();
    return Description.createTestDescription(testClass, displayName, annotation);
  }

  private static void describeTree(String indent, Description description) {
    System.out.printf("DHE: %s%s%n", indent, description.getDisplayName());
    System.out.printf("DHE: %s    TestClass  : %s%n", indent, description.getTestClass());
    System.out.printf("DHE: %s    Annotations: %s%n", indent, description.getAnnotations());
    System.out.printf("DHE: %s    MethodName : %s%n", indent, description.getMethodName());
    System.out.printf("DHE: %s    is suite   : %s%n", indent, description.isSuite());
    System.out.printf("DHE: %s    is test    : %s%n", indent, description.isTest());
    description.getChildren().forEach(c -> describeTree(indent + "    ", c));
  }

  @Override
  public void run(RunNotifier notifier) {
    super.run(notifier);
  }

  @Override
  public void filter(Filter filter) throws NoTestsRemainException {
    super.filter(new SpyFilter(filter));
  }

  private static class SpyFilter extends Filter {
    private final Filter filter;

    public SpyFilter(Filter filter) {
      this.filter = filter;
    }

    @Override
    public boolean shouldRun(Description description) {
      System.out.printf("DHE: filtering %s with %s%n", description.getDisplayName(), describe());
      boolean shouldRun = filter.shouldRun(description);
      System.out.println("DHE:     " + shouldRun);
      return shouldRun;
    }

    @Override
    public String describe() {
      return filter.describe();
    }
  }
}
