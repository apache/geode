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
package org.apache.geode.test.engine;

import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toSet;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.experimental.categories.Category;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.UniqueId;
import org.junit.runner.RunWith;
import org.junit.vintage.engine.VintageTestEngine;
import org.junit.vintage.engine.descriptor.RunnerTestDescriptor;
import org.junit.vintage.engine.descriptor.VintageTestDescriptor;

/**
 * A JUnit test engine that partially addresses {@code JUnitParamsRunner}'s inadvertent inclusion of
 * undesired tests in a test run. When {@code VintageTestEngine} attempts to apply a filter to each
 * test runner, {@code JUnitParamsRunner} ignores certain filters, which causes the framework to
 * <p>
 * Limitations:
 * <ul>
 * <li><strong>Single-category requests.</strong> {@code GeodeTestEngine} handles only the case
 * where
 * the request specifies a single category of tests to run.</li>
 * <li><strong>All tests in category or no tests in category.</strong> If a
 * {@code JUnitParamsRunner} test class includes any tests that have the specified category,
 * {@code GeodeTestEngine} is unable to remove the tests that do not have that category.</li>
 * </ul>
 */
public class GeodeTestEngine implements TestEngine {
  private static final String ENGINE_ID = "geode-test-engine";
  private static final String INCLUDE_CATEGORY = System.getenv("GEODE_TEST_CATEGORY");
  private static final String JUNIT_PARAMS_RUNNER_CLASS_NAME = "junitparams.JUnitParamsRunner";

  // Delegate most of the work to a VintageTestEngine.
  private final TestEngine vintage = new VintageTestEngine();

  @Override
  public String getId() {
    return ENGINE_ID;
  }

  @Override
  public TestDescriptor discover(EngineDiscoveryRequest request, UniqueId id) {
    TestDescriptor root = vintage.discover(request, id);
    if (!isNull(INCLUDE_CATEGORY)) {
      Set<? extends TestDescriptor> testsToExclude = root.getDescendants().stream()
          .filter(TestDescriptor::isTest)
          .map(VintageTestDescriptor.class::cast)
          .filter(GeodeTestEngine::includedByMistake)
          .collect(toSet());
      testsToExclude.forEach(GeodeTestEngine::removeFromTree);
    }
    return root;
  }

  @Override
  public void execute(ExecutionRequest request) {
    vintage.execute(request);
  }

  /**
   * Report whether descriptor was included by mistake. The vintage engine has asked each test
   * class's runner to describe the tests in a specific category. {@code JUnitParamsRunner}
   * mistakenly describes all tests, even ones that lack the desired category.
   */
  private static boolean includedByMistake(VintageTestDescriptor descriptor) {
    Class<?> testClass = descriptor.getDescription().getTestClass();
    return runsWithJUnitParamsRunner(testClass)
        && lacksIncludeCategory(testClass)
        && lacksIncludeCategory(testMethod(testClass, methodName(descriptor)));
  }

  private static boolean runsWithJUnitParamsRunner(Class<?> testClass) {
    RunWith runWithAnnotation = testClass.getAnnotation(RunWith.class);
    return !isNull(runWithAnnotation)
        && JUNIT_PARAMS_RUNNER_CLASS_NAME.equals(runWithAnnotation.value().getName());
  }

  private static boolean lacksIncludeCategory(AnnotatedElement element) {
    Category categoryAnnotation = element.getAnnotation(Category.class);
    if (isNull(categoryAnnotation)) {
      return true;
    }
    return Stream.of(categoryAnnotation.value())
        .map(Class::getName)
        .noneMatch(INCLUDE_CATEGORY::equals);
  }

  /**
   * Find the name of the test method represented by the descriptor.
   */
  private static String methodName(TestDescriptor descriptor) {
    TestDescriptor container = descriptor.getParent().get();
    if (container instanceof RunnerTestDescriptor) {
      // The container represents a test class, which means that this descriptor represents a
      // non-parameterized test method. Get the method name from the descriptor.
      return descriptor.getDisplayName();
    }
    // The container represents a method with parameters, which means that this descriptor
    // represents parameterization of that method. Get the method name from the container.
    return container.getDisplayName();
  }

  private static Method testMethod(Class<?> testClass, String methodName) {
    return Arrays.stream(testClass.getDeclaredMethods())
        .filter(m -> methodName.equals(m.getName()))
        .findAny()
        .get();
  }

  /**
   * Remove this descriptor from the descriptor tree, along with any non-root ancestors that become
   * empty as a result.
   */
  private static void removeFromTree(TestDescriptor descriptor) {
    descriptor.getParent().ifPresent(parent -> {
      parent.removeChild(descriptor);
      if (parent.getChildren().isEmpty()) {
        removeFromTree(parent);
      }
    });
  }
}
