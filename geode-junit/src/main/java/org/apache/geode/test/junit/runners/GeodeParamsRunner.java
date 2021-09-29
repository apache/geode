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
import static org.junit.runner.Description.createTestDescription;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import junitparams.JUnitParamsRunner;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * Extends and adapts {@link JUnitParamsRunner} to be compatible with the way JUnit Vintage filters
 * tests by {@link Category}. The differences from {@code JUnitParamsRunner} are:
 * <ul>
 * <li>When {@code GeodeParamsRunner} describes a test class, each parameterized test description
 * includes the {@code Category} annotation (if any) from the corresponding test method.</li>
 * <li>When Junit Vintage applies a filter, if the filter would exclude a test with parameters,
 * {@code GeodeParamsRunner} excludes <em>every</em> parameterization of the test method.
 * </li>
 * </ul>
 * <p>
 * <strong>Problem:</strong> {@code GeodeParamsRunner}'s filtering behavior makes it impossible
 * to execute a subset of the parameterizations of a single test method. But because all
 * parameterizations of a method share the same {@code Category} annotation, this behavior allows
 * JUnit Vintage to filter tests by category. A more nuanced fix would require a significant
 * rewrite of {@code JUnitParamsRunner}.
 */
public class GeodeParamsRunner extends JUnitParamsRunner {
  private static final String EXCLUDE_FILTER_PREFIX = "exclude ";
  private final Map<String, FrameworkMethod> testNameToFrameworkMethod = new HashMap<>();

  public GeodeParamsRunner(Class<?> testClass) throws InitializationError {
    super(testClass);
  }

  /**
   * Overrides {@link JUnitParamsRunner#describeMethod(FrameworkMethod)} to include a parameterized
   * test method's {@code Category} annotation (if any) in the descriptions of the method's tests.
   */
  @Override
  public Description describeMethod(FrameworkMethod method) {
    Description description = super.describeMethod(method);
    if (description.isTest()) {
      return description;
    }
    recordTestNamesToFrameworkMethod(description.getChildren(), method);
    return vintageCompatibleSuiteDescription(description, method.getMethod());
  }

  /**
   * Overrides {@link JUnitParamsRunner#filter(Filter)} to exclude all of a parameterized
   * method's tests if the filter would exclude any of the method's tests.
   */
  @Override
  public void filter(Filter filter) throws NoTestsRemainException {
    System.out.printf("DHE: filter(%s)%n", filter.describe());
    super.filter(vintageCompatibleFilter(filter, testNameToFrameworkMethod));
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + getTestClass().getJavaClass().getSimpleName() + ")";
  }

  private void recordTestNamesToFrameworkMethod(Collection<Description> testDescriptions,
      FrameworkMethod method) {
    testDescriptions.stream()
        .map(Description::getDisplayName)
        .forEach(testName -> testNameToFrameworkMethod.put(testName, method));
  }

  private String displayNameOf(FrameworkMethod method) {
    return createTestDescription(getTestClass().getJavaClass(), method.getName()).getDisplayName();
  }

  /**
   * Adapts a filter to exclude all of a parameterized test method's tests if the filter would
   * exclude any of the method's tests.
   */
  public Filter vintageCompatibleFilter(
      Filter originalFilter, Map<String, FrameworkMethod> parameterizedTestMap) {
    String filterDescription = originalFilter.describe();
    if (!filterDescription.startsWith(EXCLUDE_FILTER_PREFIX)) {
      // The filter is not an exclude filter. JUnitParamsRunner applies it correctly.
      return originalFilter;
    }
    String excludedTestName = filterDescription.substring(EXCLUDE_FILTER_PREFIX.length());
    FrameworkMethod parameterizedMethod = parameterizedTestMap.get(excludedTestName);
    if (isNull(parameterizedMethod)) {
      // The filter excludes a non-parameterized test. JUnitParamsRunner applies it correctly.
      return originalFilter;
    }
    return new ExcludeByDisplayName(displayNameOf(parameterizedMethod));
  }

  /**
   * Adapts a suite description to add the given method's {@code Category} annotation (if any) to
   * the descriptions of the suite's tests.
   */
  private static Description vintageCompatibleSuiteDescription(
      Description originalDescription, Method method) {
    Category categories = method.getAnnotation(Category.class);
    if (isNull(categories)) {
      return originalDescription;
    }
    Description compatibleDescription = originalDescription.childlessCopy();
    originalDescription.getChildren().stream()
        .map(testDescription -> vintageCompatibleTestDescription(testDescription, categories))
        .forEach(compatibleDescription::addChild);
    return compatibleDescription;
  }

  /**
   * Adapts a test description to add the given {@link Category} annotation.
   */
  private static Description vintageCompatibleTestDescription(
      Description originalDescription, Category categories) {
    Class<?> testClass = originalDescription.getTestClass();
    String displayName = originalDescription.getMethodName();
    return createTestDescription(testClass, displayName, categories);
  }

  /**
   * Filter descriptions by display name.
   */
  private static class ExcludeByDisplayName extends Filter {
    private final String displayNameToExclude;

    public ExcludeByDisplayName(String displayNameToExclude) {
      this.displayNameToExclude = displayNameToExclude;
    }

    @Override
    public boolean shouldRun(Description description) {
      boolean shouldRun = !description.getDisplayName().equals(displayNameToExclude);
      System.out.printf("DHE: %s shouldRun(%s): %s%n", describe(), description, shouldRun);
      return shouldRun;
    }

    @Override
    public String describe() {
      return "exclude display name " + displayNameToExclude;
    }
  }
}
