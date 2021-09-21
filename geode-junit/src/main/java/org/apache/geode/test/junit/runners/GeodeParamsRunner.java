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

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.junit.runner.Description.createTestDescription;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import junitparams.JUnitParamsRunner;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * {@code GeodeParamsRunner} adapts {@link JUnitParamsRunner} to be somewhat more compatible with
 * the JUnit Vintage test engine.
 * <ul>
 * <li>When {@code JUnitParamsRunner} describes tests, it omits the annotations that JUnit
 * Vintage uses to filter tests. In particular, it omits the {@link Category} annotation that
 * Geode's build system sometimes uses to filter tests by category.</li>
 * <li>When JUnit Vintage attempts to exclude parameterized tests by name, {@code
 * JUnitParamsRunner} applies the filters to the un-parameterized name, and so fails to exclude
 * the test.</li>
 * </ul>
 * <p>
 * This class changes {@code JUnitParamsRunner}'s test descriptions to restore discarded
 * {@code Category} annotations. And it changes {@code JUnitParamsRunner}'s filtering to exclude a
 * parameterized test method if any of its parameterizations are excluded.
 * <p>
 * <strong>Problem:</strong> This runner makes it impossible to execute a subset of the
 * parameterizations of a single test. If you exclude even one parameterization of a test method,
 * this runner excludes all of that test method's parameterizations.
 */
public class GeodeParamsRunner extends JUnitParamsRunner {
  private static final String EXCLUDE_FILTER_PREFIX = "exclude ";
  private final Map<String, String> testNameToSuiteName = new HashMap<>();

  public GeodeParamsRunner(Class<?> testClass) throws InitializationError {
    super(testClass);
  }

  /**
   * Overrides {@link JUnitParamsRunner#describeMethod(FrameworkMethod)} to restore any discarded
   * {@link Category} annotation.
   */
  @Override
  public Description describeMethod(FrameworkMethod method) {
    Description description = super.describeMethod(method);
    if (description.isTest()) {
      return description;
    }
    recordTestNamesToSuiteName(description);
    return attachCategory(description, method.getMethod());
  }

  private void recordTestNamesToSuiteName(Description suite) {
    Class<?> testClass = suite.getChildren().get(0).getTestClass();
    String suiteName = format("%s(%s)", suite.getDisplayName(), testClass.getName());
    suite.getChildren().stream()
        .map(Description::getDisplayName)
        .forEach(testName -> testNameToSuiteName.put(testName, suiteName));
  }

  /**
   * Overrides {@link JUnitParamsRunner#filter} to exclude a suite if the given filter would
   * exclude any of the suites tests.
   */
  @Override
  public void filter(Filter filter) throws NoTestsRemainException {
    super.filter(vintageCompatibleExcludeFilter(filter, testNameToSuiteName::get));
  }

  private static Description attachCategory(Description description, Method method) {
    Category categories = method.getAnnotation(Category.class);
    if (isNull(categories)) {
      return description;
    }
    Description restoredDescription = description.childlessCopy();
    description.getChildren().stream()
        .map(testDescription -> addCategory(testDescription, categories))
        .forEach(restoredDescription::addChild);
    return restoredDescription;
  }

  private static Description addCategory(Description testDescription, Category categories) {
    Class<?> testClass = testDescription.getTestClass();
    String displayName = testDescription.getMethodName();
    return createTestDescription(testClass, displayName, categories);
  }

  /**
   * Adapts a JUnit Vintage exclude filter to exclude a suite if the filter would exclude any of
   * the suite's tests.
   */
  public static Filter vintageCompatibleExcludeFilter(Filter originalFilter,
      Function<String, String> toSuite) {
    String filterDescription = originalFilter.describe();
    if (!filterDescription.startsWith(EXCLUDE_FILTER_PREFIX)) {
      return originalFilter;
    }
    String excludedTestName = filterDescription.substring(EXCLUDE_FILTER_PREFIX.length());
    String suiteName = toSuite.apply(excludedTestName);
    if (isNull(suiteName)) {
      return originalFilter;
    }
    return new ExcludeByName(suiteName);
  }

  /**
   * Rejects a description if its display name is the given name.
   */
  private static class ExcludeByName extends Filter {
    private final String excludedName;

    public ExcludeByName(String name) {
      excludedName = name;
    }

    @Override
    public boolean shouldRun(Description description) {
      return !description.getDisplayName().equals(excludedName);
    }

    @Override
    public String describe() {
      return "exclude suite " + excludedName;
    }
  }
}
