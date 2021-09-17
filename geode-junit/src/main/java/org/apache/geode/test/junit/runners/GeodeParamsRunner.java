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
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * <strong>Problem:</strong> There is no longer a way to execute a subset of the parameterizations
 * of a single test. If you try to execute a single parameterization of a method, the adapted filter
 * excludes the entire method and all of its parameterizations.
 */
public class GeodeParamsRunner extends JUnitParamsRunner {
  public GeodeParamsRunner(Class<?> testClass) throws InitializationError {
    super(testClass);
  }

  /**
   * Overrides {@link JUnitParamsRunner#describeMethod(FrameworkMethod)} to restore an omitted
   * {@link Category} annotation.
   */
  @Override
  public Description describeMethod(FrameworkMethod method) {
    Description paramsDescription = super.describeMethod(method);
    return restoreMethodCategory(paramsDescription, method.getMethod());
  }

  /**
   * Overrides {@link JUnitParamsRunner#filter} to exclude a "parameterized method container" if
   * the given filter would exclude a parameterization of that method.
   */
  @Override
  public void filter(Filter filter) throws NoTestsRemainException {
    super.filter(ExcludeParameterizedTestMethodByName.adapt(filter));
  }

  private static Description restoreMethodCategory(Description description, Method method) {
    if (description.isTest()) {
      return description;
    }
    Category categories = method.getAnnotation(Category.class);
    if (isNull(categories)) {
      return description;
    }
    Description restoredDescription = description.childlessCopy();
    description.getChildren().stream()
        .map(testDescription -> addTestCategory(testDescription, categories))
        .forEach(restoredDescription::addChild);
    return restoredDescription;
  }

  private static Description addTestCategory(Description testDescription, Category categories) {
    Class<?> testClass = testDescription.getTestClass();
    String displayName = testDescription.getMethodName();
    return createTestDescription(testClass, displayName, categories);
  }

  /**
   * Rejects a description if its display name is the given name.
   */
  private static class ExcludeParameterizedTestMethodByName extends Filter {
    // Finds the method name in a test description's display name or an exclude filter's name.
    private static final String METHOD_NAME_REGEX = "([a-zA-Z0-9_]+)\\(.*";
    private static final Pattern METHOD_NAME = Pattern.compile(METHOD_NAME_REGEX);
    // Detects whether a filter is a JUnit Vintage "exclude test by name" filter.
    private static final Pattern EXCLUDE_FILTER_NAME = Pattern.compile("exclude " + METHOD_NAME);

    private final String nameToExclude;

    public ExcludeParameterizedTestMethodByName(String nameToExclude) {
      this.nameToExclude = nameToExclude;
    }

    @Override
    public boolean shouldRun(Description description) {
      String displayName = description.getDisplayName();
      Matcher methodNameExtractor = METHOD_NAME.matcher(displayName);
      if (!methodNameExtractor.matches()) {
        return false;
      }
      String methodName = methodNameExtractor.group(1);
      return !Objects.equals(methodName, nameToExclude);
    }

    @Override
    public String describe() {
      return "exclude " + nameToExclude;
    }

    /**
     * Adapts a JUnit Vintage "exclude" filter to exclude a parameterized method if the filter would
     * exclude any of the parameterizations.
     */
    public static Filter adapt(Filter originalFilter) {
      String filterName = originalFilter.describe();
      Matcher excludeFilterMatcher = EXCLUDE_FILTER_NAME.matcher(filterName);
      if (!excludeFilterMatcher.matches()) {
        return originalFilter;
      }
      String methodNameToExclude = excludeFilterMatcher.group(1);
      return new ExcludeParameterizedTestMethodByName(methodNameToExclude);
    }
  }
}
