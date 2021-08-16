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

import static java.util.Arrays.stream;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.assertj.core.api.SoftAssertions;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;

class GeodeParamsRunnerTest {
  @Nested
  class GetDescription {
    @ParameterizedTest(name = "{displayName}({arguments})")
    @ValueSource(classes = {ATestClass.class, ATestSubclass.class})
    public void describesTheSameNonParameterizedTestsAsJUnitParamsRunner(Class<?> testClass)
        throws InitializationError {
      Description geodeParamsRunnerClassDescription = new GeodeParamsRunner(testClass)
          .getDescription();

      Collection<ComparableDescription> geodeParamsRunnerNonParameterizedTestDescriptions =
          geodeParamsRunnerClassDescription.getChildren().stream()
              // Each test child describes a non-parameterized test method.
              .filter(Description::isTest)
              .map(ComparableDescription::comparingAllDetails)
              .collect(toList());

      Description jUnitParamsRunnerClassDescription = new JUnitParamsRunner(testClass)
          .getDescription();
      Collection<ComparableDescription> jUnitParamsRunnerNonParameterizedTestDescriptions =
          jUnitParamsRunnerClassDescription.getChildren().stream()
              .filter(Description::isTest)
              .map(ComparableDescription::comparingAllDetails)
              .collect(toList());

      assertThat(geodeParamsRunnerNonParameterizedTestDescriptions)
          .isEqualTo(jUnitParamsRunnerNonParameterizedTestDescriptions);
    }

    /**
     * Compares the parameterized test suites produced by {@code GeodeParamsRunner} to those
     * produced by {@code JUnitParamsRunner}, comparing every detail except the annotations on
     * the parameterized test descriptions. A separate test verifies those annotations.
     */
    @ParameterizedTest(name = "{displayName}({arguments})")
    @ValueSource(classes = {ATestClass.class, ATestSubclass.class})
    public void describesTheSameParameterizedSuitesAsJUnitParamsRunnerIgnoringTestAnnotations(
        Class<?> testClass)
        throws InitializationError {
      Description geodeParamsRunnerClassDescription = new GeodeParamsRunner(testClass)
          .getDescription();

      Collection<ComparableDescription> geodeParamsRunnerSuiteDescriptions =
          geodeParamsRunnerClassDescription.getChildren().stream()
              // Each suite child describes a parameterized test method.
              .filter(Description::isSuite)
              // Ignore annotations on the test descriptions in the suite. Another test verifies
              // those.
              .map(ComparableDescription::ignoringTestAnnotations)
              .collect(toList());

      Description jUnitParamsRunnerClassDescription = new JUnitParamsRunner(testClass)
          .getDescription();
      Collection<ComparableDescription> jUnitParamsRunnerSuiteDescriptions =
          jUnitParamsRunnerClassDescription.getChildren().stream()
              .filter(Description::isSuite)
              .map(ComparableDescription::ignoringTestAnnotations)
              .collect(toList());

      assertThat(geodeParamsRunnerSuiteDescriptions)
          .isEqualTo(jUnitParamsRunnerSuiteDescriptions);
    }

    @ParameterizedTest(name = "{displayName}({arguments})")
    @ValueSource(classes = {ATestClass.class, ATestSubclass.class})
    public void describesEachParameterizedTestWithCategoryAnnotationFromMethod(Class<?> testClass)
        throws InitializationError {
      List<Description> childDescriptions = new GeodeParamsRunner(testClass)
          .getDescription()
          .getChildren();

      List<Method> parameterizedTestMethods = parameterizedTestMethodsOf(testClass);
      SoftAssertions softly = new SoftAssertions();
      parameterizedTestMethods.forEach(method -> {
        String methodName = method.getName();
        Description suiteDescription = findByDisplayName(childDescriptions, methodName);
        softly.assertThat(suiteDescription)
            .as("suite description for parameterized method " + methodName)
            .isNotNull();
        if (isNull(suiteDescription)) {
          return;
        }

        // The suite description contains one test description for each parameterization. Each test
        // description must have the same Category annotation as the method.
        suiteDescription.getChildren().forEach(
            testDescription -> softly.assertThat(testDescription.getAnnotation(Category.class))
                .as("Category annotation on test description " + testDescription)
                .isEqualTo(method.getAnnotation(Category.class)));
      });
      softly.assertAll();
    }
  }

  @Nested
  class Filtering {
    private final Class<?> testClass = ATestSubclass.class;
    private final RunNotifier notifier = new RunNotifier();
    private final List<String> executions = new ArrayList<>();

    @BeforeEach
    void recordTestExecutions() {
      RunListener listener = new RunListener() {
        @Override
        public void testStarted(Description description) {
          executions.add(description.getDisplayName());
        }
      };
      notifier.addListener(listener);
    }

    @ParameterizedTest(name = "{displayName}({arguments})")
    @ValueSource(classes = {ATestClass.class, ATestSubclass.class})
    void executesEveryTest_ifNoFilterApplied(Class<?> testClass) throws InitializationError {
      GeodeParamsRunner runner = new GeodeParamsRunner(testClass);
      Description classDescription = runner.getDescription();

      // The runner must execute every test.
      Collection<String> requiredExecutions =
          testDescriptionDescendantsOf(classDescription).stream()
              .map(Description::getDisplayName)
              .collect(toSet());

      // No filter applied

      runner.run(notifier);

      assertThat(executions)
          .as(() -> "tests executed by " + runner)
          .hasSameElementsAs(requiredExecutions);
    }

    @ParameterizedTest(name = "{displayName}({arguments})")
    @ValueSource(classes = {ATestClass.class, ATestSubclass.class})
    void skipsNonParameterizedTest_ifExcludedByFilter(Class<?> testClass)
        throws InitializationError, NoTestsRemainException {
      GeodeParamsRunner runner = new GeodeParamsRunner(testClass);
      Description classDescription = runner.getDescription();

      // Each test description child describes a non-parameterized test. Pick one to exclude.
      Description testToExclude = testDescriptionChildrenOf(classDescription)
          .get(0);

      // The runner must execute every test except the excluded one.
      Collection<String> requiredExecutions = testDescriptionDescendantsOf(classDescription)
          .stream()
          .filter(d -> !d.equals(testToExclude))
          .map(Description::getDisplayName)
          .collect(toList());

      runner.filter(new ExcludeDescriptionFilter(testToExclude));

      runner.run(notifier);

      assertThat(executions)
          .as(() -> "tests executed by " + runner + " after excluding " + testToExclude)
          .hasSameElementsAs(requiredExecutions);
    }

    @ParameterizedTest(name = "{displayName}({arguments})")
    @ValueSource(classes = {ATestClass.class, ATestSubclass.class})
    void skipsEveryTestInParameterizedTestSuite_ifFilterExcludesAnyTestInSuite(Class<?> testClass)
        throws InitializationError, NoTestsRemainException {
      GeodeParamsRunner runner = new GeodeParamsRunner(testClass);
      Description classDescription = runner.getDescription();

      // Each suite child of classDescription describes a parameterized method. Pick a suite.
      Description suite = suiteDescriptionChildrenOf(classDescription).get(0);

      // Each child of the suite describes a test with parameters. Pick a test to exclude.
      List<Description> testsInSuite = suite.getChildren();
      Description testToExclude = testsInSuite.get(0);

      // The runner must execute every test except the ones in the same suite as the excluded test.
      Collection<String> requiredExecutions =
          testDescriptionDescendantsOf(classDescription).stream()
              .filter(d -> !testsInSuite.contains(d))
              .map(Description::getDisplayName)
              .collect(toSet());

      runner.filter(new ExcludeDescriptionFilter(testToExclude));

      runner.run(notifier);

      assertThat(executions)
          .as(() -> "tests executed by " + runner + " after excluding " + testToExclude)
          .hasSameElementsAs(requiredExecutions);
    }
  }

  // A category to test GeodeParamsRunner
  public static class ATestCategory {
  }

  /**
   * Cases for {@code GeodeParamsRunner} to handle: Every combination of parameterized (or not)
   * and categorized (or not).
   */
  @RunWith(GeodeParamsRunner.class)
  public static class ATestClass {
    @org.junit.Test
    @Parameters({"A", "B"})
    @Category(ATestCategory.class)
    public void parameterizedTestInCategory(String ignored) {}

    @org.junit.Test
    @Parameters({"A", "B"})
    public void parameterizedTestNotInCategory(String ignored) {}

    @org.junit.Test
    @Category(ATestCategory.class)
    public void nonParameterizedTestInCategory() {}

    @Test
    public void nonParameterizedTestNotInCategory() {}
  }

  /**
   * A special case for {@code GeodeParamsRunner} to handle: {@code JUnitParamsRunner} describes
   * the same methods for this subclass as for {@code ATestClass}, but the display names mention
   * the subclass and not the class where the method was declared.
   */
  public static class ATestSubclass extends ATestClass {
  }

  private static class ExcludeDescriptionFilter extends Filter {
    private final Description description;

    ExcludeDescriptionFilter(Description description) {
      this.description = description;
    }

    @Override
    public boolean shouldRun(Description description) {
      return !this.description.equals(description);
    }

    @Override
    public String describe() {
      return "exclude " + description;
    }
  }

  /**
   * Returns a consumer that accepts a Description and adds the description and its descendants to
   * the given collection.
   */
  private static Consumer<Description> addSelfAndDescendantsTo(
      Collection<Description> descriptions) {
    return parent -> {
      descriptions.add(parent);
      parent.getChildren().forEach(addSelfAndDescendantsTo(descriptions));
    };
  }

  /**
   * Returns a collection of root's descendants. The collection does not contain root.
   */
  private static Collection<Description> descendantsOf(Description root) {
    List<Description> descendants = new ArrayList<>();
    root.getChildren().forEach(addSelfAndDescendantsTo(descendants));
    return descendants;
  }

  /**
   * Returns the description with the given name in descriptions, or null if there is no such
   * description.
   */
  private static Description findByDisplayName(List<Description> descriptions, String displayName) {
    return descriptions.stream()
        .filter(d -> d.getDisplayName().equals(displayName))
        .findAny()
        .orElse(null);
  }

  private static List<Method> parameterizedTestMethodsOf(Class<?> testClass) {
    return testMethodsOf(testClass)
        .filter(m -> m.isAnnotationPresent(Parameters.class))
        .collect(toList());
  }

  /**
   * Returns a list of the suite description children of root.
   */
  private static List<Description> suiteDescriptionChildrenOf(Description root) {
    return root.getChildren().stream()
        .filter(Description::isSuite)
        .collect(toList());
  }

  /**
   * Returns a list of the test description children of root.
   */
  private static List<Description> testDescriptionChildrenOf(Description root) {
    return root.getChildren().stream()
        .filter(Description::isTest)
        .collect(toList());
  }

  /**
   * Returns a list of the test description descendants of root.
   */
  private static List<Description> testDescriptionDescendantsOf(Description root) {
    return descendantsOf(root).stream()
        .filter(Description::isTest)
        .collect(toList());
  }

  private static Stream<Method> testMethodsOf(Class<?> testClass) {
    return stream(testClass.getMethods())
        .filter(m -> m.isAnnotationPresent(org.junit.Test.class));
  }
}
