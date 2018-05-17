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

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import org.junit.runner.Runner;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * SuiteRunner is like Junit Suite, it's used in conjunction with {@code SuiteClass({xx.class,
 * yy.class})} It's different from Suite in two ways:
 * <p>
 *
 * 1. it should only contain contain Junit4 test classes<br>
 * 2. the test method names inside each test class are appended with the suiteClass name so that the
 * result will show up different as when you run these tests alone.
 */
public class SuiteRunner extends Suite {

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Inherited
  public @interface Candidate {
    /**
     * @return the classes to be run
     */
    Class<?> value();
  }

  public SuiteRunner(final Class<?> testClass, final RunnerBuilder builder)
      throws InitializationError {
    super(testClass, getRunners(testClass));
  }

  private static List<Runner> getRunners(final Class<?> testClass) throws InitializationError {
    SuiteClasses annotation = testClass.getAnnotation(SuiteClasses.class);
    if (annotation == null) {
      throw new InitializationError(
          String.format("class '%s' must have a SuiteClasses annotation", testClass.getName()));
    }

    SuiteRunner.Candidate candidate = testClass.getAnnotation(Candidate.class);
    Class<?> candidateClass = null;
    if (candidate != null) {
      candidateClass = candidate.value();
    }

    Class<?>[] childClasses = annotation.value();
    List<Runner> runners = new ArrayList<>();
    for (Class childClass : childClasses) {
      runners.add(new SuiteBlockRunner(testClass, childClass));

      if (candidateClass != null) {
        runners.add(new SuiteBlockRunner(testClass, candidateClass));
      }
    }
    return runners;
  }
}
