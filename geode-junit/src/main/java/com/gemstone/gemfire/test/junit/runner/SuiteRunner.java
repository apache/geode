/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.test.junit.runner;

import java.util.ArrayList;
import java.util.List;

import org.junit.runner.Runner;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * SuiteRunner is like Junit Suite, it's used in conjunction with <code>SuiteClass({xx.class, yy.class})</code>
 * It's different from Suite in two ways:
 * 1. it should only contain contain Junit4 test classes
 * 2. the test method names inside each test class are appended with the suiteClass name so that the result will show up different
 * as when you run these tests alone.
 */
public class SuiteRunner extends Suite {

  public SuiteRunner(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
    super(klass, getRunners(klass));
  }

  private static List<Runner> getRunners(final Class<?> klass) throws InitializationError{
    SuiteClasses annotation = klass.getAnnotation(SuiteClasses.class);
    if (annotation == null) {
      throw new InitializationError(String.format("class '%s' must have a SuiteClasses annotation", klass.getName()));
    }
    Class<?>[] childClasses = annotation.value();
    List<Runner> runners = new ArrayList<>();
    for(Class childClass:childClasses){
      runners.add(new SuiteBlockRunner(klass, childClass));
    }
    return runners;
  }
}
