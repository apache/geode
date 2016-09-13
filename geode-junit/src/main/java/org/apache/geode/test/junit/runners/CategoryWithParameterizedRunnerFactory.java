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
package com.gemstone.gemfire.test.junit.runners;

import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides an early fix for JUnit issue <a href="https://github.com/junit-team/junit4/issues/751>751</a>.
 *
 * Once JUnit 4.13 is released, this class can be removed.
 *
 * See also <a href="https://issues.apache.org/jira/browse/GEODE-1350">GEODE-1350</a>
 */
public class CategoryWithParameterizedRunnerFactory implements ParametersRunnerFactory {
  @Override
  public Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {
    return new CategoryWithParameterizedRunner(test);
  }
}
