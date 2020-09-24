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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runners.Parameterized;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.runners.parameterized.TestWithParameters;


public class CategoryWithParameterizedRunnerFactoryTest {

  /**
   * So much hacking in order to expose JUnit internals. I have no words...
   */
  public static class ExposedBlockJUnit4ClassRunnerWithParameters
      extends BlockJUnit4ClassRunnerWithParameters implements ExposedGetAnnotations {
    public ExposedBlockJUnit4ClassRunnerWithParameters(TestWithParameters test)
        throws InitializationError {
      super(test);
    }

    @Override
    public Annotation[] getRunnerAnnotations() {
      return super.getRunnerAnnotations();
    }
  }

  public static class ExposedBlockJUnit4ClassRunnerWithParametersFactory
      extends BlockJUnit4ClassRunnerWithParametersFactory {
    @Override
    public Runner createRunnerForTestWithParameters(TestWithParameters test)
        throws InitializationError {
      return new ExposedBlockJUnit4ClassRunnerWithParameters(test);
    }
  }

  public static class ExposedParameterized extends Parameterized {
    public ExposedParameterized(Class<?> klass) throws Throwable {
      super(klass);
    }

    @Override
    protected List<Runner> getChildren() {
      return super.getChildren();
    }
  }

  @RunWith(ExposedParameterized.class)
  @Parameterized.UseParametersRunnerFactory(ExposedBlockJUnit4ClassRunnerWithParametersFactory.class)
  public static class BrokenCategoryClass {
    @Parameterized.Parameters
    public static Iterable<String> getParams() {
      return Arrays.asList("one", "two");
    }

    @Parameterized.Parameter
    public String value;

    @Test
    public void insanity() {
      assertTrue(true);
    }
  }

  @RunWith(ExposedParameterized.class)
  @Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
  public static class WorkingCategoryClass {
    @Parameterized.Parameters
    public static Iterable<String> getParams() {
      return Arrays.asList("one", "two");
    }

    @Parameterized.Parameter
    public String value;

    @Test
    public void insanity() {
      assertTrue(true);
    }
  }

  @Test
  public void testBrokenCategoryAndParameterized() {
    Request request = Request.aClass(BrokenCategoryClass.class);
    ExposedParameterized runner = (ExposedParameterized) request.getRunner();
    request = request.filterWith(new CategoryFilter(
        (ExposedBlockJUnit4ClassRunnerWithParameters) runner.getChildren().get(0)));
    Result result = new JUnitCore().run(request);
    assertEquals(2, result.getRunCount());
  }

  @Test
  public void testWorkingCategoryAndParameterized() {
    Request request = Request.aClass(WorkingCategoryClass.class);
    ExposedParameterized runner = (ExposedParameterized) request.getRunner();
    request =
        request.filterWith(new CategoryFilter((ExposedGetAnnotations) runner.getChildren().get(0)));
    Result result = new JUnitCore().run(request);
    assertEquals(2, result.getRunCount());
  }

  public static class CategoryFilter extends Filter {

    private ExposedGetAnnotations runner;

    public CategoryFilter(ExposedGetAnnotations runner) {
      this.runner = runner;
    }

    @Override
    public boolean shouldRun(Description description) {
      if (description.getChildren().size() == 0) {
        return true;
      }

      List<Annotation> runnerAnnotations = new ArrayList<>();
      Collections.addAll(runnerAnnotations, runner.getRunnerAnnotations());
      for (Annotation a : description.getAnnotations()) {
        if (runnerAnnotations.contains(a)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String describe() {
      return CategoryFilter.class.getSimpleName();
    }
  }
}
