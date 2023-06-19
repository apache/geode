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
package org.apache.geode.test.concurrency;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import junit.framework.AssertionFailedError;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import org.apache.geode.test.concurrency.annotation.ConcurrentTestConfig;
import org.apache.geode.test.concurrency.loop.LoopRunner;


/**
 * Test runner for tests that involve multiple threads.
 *
 * All methods annotated with @Test must take a {@link ParallelExecutor} as a parameter, which the
 * test can use to invoke code in parallel.
 *
 * This test run will try to exercise the test method to flush out any concurrent bugs in the
 * parallel execution.
 *
 * All test logic and state *must* be encapsulated in the individual test methods. This is because
 * the concurrency testing logic may need to invoke the test body multiple times, possibly in
 * parallel.
 *
 * No guarantees are currently made about logic in methods annotated with @Before, @BeforeClass or
 * about the behavior of {@link Rule} for concurrent tests.
 *
 * Example
 *
 * <code>
 * &#64;RunWith(ConcurrentTestRunner.class)
 * public void MyTest {
 *   &#64;Test
 *   public void someTestMethod(ParallelExecutor executor) {
 *     AtomicInteger atomicInteger = new AtomicInteger();
 *     executor.inParallel(() -&gt; atomicInteger.incrementAndGet());
 *     executor.inParallel(() -&gt; atomicInteger.incrementAndGet());
 *     executor.execute();
 *     assertEquals(2, atomicInteger.get());
 *   }
 * }
 * </code>
 *
 * ConcurrentTestRunner currently executes tests using the {@link LoopRunner} which will run the
 * test many times.
 */
public class ConcurrentTestRunner extends ParentRunner<FrameworkMethod> {
  /**
   * Delegate to actually run the test.
   */
  private final Runner runner;

  public ConcurrentTestRunner(Class testClass) throws InitializationError {
    super(testClass);
    ConcurrentTestConfig configuration = getTestClass().getAnnotation(ConcurrentTestConfig.class);
    if (configuration == null) {
      runner = new LoopRunner();
      return;
    }

    try {
      runner = configuration.runner().getDeclaredConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException
        | NoSuchMethodException e) {
      throw new InitializationError(e);
    }
  }

  @Override
  protected List<FrameworkMethod> getChildren() {
    return getTestClass().getAnnotatedMethods(Test.class);
  }

  @Override
  protected void collectInitializationErrors(List<Throwable> errors) {
    super.collectInitializationErrors(errors);
    validateTestMethods(getChildren(), errors);
  }

  private void validateTestMethods(List<FrameworkMethod> methods, List<Throwable> errors) {
    for (FrameworkMethod method : methods) {
      if (!Arrays.equals(method.getMethod().getParameterTypes(),
          new Class[] {ParallelExecutor.class})) {
        errors.add(new AssertionFailedError("Incorrect signature on method: " + method
            + ". For a concurrent test, all test methods should take a ParallelExector parameter."));
      }
    }
  }

  @Override
  protected Description describeChild(FrameworkMethod child) {
    return Description.createTestDescription(child.getDeclaringClass(), child.getName());
  }

  @Override
  protected void runChild(FrameworkMethod child, RunNotifier notifier) {
    notifier.fireTestStarted(describeChild(child));
    try {
      List<Throwable> failures = runner.runTestMethod(child.getMethod());
      failures.stream()
          .forEach(failure -> notifier.fireTestFailure(new Failure(describeChild(child), failure)));
    } finally {
      notifier.fireTestFinished(describeChild(child));
    }
  }
}
