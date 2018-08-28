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
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.test.dunit.VM.getAllVMs;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.hamcrest.Matcher;
import org.junit.rules.ErrorCollector;

import org.apache.geode.test.dunit.VM;

/**
 * JUnit Rule that provides a shared ErrorCollector in all DistributedTest VMs. In particular, this
 * is a useful way to add assertions to CacheListener methods or other callbacks which are then
 * registered in multiple DistributedTest VMs.
 *
 * <p>
 * {@code SharedErrorCollector} can be used in DistributedTests as a {@code Rule}:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedRule distributedRule = new DistributedRule();
 *
 * {@literal @}Rule
 * public SharedErrorCollector errorCollector = new SharedErrorCollector();
 *
 * {@literal @}Test
 * public void everyVmFailsAssertion() {
 *   for (VM vm : VM.getAllVMs()) {
 *     vm.invoke(() -> errorCollector.checkThat("Failure in VM-" + vm.getId(), false, is(true)));
 *   }
 * }
 * </pre>
 *
 * <p>
 * For a more thorough example, please see
 * {@code org.apache.geode.cache.ReplicateCacheListenerDistributedTest} in the tests of geode-core.
 */
@SuppressWarnings("serial,unused")
public class SharedErrorCollector extends AbstractDistributedRule {

  private static volatile ProtectedErrorCollector errorCollector;

  public SharedErrorCollector() {
    // nothing
  }

  @Override
  protected void before() {
    invoker().invokeInEveryVMAndController(() -> errorCollector = new ProtectedErrorCollector());
  }

  @Override
  protected void after() throws Throwable {
    ProtectedErrorCollector allErrors = errorCollector;
    try {
      for (VM vm : getAllVMs()) {
        List<Throwable> remoteFailures = new ArrayList<>();
        remoteFailures.addAll(vm.invoke(() -> errorCollector.errors()));
        for (Throwable t : remoteFailures) {
          allErrors.addError(t);
        }
      }
      invoker().invokeInEveryVMAndController(() -> errorCollector = null);
    } finally {
      allErrors.verify();
    }
  }

  /**
   * @see ErrorCollector#addError(Throwable)
   */
  public void addError(Throwable error) {
    errorCollector.addError(error);
  }

  /**
   * @see ErrorCollector#checkThat(Object, Matcher)
   */
  public <T> void checkThat(final T value, final Matcher<T> matcher) {
    errorCollector.checkThat(value, matcher);
  }

  /**
   * @see ErrorCollector#checkThat(String, Object, Matcher)
   */
  public <T> void checkThat(final String reason, final T value, final Matcher<T> matcher) {
    errorCollector.checkThat(reason, value, matcher);
  }

  /**
   * @see ErrorCollector#checkSucceeds(Callable)
   */
  public <T> T checkSucceeds(Callable<T> callable) {
    return errorCollector.checkSucceeds(callable);
  }

  /**
   * Uses reflection to acquire access to the {@code List} of {@code Throwable}s in
   * {@link ErrorCollector}.
   */
  private static class ProtectedErrorCollector extends ErrorCollector {

    private final List<Throwable> protectedErrors;

    ProtectedErrorCollector() {
      try {
        Field superErrors = ErrorCollector.class.getDeclaredField("errors");
        superErrors.setAccessible(true);
        protectedErrors = (List<Throwable>) superErrors.get(this);
      } catch (IllegalAccessException | NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }

    List<Throwable> errors() {
      return protectedErrors;
    }

    @Override
    public void verify() throws Throwable {
      super.verify();
    }
  }
}
