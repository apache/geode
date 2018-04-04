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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.hamcrest.Matcher;
import org.junit.rules.ErrorCollector;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.serializable.SerializableTestRule;

/**
 * JUnit Rule that provides a shared ErrorCollector in all DistributedTest VMs. In particular, this
 * is a useful way to add assertions to CacheListener callbacks which are then registered in
 * multiple DistributedTest VMs.
 *
 * <p>
 * {@code SharedErrorCollector} can be used in DistributedTests as a {@code Rule}:
 *
 * <pre>
 * {@literal @}ClassRule
 * public static DistributedTestRule distributedTestRule = new DistributedTestRule();
 *
 * {@literal @}Rule
 * public SharedErrorCollector errorCollector = new SharedErrorCollector();
 *
 * {@literal @}Test
 * public void everyVMFailsAssertion() {
 *   for (VM vm : Host.getHost(0).getAllVMs()) {
 *     vm.invoke(() -> errorCollector.checkThat("Failure in VM-" + vm.getId(), false, is(true)));
 *   }
 * }
 * </pre>
 */
@SuppressWarnings({"serial", "unused"})
public class SharedErrorCollector implements SerializableTestRule {

  private static volatile ProtectedErrorCollector errorCollector;

  private final RemoteInvoker invoker;

  public SharedErrorCollector() {
    this(new RemoteInvoker());
  }

  SharedErrorCollector(final RemoteInvoker invoker) {
    this.invoker = invoker;
  }

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before();
        try {
          base.evaluate();
        } finally {
          after();
        }
      }
    };
  }

  protected void before() throws Throwable {
    invoker.invokeInEveryVMAndController(() -> errorCollector = new ProtectedErrorCollector());
  }

  protected void after() throws Throwable {
    ProtectedErrorCollector allErrors = errorCollector;
    try {
      for (VM vm : Host.getHost(0).getAllVMs()) {
        List<Throwable> remoteFailures = new ArrayList<>();
        remoteFailures.addAll(vm.invoke(() -> errorCollector.errors()));
        for (Throwable t : remoteFailures) {
          allErrors.addError(t);
        }
      }
      invoker.invokeInEveryVMAndController(() -> errorCollector = null);
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

    protected final List<Throwable> protectedErrors;

    public ProtectedErrorCollector() {
      super();
      try {
        Field superErrors = ErrorCollector.class.getDeclaredField("errors");
        superErrors.setAccessible(true);
        this.protectedErrors = (List<Throwable>) superErrors.get(this);
      } catch (IllegalAccessException | NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }

    public List<Throwable> errors() {
      return protectedErrors;
    }

    @Override
    public void verify() throws Throwable {
      super.verify();
    }
  }
}
