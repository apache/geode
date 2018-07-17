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
package org.apache.geode.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;


@Category({IntegrationTest.class})
public class ConcurrencyTestHelperTest {
  public static Boolean r1Status = Boolean.FALSE;
  public static Boolean r2Status = Boolean.FALSE;
  public static Boolean r3Status = Boolean.FALSE;
  public static Boolean r4Status = Boolean.FALSE;

  public Callable<Boolean> c1 = () -> {
    return Boolean.TRUE;
  };

  public Callable<Boolean> c2 = () -> {
    try {
      Thread.sleep(350000);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted");
    }

    return Boolean.TRUE;
  };

  public Callable<Boolean> c3 = () -> {
    throw new IllegalStateException("Expect this");
  };

  public Callable<Boolean> c4 = () -> {
    try {
      Thread.sleep(25000);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted");
    }

    return Boolean.TRUE;
  };

  public Runnable r1 = () -> {
    r1Status = Boolean.TRUE;
  };

  public Runnable r2 = () -> {
    try {
      Thread.sleep(350000);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted");
    }

    r2Status = Boolean.TRUE;
  };

  public Runnable r3 = () -> {
    r3Status = Boolean.TRUE;
    throw new IllegalStateException("Expect this");
  };

  public Runnable r4 = () -> {
    try {
      Thread.sleep(25000);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted");
    }

    r4Status = Boolean.TRUE;
  };

  @Before
  public void before() {
    r1Status = Boolean.FALSE;
    r2Status = Boolean.FALSE;
    r3Status = Boolean.FALSE;
    r4Status = Boolean.FALSE;
  }

  @Test
  public void testRunnablesWithDefaultTimeout() throws InterruptedException {
    List<Runnable> runnables = Arrays.asList(r1, r2, r3, r4);
    List<Throwable> exceptions = ConcurrencyTestHelper.runRunnables(runnables);


    assertThat(exceptions.get(0)).isNull();
    assertThat(exceptions.get(1)).isInstanceOf(CancellationException.class);
    assertThat(exceptions.get(2)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expect this");
    assertThat(exceptions.get(3)).isNull();

    assertThat(r1Status).isTrue();
    assertThat(r2Status).isFalse();
    assertThat(r3Status).isTrue();
    assertThat(r4Status).isTrue();
  }

  @Test
  public void testRunnablesWithNonDefaultTimeout() throws InterruptedException {
    List<Runnable> runnables = Arrays.asList(r1, r2, r3, r4);
    List<Throwable> exceptions = ConcurrencyTestHelper.runRunnables(runnables, 20);

    assertThat(exceptions.get(0)).isNull();
    assertThat(exceptions.get(1)).isInstanceOf(CancellationException.class);
    assertThat(exceptions.get(2)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expect this");
    assertThat(exceptions.get(3)).isInstanceOf(CancellationException.class);

    assertThat(r1Status).isTrue();
    assertThat(r2Status).isFalse();
    assertThat(r3Status).isTrue();
    assertThat(r4Status).isFalse();
  }

  @Test
  public void testCallablesWithDefaultTimeoutWithNoExceptions() {
    List<Callable<Boolean>> callables = Arrays.asList(c1, c4);
    List<Boolean> results = null;

    try {
      results = ConcurrencyTestHelper.runCallables(callables);
    } catch (Exception e) {
      fail("Expected no exceptions to be thrown.");
    }

    assertThat(results).isNotNull().containsExactly(Boolean.TRUE, Boolean.TRUE);
  }

  @Test
  public void testCallablesWithNonDefaultTimeoutWithNoExceptions() {
    List<Callable<Boolean>> callables = Arrays.asList(c1, c4);
    List<Boolean> results = null;

    try {
      results = ConcurrencyTestHelper.runCallables(callables, 40);
    } catch (Exception e) {
      fail("Expected no exceptions to be thrown.");
    }

    assertThat(results).isNotNull().containsExactly(Boolean.TRUE, Boolean.TRUE);
  }

  @Test
  public void testCallablesWithDefaultTimeoutWithExceptions() throws InterruptedException {
    List<Callable<Boolean>> callables = Arrays.asList(c1, c2, c3, c4);

    try {
      ConcurrencyTestHelper.runCallables(callables);
      fail("Expected an exception");
    } catch (RuntimeException e) {
      assertThat(e.getMessage()).isNotNull();
    }
  }

  @Test
  public void testCallablesWithNonDefaultTimeoutWithExceptions() throws InterruptedException {
    List<Callable<Boolean>> callables = Arrays.asList(c1, c2, c3, c4);

    try {
      ConcurrencyTestHelper.runCallables(callables, 40);
      fail("Expected an exception");
    } catch (RuntimeException e) {
      assertThat(e.getMessage()).isNotNull();
    }
  }
}
