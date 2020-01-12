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

import static org.apache.geode.test.concurrency.Utilities.availableProcessors;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;

public class ConcurrentTestRunnerTest {
  @Test
  public void confirmThatInParallelRunsConcurrently() {
    // We only need FailingTest to fail once for the following
    // assertion to pass. ConcurrentTestRunner runs FailingTest
    // 2000 times by default. It will stop running it once it
    // sees it fail, which is what we want to see because it
    // confirms that running inParallel actually runs concurrently.
    assertThat(JUnitCore.runClasses(CheckForConcurrency.class).wasSuccessful()).isFalse();
  }

  /**
   * This "test" is only meant to be run by confirmThatInParallelRunsConcurrently.
   * If you run this "test" directly you can expect to see if fail.
   */
  @RunWith(ConcurrentTestRunner.class)
  public static class CheckForConcurrency {
    @Test
    public void validateConcurrentExecution(ParallelExecutor executor)
        throws ExecutionException, InterruptedException {
      final AtomicInteger atomicInteger = new AtomicInteger(0);
      executor.inParallel(() -> {
        int oldValue = atomicInteger.get();
        // We want to see the following assertion fail because that indicates
        // that another thread currently modified the atomic.
        assertThat(atomicInteger.compareAndSet(oldValue, oldValue + 1)).isTrue();
      }, availableProcessors());
      executor.execute();
    }
  }
}
