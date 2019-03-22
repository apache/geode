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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;

import org.apache.geode.test.concurrency.loop.LoopRunnerConfig;


public class ConcurrentTestRunnerTest {

  @Test
  public void failingTest() {
    assertThat(JUnitCore.runClasses(FailingTest.class).wasSuccessful()).isFalse();
  }

  @Test
  public void passingTest() {
    final Result result = JUnitCore.runClasses(PassingTest.class);
    assertThat(result.wasSuccessful()).isTrue();
  }

  public abstract static class BaseTest {
    protected void test(final ParallelExecutor executor, final Map<Integer, Integer> map)
        throws ExecutionException, InterruptedException {
      executor.inParallel(() -> {
        map.put(1, 1);
        assertThat(map.get(1)).isEqualTo(1);
      });

      executor.inParallel(() -> {
        map.put(2, 2);
        assertThat(map.get(2)).isEqualTo(2);
      });

      executor.execute();

      assertThat(map.size()).isEqualTo(2);
    }

  }

  @RunWith(ConcurrentTestRunner.class)
  @LoopRunnerConfig(count = 100000)
  public static class FailingTest extends BaseTest {
    @Test
    public void test(ParallelExecutor executor) throws ExecutionException, InterruptedException {
      super.test(executor, new HashMap<>());
    }
  }

  @RunWith(ConcurrentTestRunner.class)
  @LoopRunnerConfig(count = 100000)
  public static class PassingTest extends BaseTest {
    @Test
    public void test(ParallelExecutor executor) throws ExecutionException, InterruptedException {
      super.test(executor, new ConcurrentHashMap<>());
    }
  }

}
