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
package com.gemstone.gemfire.test.junit.rules.tests;

import static com.gemstone.gemfire.test.junit.rules.tests.TestRunner.*;
import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.gemstone.gemfire.test.junit.IgnoreUntil;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.gemstone.gemfire.test.junit.rules.IgnoreUntilRule;

/**
 * Unit tests for IgnoreUntil JUnit Rule
 * 
 */
@Category(UnitTest.class)
public class IgnoreUntilRuleTest {

  private static final String ASSERTION_ERROR_MESSAGE = "failing test";
  
  @Test
  public void shouldIgnoreWhenUntilIsInFuture() {
    Result result = runTest(ShouldIgnoreWhenUntilIsInFuture.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(ShouldIgnoreWhenUntilIsInFuture.count).isEqualTo(0);
  }
  
  @Test
  public void shouldExecuteWhenUntilIsInPast() {
    Result result = runTest(ShouldExecuteWhenUntilIsInPast.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(ShouldExecuteWhenUntilIsInPast.count).isEqualTo(1);
  }
  
  @Test
  public void shouldExecuteWhenUntilIsDefault() {
    Result result = runTest(ShouldExecuteWhenUntilIsDefault.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(ASSERTION_ERROR_MESSAGE);
    assertThat(ShouldExecuteWhenUntilIsDefault.count).isEqualTo(1);
  }
  
  public static class ShouldIgnoreWhenUntilIsInFuture {
    private static int count;
    
    @Rule
    public final IgnoreUntilRule ignoreUntilRule = new IgnoreUntilRule();
    
    @Test
    @IgnoreUntil(value = "description", until = "3000-01-01")
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  public static class ShouldExecuteWhenUntilIsInPast {
    private static int count;
    
    @Rule
    public final IgnoreUntilRule ignoreUntilRule = new IgnoreUntilRule();
    
    @Test
    @IgnoreUntil(value = "description", until = "1980-01-01")
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }

  public static class ShouldExecuteWhenUntilIsDefault {
    private static int count;
    
    @Rule
    public final IgnoreUntilRule ignoreUntilRule = new IgnoreUntilRule();
    
    @Test
    @IgnoreUntil(value = "description")
    public void doTest() throws Exception {
      count++;
      fail(ASSERTION_ERROR_MESSAGE);
    }
  }
}
