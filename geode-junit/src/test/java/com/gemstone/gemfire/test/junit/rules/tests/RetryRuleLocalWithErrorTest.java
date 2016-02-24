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
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.gemstone.gemfire.test.junit.Retry;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.gemstone.gemfire.test.junit.rules.RetryRule;

/**
 * Unit tests for Retry JUnit Rule involving local scope (ie Rule affects 
 * test methods annotated with @Retry) with failures due to an Error.
 * 
 */
@Category(UnitTest.class)
public class RetryRuleLocalWithErrorTest {

  @Test
  public void failsUnused() {
    Result result = runTest(FailsUnused.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(FailsUnused.message);
    assertThat(FailsUnused.count).isEqualTo(1);
  }
  
  @Test
  public void passesUnused() {
    Result result = runTest(PassesUnused.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassesUnused.count).isEqualTo(1);
  }
  
  @Test
  public void failsOnSecondAttempt() {
    Result result = runTest(FailsOnSecondAttempt.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(FailsOnSecondAttempt.message);
    assertThat(FailsOnSecondAttempt.count).isEqualTo(2);
  }

  @Test
  public void passesOnSecondAttempt() {
    Result result = runTest(PassesOnSecondAttempt.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassesOnSecondAttempt.count).isEqualTo(2);
  }
  
  @Test
  public void failsOnThirdAttempt() {
    Result result = runTest(FailsOnThirdAttempt.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(FailsOnThirdAttempt.message);
    assertThat(FailsOnThirdAttempt.count).isEqualTo(3);
  }

  @Test
  public void passesOnThirdAttempt() {
    Result result = runTest(PassesOnThirdAttempt.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassesOnThirdAttempt.count).isEqualTo(3);
  }
  
  public static class FailsUnused {
    protected static int count;
    protected static String message;

    @Rule
    public RetryRule retryRule = new RetryRule();

    @Test
    public void failsUnused() throws Exception {
      count++;
      message = "Failing " + count;
      fail(message);
    }
  }
  
  public static class PassesUnused {
    protected static int count;
    protected static String message;

    @Rule
    public RetryRule retryRule = new RetryRule();

    @Test
    public void passesUnused() throws Exception {
      count++;
    }
  }
  
  public static class FailsOnSecondAttempt {
    protected static int count;
    protected static String message;
    
    @Rule
    public RetryRule retryRule = new RetryRule();

    @Test
    @Retry(2)
    public void failsOnSecondAttempt() {
      count++;
      message = "Failing " + count;
      fail(message);
    }
  }
  
  public static class PassesOnSecondAttempt {
    protected static int count;
    protected static String message;
    
    @Rule
    public RetryRule retryRule = new RetryRule();

    @Test
    @Retry(2)
    public void failsOnSecondAttempt() {
      count++;
      if (count < 2) {
        message = "Failing " + count;
        fail(message);
      }
    }
  }
  
  public static class FailsOnThirdAttempt {
    protected static int count;
    protected static String message;
    
    @Rule
    public RetryRule retryRule = new RetryRule();

    @Test
    @Retry(3)
    public void failsOnThirdAttempt() {
      count++;

      message = "Failing " + count;
      fail(message);
    }
  }

  public static class PassesOnThirdAttempt {
    protected static int count;
    protected static String message;
    
    @Rule
    public RetryRule retryRule = new RetryRule();

    @Test
    @Retry(3)
    public void failsOnThirdAttempt() {
      count++;

      if (count < 3) {
        message = "Failing " + count;
        fail(message);
      }
    }
  }
}
