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
package org.apache.geode.test.junit.rules;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import org.apache.geode.test.junit.Retry;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link RetryRule} involving global scope (ie rule affects all
 * tests in the test class) with failures due to an {@code Error}.
 * 
 * @see org.apache.geode.test.junit.rules.RetryRule
 */
@Category(UnitTest.class)
public class RetryRuleGlobalWithErrorTest {
  
  @Test
  public void zeroIsIllegal() {
    Result result = TestRunner.runTest(ZeroIsIllegal.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage(ZeroIsIllegal.message);
    assertThat(ZeroIsIllegal.count).isEqualTo(0);
  }
  
  @Test
  public void failsWithOne() {
    Result result = TestRunner.runTest(FailsWithOne.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(FailsWithOne.message);
    assertThat(FailsWithOne.count).isEqualTo(1);
  }
  
  @Test
  public void passesWithOne() {
    Result result = TestRunner.runTest(PassesWithOne.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassesWithOne.count).isEqualTo(1);
  }
  
  @Test
  public void passesWithUnused() {
    Result result = TestRunner.runTest(PassesWhenUnused.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassesWhenUnused.count).isEqualTo(1);
  }
  
  @Test
  public void failsOnSecondAttempt() {
    Result result = TestRunner.runTest(FailsOnSecondAttempt.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(FailsOnSecondAttempt.message);
    assertThat(FailsOnSecondAttempt.count).isEqualTo(2);
  }

  @Test
  public void passesOnSecondAttempt() {
    Result result = TestRunner.runTest(PassesOnSecondAttempt.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassesOnSecondAttempt.count).isEqualTo(2);
  }
  
  @Test
  public void failsOnThirdAttempt() {
    Result result = TestRunner.runTest(FailsOnThirdAttempt.class);
    
    assertThat(result.wasSuccessful()).isFalse();
    
    List<Failure> failures = result.getFailures();
    assertThat(failures.size()).as("Failures: " + failures).isEqualTo(1);

    Failure failure = failures.get(0);
    assertThat(failure.getException()).isExactlyInstanceOf(AssertionError.class).hasMessage(FailsOnThirdAttempt.message);
    assertThat(FailsOnThirdAttempt.count).isEqualTo(3);
  }

  @Test
  public void passesOnThirdAttempt() {
    Result result = TestRunner.runTest(PassesOnThirdAttempt.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(PassesOnThirdAttempt.count).isEqualTo(3);
  }

  /**
   * Used by test {@link #zeroIsIllegal()}
   */
  public static class ZeroIsIllegal {

    static final String message = "Retry count must be greater than zero";
    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RetryRule retryRule = new RetryRule(0);

    @Test
    public void doTest() throws Exception {
      count++;
    }
  }

  /**
   * Used by test {@link #failsWithOne()}
   */
  public static class FailsWithOne {

    static int count = 0;
    static String message = null;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
      message = null;
    }

    @Rule
    public RetryRule retryRule = new RetryRule(1);

    @Test
    public void doTest() throws Exception {
      count++;
      message = "Failing " + count;
      fail(message);
    }
  }

  /**
   * Used by test {@link #passesWithOne()}
   */
  public static class PassesWithOne {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RetryRule retryRule = new RetryRule(1);

    @Test
    public void doTest() throws Exception {
      count++;
    }
  }

  /**
   * Used by test {@link #passesWithUnused()}
   */
  public static class PassesWhenUnused {

    static int count = 0;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
    }

    @Rule
    public RetryRule retryRule = new RetryRule(2);

    @Test
    public void doTest() throws Exception {
      count++;
    }
  }

  /**
   * Used by test {@link #failsOnSecondAttempt()}
   */
  public static class FailsOnSecondAttempt {

    static int count = 0;
    static String message = null;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
      message = null;
    }

    @Rule
    public RetryRule retryRule = new RetryRule(2);

    @Test
    @Retry(2)
    public void doTest() throws Exception {
      count++;
      message = "Failing " + count;
      fail(message);
    }
  }

  /**
   * Used by test {@link #passesOnSecondAttempt()}
   */
  public static class PassesOnSecondAttempt {

    static int count = 0;
    static String message = null;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
      message = null;
    }

    @Rule
    public RetryRule retryRule = new RetryRule(2);

    @Test
    @Retry(2)
    public void doTest() throws Exception {
      count++;
      if (count < 2) {
        message = "Failing " + count;
        fail(message);
      }
    }
  }

  /**
   * Used by test {@link #failsOnThirdAttempt()}
   */
  public static class FailsOnThirdAttempt {

    static int count = 0;
    static String message = null;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
      message = null;
    }

    @Rule
    public RetryRule retryRule = new RetryRule(3);

    @Test
    @Retry(3)
    public void doTest() throws Exception {
      count++;
      message = "Failing " + count;
      fail(message);
    }
  }

  /**
   * Used by test {@link #passesOnThirdAttempt()}
   */
  public static class PassesOnThirdAttempt {

    static int count = 0;
    static String message = null;

    @BeforeClass
    public static void beforeClass() {
      count = 0;
      message = null;
    }

    @Rule
    public RetryRule retryRule = new RetryRule(3);

    @Test
    public void doTest() throws Exception {
      count++;
      if (count < 3) {
        message = "Failing " + count;
        fail(message);
      }
    }
  }
}
